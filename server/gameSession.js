// A GameSession is one isolated match. Stage 4 adds:
//   - A short rolling history of player positions, so we can answer
//     "where was player X 120ms ago?" — this is what lag compensation
//     needs.
//   - Server-measured latency per player (via ping/pong), used to decide
//     HOW FAR back to rewind for that specific player's shots.
//   - A hitscan "railgun" weapon that uses that rewound history — this
//     is the actual lag-compensation feature. The original projectile
//     bullets don't need it (a physical object traveling through space
//     is already "fair" — lag comp specifically matters for INSTANT-HIT
//     weapons, where without it, players would have to "lead" their
//     shots to account for others' latency).
//   - Lightweight performance metrics per session (avg/max tick time),
//     exposed for the observability dashboard.

const WebSocket = require('ws');

const TICK_RATE = 20;
const TICK_MS = 1000 / TICK_RATE;
const ARENA_WIDTH = 800;
const ARENA_HEIGHT = 600;
const PLAYER_SPEED = 200;
const BULLET_SPEED = 500;
const PLAYER_RADIUS = 15;
const BULLET_RADIUS = 4;
const MATCH_DURATION_MS = 3 * 60 * 1000;
const RECONNECT_GRACE_MS = 20 * 1000;
const JOIN_GRACE_MS = 15 * 1000;

const HISTORY_WINDOW_MS = 1000; // how far back we keep position snapshots
const PING_INTERVAL_TICKS = 40; // ~every 2s at 20Hz
const MAX_REWIND_MS = 300; // hard cap so a spoofed/huge latency can't rewind too far
const RAILGUN_COOLDOWN_MS = 1000;
const RAILGUN_DAMAGE = 30;
const RAILGUN_RANGE = 1000;
const RAILGUN_HIT_TOLERANCE = 6; // extra pixels of forgiveness on the hit check

let nextBulletId = 1;

function lerp(a, b, t) {
  return a + (b - a) * t;
}

// Shortest distance from point P to line segment AB, and how far along
// the segment (0..1) that closest point is.
function pointToSegment(px, py, ax, ay, bx, by) {
  const abx = bx - ax;
  const aby = by - ay;
  const abLenSq = abx * abx + aby * aby || 1;
  let t = ((px - ax) * abx + (py - ay) * aby) / abLenSq;
  t = Math.max(0, Math.min(1, t));
  const cx = ax + abx * t;
  const cy = ay + aby * t;
  const dx = px - cx;
  const dy = py - cy;
  return { distance: Math.sqrt(dx * dx + dy * dy), t };
}

class GameSession {
  constructor(sessionId, expectedPlayers, onEnd) {
    this.id = sessionId;
    this.onEnd = onEnd;
    this.players = new Map();
    this.bullets = [];
    this.startedAt = Date.now();
    this.interval = null;
    this.ended = false;
    this.tickCount = 0;
    this.history = []; // [{ t, positions: Map(id -> {x,y}) }]
    this.avgTickMs = 0;
    this.maxTickMs = 0;

    for (const ep of expectedPlayers) {
      const pos = this.spawnPosition();
      this.players.set(ep.id, {
        id: ep.id,
        token: ep.token,
        ws: null,
        connected: false,
        removed: false,
        x: pos.x,
        y: pos.y,
        hp: 100,
        score: 0,
        keys: { up: false, down: false, left: false, right: false },
        aim: { x: 0, y: 0 },
        lastShot: 0,
        lastRailgun: 0,
        latencyMs: 0,
        pendingPings: new Map(), // pingId -> sentAt, so pong replies can't be spoofed to arbitrary times
        disconnectTimer: null,
      });
    }

    this.joinGraceTimer = setTimeout(() => {
      const anyoneEverConnected = Array.from(this.players.values()).some((p) => p.connected || p.hadConnected);
      if (!anyoneEverConnected) this.end('no_players_joined');
    }, JOIN_GRACE_MS);

    this.start();
  }

  spawnPosition() {
    return {
      x: 50 + Math.random() * (ARENA_WIDTH - 100),
      y: 50 + Math.random() * (ARENA_HEIGHT - 100),
    };
  }

  // Called whenever hp crosses from >0 to 0 — awards the kill, respawns
  // the victim, and tells every client so a kill feed can render it.
  registerKill(killerId, victimId, weapon) {
    const killer = this.players.get(killerId);
    const victim = this.players.get(victimId);
    if (killer) killer.score += 1;
    if (victim) {
      const pos = this.spawnPosition();
      victim.x = pos.x;
      victim.y = pos.y;
      victim.hp = 100;
    }
    this.broadcast({ type: 'kill_feed', killerId, victimId, weapon });
  }

  attachPlayer(playerId, ws) {
    const player = this.players.get(playerId);
    if (!player || player.removed) return false;

    if (player.disconnectTimer) {
      clearTimeout(player.disconnectTimer);
      player.disconnectTimer = null;
    }

    player.ws = ws;
    player.connected = true;
    player.hadConnected = true;

    ws.send(
      JSON.stringify({
        type: 'match_found',
        sessionId: this.id,
        arena: { w: ARENA_WIDTH, h: ARENA_HEIGHT },
        players: Array.from(this.players.keys()),
        yourState: { x: player.x, y: player.y, hp: player.hp },
      })
    );
    return true;
  }

  handleMessage(playerId, msg) {
    const player = this.players.get(playerId);
    if (!player || !player.connected || player.removed) return;

    if (msg.type === 'input') {
      player.keys = {
        up: !!msg.keys?.up,
        down: !!msg.keys?.down,
        left: !!msg.keys?.left,
        right: !!msg.keys?.right,
      };
      if (msg.aim && typeof msg.aim.x === 'number' && typeof msg.aim.y === 'number') {
        player.aim = msg.aim;
      }
    }

    if (msg.type === 'shoot') {
      const now = Date.now();
      if (now - player.lastShot < 200) return;
      player.lastShot = now;

      const dx = player.aim.x - player.x;
      const dy = player.aim.y - player.y;
      const len = Math.sqrt(dx * dx + dy * dy) || 1;

      this.bullets.push({
        id: nextBulletId++,
        x: player.x,
        y: player.y,
        dx: (dx / len) * BULLET_SPEED,
        dy: (dy / len) * BULLET_SPEED,
        ownerId: playerId,
      });
    }

    if (msg.type === 'railgun') {
      this.handleRailgun(player);
    }

    if (msg.type === 'pong' && typeof msg.pingId === 'number') {
      const sentAt = player.pendingPings.get(msg.pingId);
      if (sentAt != null) {
        const rttMs = Date.now() - sentAt;
        // Smooth it so one slow reply doesn't cause a huge rewind swing
        player.latencyMs = player.latencyMs ? player.latencyMs * 0.7 + (rttMs / 2) * 0.3 : rttMs / 2;
        player.pendingPings.delete(msg.pingId);
      }
    }
  }

  // ---- Lag compensation: the actual rewind ----
  getRewoundPosition(playerId, rewindTimeMs) {
    if (this.history.length === 0) {
      const p = this.players.get(playerId);
      return p ? { x: p.x, y: p.y } : null;
    }

    // History is oldest-first. Find the two snapshots that bracket rewindTimeMs.
    if (rewindTimeMs <= this.history[0].t) {
      return this.history[0].positions.get(playerId) || this.currentPos(playerId);
    }
    const last = this.history[this.history.length - 1];
    if (rewindTimeMs >= last.t) {
      return this.currentPos(playerId);
    }

    for (let i = 0; i < this.history.length - 1; i++) {
      const a = this.history[i];
      const b = this.history[i + 1];
      if (rewindTimeMs >= a.t && rewindTimeMs <= b.t) {
        const posA = a.positions.get(playerId);
        const posB = b.positions.get(playerId);
        if (!posA || !posB) return this.currentPos(playerId);
        const frac = (rewindTimeMs - a.t) / (b.t - a.t || 1);
        return { x: lerp(posA.x, posB.x, frac), y: lerp(posA.y, posB.y, frac) };
      }
    }
    return this.currentPos(playerId);
  }

  currentPos(playerId) {
    const p = this.players.get(playerId);
    return p ? { x: p.x, y: p.y } : null;
  }

  handleRailgun(shooter) {
    const now = Date.now();
    if (now - shooter.lastRailgun < RAILGUN_COOLDOWN_MS) return;
    shooter.lastRailgun = now;

    // Rewind by the shooter's full round trip: that's roughly how stale
    // the world looked to them at the moment they clicked.
    const rewindMs = Math.min(MAX_REWIND_MS, shooter.latencyMs * 2);
    const rewindTime = now - rewindMs;

    const dx = shooter.aim.x - shooter.x;
    const dy = shooter.aim.y - shooter.y;
    const len = Math.sqrt(dx * dx + dy * dy) || 1;
    const dirX = dx / len;
    const dirY = dy / len;
    const rayEndX = shooter.x + dirX * RAILGUN_RANGE;
    const rayEndY = shooter.y + dirY * RAILGUN_RANGE;

    let bestHit = null; // { playerId, t }

    for (const target of this.players.values()) {
      if (target.removed || target.id === shooter.id) continue;
      const rewoundPos = this.getRewoundPosition(target.id, rewindTime);
      if (!rewoundPos) continue;

      const { distance, t } = pointToSegment(rewoundPos.x, rewoundPos.y, shooter.x, shooter.y, rayEndX, rayEndY);
      if (distance <= PLAYER_RADIUS + RAILGUN_HIT_TOLERANCE) {
        if (!bestHit || t < bestHit.t) {
          bestHit = { playerId: target.id, t };
        }
      }
    }

    if (bestHit) {
      const target = this.players.get(bestHit.playerId);
      const wasAlive = target.hp > 0;
      target.hp = Math.max(0, target.hp - RAILGUN_DAMAGE);
      if (wasAlive && target.hp === 0) this.registerKill(shooter.id, target.id, 'railgun');
    }

    this.broadcast({
      type: 'railgun_shot',
      shooterId: shooter.id,
      from: { x: Math.round(shooter.x), y: Math.round(shooter.y) },
      to: { x: Math.round(rayEndX), y: Math.round(rayEndY) },
      hitPlayerId: bestHit ? bestHit.playerId : null,
      rewoundMs: Math.round(rewindMs), // useful to show in UI/debug — "this shot rewound 84ms"
    });
  }

  handleDisconnect(playerId) {
    const player = this.players.get(playerId);
    if (!player || player.removed) return;

    player.connected = false;
    player.ws = null;
    player.keys = { up: false, down: false, left: false, right: false };

    player.disconnectTimer = setTimeout(() => {
      player.removed = true;
      this.checkForEmptySession();
    }, RECONNECT_GRACE_MS);
  }

  checkForEmptySession() {
    const anyoneLeft = Array.from(this.players.values()).some((p) => !p.removed);
    if (!anyoneLeft) this.end('all_players_left');
  }

  start() {
    let lastTime = Date.now();
    this.interval = setInterval(() => {
      const now = Date.now();
      const dt = (now - lastTime) / 1000;
      lastTime = now;
      this.tick(dt);

      if (now - this.startedAt > MATCH_DURATION_MS) {
        this.end('time_limit');
      }
    }, TICK_MS);
  }

  tick(dt) {
    const tickStart = process.hrtime.bigint();
    this.tickCount++;

    for (const p of this.players.values()) {
      if (p.removed || !p.connected) continue;
      let vx = 0;
      let vy = 0;
      if (p.keys.up) vy -= 1;
      if (p.keys.down) vy += 1;
      if (p.keys.left) vx -= 1;
      if (p.keys.right) vx += 1;

      const len = Math.sqrt(vx * vx + vy * vy);
      if (len > 0) {
        vx = (vx / len) * PLAYER_SPEED * dt;
        vy = (vy / len) * PLAYER_SPEED * dt;
      }

      p.x = Math.max(PLAYER_RADIUS, Math.min(ARENA_WIDTH - PLAYER_RADIUS, p.x + vx));
      p.y = Math.max(PLAYER_RADIUS, Math.min(ARENA_HEIGHT - PLAYER_RADIUS, p.y + vy));
    }

    for (const b of this.bullets) {
      b.x += b.dx * dt;
      b.y += b.dy * dt;
    }

    for (let i = this.bullets.length - 1; i >= 0; i--) {
      const b = this.bullets[i];
      if (b.x < 0 || b.x > ARENA_WIDTH || b.y < 0 || b.y > ARENA_HEIGHT) {
        this.bullets.splice(i, 1);
        continue;
      }
      for (const p of this.players.values()) {
        if (p.removed || p.id === b.ownerId) continue;
        const dx = p.x - b.x;
        const dy = p.y - b.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < PLAYER_RADIUS + BULLET_RADIUS) {
          const wasAlive = p.hp > 0;
          p.hp = Math.max(0, p.hp - 10);
          if (wasAlive && p.hp === 0) this.registerKill(b.ownerId, p.id, 'bullet');
          this.bullets.splice(i, 1);
          break;
        }
      }
    }

    // Record a history snapshot for lag compensation
    const snapshot = { t: Date.now(), positions: new Map() };
    for (const p of this.players.values()) {
      if (!p.removed) snapshot.positions.set(p.id, { x: p.x, y: p.y });
    }
    this.history.push(snapshot);
    const cutoff = Date.now() - HISTORY_WINDOW_MS;
    while (this.history.length > 0 && this.history[0].t < cutoff) {
      this.history.shift();
    }

    // Periodic latency pings
    if (this.tickCount % PING_INTERVAL_TICKS === 0) {
      for (const p of this.players.values()) {
        if (!p.connected || !p.ws) continue;
        const pingId = Date.now() + Math.random();
        p.pendingPings.set(pingId, Date.now());
        p.ws.send(JSON.stringify({ type: 'ping', pingId }));
        // Don't let pendingPings grow forever if a pong never comes back
        if (p.pendingPings.size > 10) {
          const oldestKey = p.pendingPings.keys().next().value;
          p.pendingPings.delete(oldestKey);
        }
      }
    }

    this.broadcastState();

    const tickMs = Number(process.hrtime.bigint() - tickStart) / 1e6;
    this.avgTickMs = this.avgTickMs ? this.avgTickMs * 0.9 + tickMs * 0.1 : tickMs;
    this.maxTickMs = Math.max(this.maxTickMs, tickMs);
  }

  broadcastState() {
    const state = {
      type: 'state',
      players: Array.from(this.players.values())
        .filter((p) => !p.removed)
        .map((p) => ({
          id: p.id,
          x: Math.round(p.x),
          y: Math.round(p.y),
          hp: p.hp,
          score: p.score,
          connected: p.connected,
          latencyMs: Math.round(p.latencyMs),
        })),
      bullets: this.bullets.map((b) => ({ id: b.id, x: Math.round(b.x), y: Math.round(b.y) })),
    };
    this.broadcast(state);
  }

  broadcast(obj) {
    const payload = JSON.stringify(obj);
    for (const p of this.players.values()) {
      if (p.connected && p.ws && p.ws.readyState === WebSocket.OPEN) {
        p.ws.send(payload);
      }
    }
  }

  currentLoad() {
    return Array.from(this.players.values()).filter((p) => !p.removed).length;
  }

  getMetrics() {
    return {
      id: this.id,
      players: this.currentLoad(),
      avgTickMs: Math.round(this.avgTickMs * 100) / 100,
      maxTickMs: Math.round(this.maxTickMs * 100) / 100,
      ageSec: Math.round((Date.now() - this.startedAt) / 1000),
    };
  }

  end(reason) {
    if (this.ended) return;
    this.ended = true;
    if (this.interval) clearInterval(this.interval);
    if (this.joinGraceTimer) clearTimeout(this.joinGraceTimer);
    for (const p of this.players.values()) {
      if (p.disconnectTimer) clearTimeout(p.disconnectTimer);
    }

    const finalScores = Array.from(this.players.values())
      .map((p) => ({ id: p.id, score: p.score }))
      .sort((a, b) => b.score - a.score);
    const winnerId = finalScores.length > 0 && finalScores[0].score > 0 ? finalScores[0].id : null;

    this.broadcast({ type: 'match_end', reason, finalScores, winnerId });
    this.onEnd(this.id);
  }
}

module.exports = { GameSession };