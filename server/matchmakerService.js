const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');
const { makeRedisClient } = require('./redisClient');

const PORT = Number(process.env.PORT) || 9000;

const PUBLIC_WS_URL =
  process.env.PUBLIC_WS_URL || `ws://localhost:${PORT}`;

const MIN_PLAYERS = 2;
const MAX_PLAYERS = 4;
const MAX_WAIT_MS = 5000;
const CHECK_INTERVAL_MS = 500;

let nextPlayerId = 1;
let nextSessionNumber = 1;

const metrics = {
  startedAt: Date.now(),
  matchesFormed: 0,
  totalPlayersMatched: 0,
  assignmentWaitSamplesMs: [],
};

function recordAssignmentWait(ms) {
  metrics.assignmentWaitSamplesMs.push(ms);

  if (metrics.assignmentWaitSamplesMs.length > 200) {
    metrics.assignmentWaitSamplesMs.shift();
  }
}

function avg(arr) {
  return arr.length
    ? arr.reduce((a, b) => a + b, 0) / arr.length
    : 0;
}

async function main() {
  const redis = await makeRedisClient('matchmaker');

  /*
   * IMPORTANT:
   * HTTP server and WebSocket server share the SAME Render port.
   */
  const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');

    if (req.url === '/health') {
      res.writeHead(200, {
        'Content-Type': 'text/plain',
      });

      res.end('Matchmaker OK');
      return;
    }

    if (req.url === '/metrics') {
      res.writeHead(200, {
        'Content-Type': 'application/json',
      });

      res.end(
        JSON.stringify({
          uptimeSec: Math.round(
            (Date.now() - metrics.startedAt) / 1000
          ),
          queueLength: queue.length,
          matchesFormed: metrics.matchesFormed,
          totalPlayersMatched: metrics.totalPlayersMatched,
          avgAssignmentWaitMs: Math.round(
            avg(metrics.assignmentWaitSamplesMs)
          ),
        })
      );

      return;
    }

    res.writeHead(404);
    res.end();
  });

  const wss = new WebSocket.Server({
    server,
  });

  server.listen(PORT, '0.0.0.0', () => {
    console.log(
      `Matchmaker listening on ${PUBLIC_WS_URL}`
    );

    console.log(
      `Matchmaker HTTP server listening on port ${PORT}`
    );
  });

  const queue = [];

  const connections = new Map();

  wss.on('connection', (ws) => {
    const id = nextPlayerId++;

    connections.set(id, ws);

    ws.send(
      JSON.stringify({
        type: 'welcome',
        id,
      })
    );

    console.log(
      `Player ${id} connected to matchmaker.`
    );

    ws.on('message', (raw) => {
      let msg;

      try {
        msg = JSON.parse(raw);
      } catch (e) {
        return;
      }

      if (msg.type === 'find_match') {
        if (queue.some((q) => q.id === id)) {
          return;
        }

        queue.push({
          id,
          ws,
          queuedAt: Date.now(),
        });

        ws.send(
          JSON.stringify({
            type: 'queued',
            position: queue.length,
          })
        );

        console.log(
          `Player ${id} queued. Queue size: ${queue.length}`
        );
      }
    });

    ws.on('close', () => {
      connections.delete(id);

      const idx = queue.findIndex(
        (q) => q.id === id
      );

      if (idx !== -1) {
        queue.splice(idx, 1);
      }

      console.log(
        `Player ${id} disconnected from matchmaker.`
      );
    });
  });

  /*
   * Track assignments that happened before
   * the next Redis heartbeat arrives.
   */
  const pendingAssignments = new Map();

  async function pickGameServer() {
    const keys = await redis.keys(
      'gameserver:*:info'
    );

    if (keys.length === 0) {
      return null;
    }

    const infos = await Promise.all(
      keys.map((k) => redis.get(k))
    );

    const servers = infos
      .filter(Boolean)
      .map((raw) => JSON.parse(raw));

    if (servers.length === 0) {
      return null;
    }

    for (const server of servers) {
      const pending = pendingAssignments.get(
        server.id
      );

      if (pending) {
        if (server.updatedAt > pending.since) {
          pendingAssignments.delete(server.id);

          server.effectiveLoad = server.load;
        } else {
          server.effectiveLoad =
            server.load + pending.count;
        }
      } else {
        server.effectiveLoad = server.load;
      }
    }

    servers.sort(
      (a, b) =>
        a.effectiveLoad - b.effectiveLoad
    );

    return servers[0];
  }

  function recordPendingAssignment(
    serverId,
    count
  ) {
    const existing =
      pendingAssignments.get(serverId);

    pendingAssignments.set(serverId, {
      count:
        (existing ? existing.count : 0) + count,
      since: Date.now(),
    });
  }

  async function checkQueue() {
    /*
     * Remove disconnected players.
     */
    for (
      let i = queue.length - 1;
      i >= 0;
      i--
    ) {
      if (
        queue[i].ws.readyState !==
        WebSocket.OPEN
      ) {
        queue.splice(i, 1);
      }
    }

    if (queue.length === 0) {
      return;
    }

    const oldestWait =
      Date.now() - queue[0].queuedAt;

    const shouldStart =
      queue.length >= MAX_PLAYERS ||
      (
        queue.length >= MIN_PLAYERS &&
        oldestWait >= MAX_WAIT_MS
      );

    if (!shouldStart) {
      return;
    }

    const server =
      await pickGameServer();

    if (!server) {
      console.warn(
        'No game servers registered yet — players stay queued.'
      );

      return;
    }

    const matchSize = Math.min(
      MAX_PLAYERS,
      queue.length
    );

    const matchedPlayers =
      queue.splice(0, matchSize);

    const sessionId =
      `s${nextSessionNumber++}`;

    const playersWithTokens =
      matchedPlayers.map((p) => ({
        id: p.id,
        token: crypto
          .randomBytes(8)
          .toString('hex'),
      }));

    console.log(
      `Assigning session ${sessionId} (${playersWithTokens.length} players) to ${server.id}`
    );

    metrics.matchesFormed++;

    metrics.totalPlayersMatched +=
      matchedPlayers.length;

    for (const p of matchedPlayers) {
      recordAssignmentWait(
        Date.now() - p.queuedAt
      );
    }

    recordPendingAssignment(
      server.id,
      matchedPlayers.length
    );

    /*
     * Tell the selected game server to create
     * the session.
     */
    await redis.publish(
      `gameserver:${server.id}:commands`,
      JSON.stringify({
        type: 'create_session',
        sessionId,
        players: playersWithTokens,
      })
    );

    /*
     * Tell the browser the PUBLIC WebSocket URL.
     */
    for (const p of matchedPlayers) {
      const tokenEntry =
        playersWithTokens.find(
          (t) => t.id === p.id
        );

      p.ws.send(
        JSON.stringify({
          type: 'match_assigned',
          sessionId,
          playerId: p.id,
          token: tokenEntry.token,

          server: {
            id: server.id,
            url: server.url,
          },
        })
      );
    }
  }

  setInterval(() => {
    checkQueue().catch((err) => {
      console.error(
        'checkQueue error:',
        err
      );
    });
  }, CHECK_INTERVAL_MS);
}

main().catch((err) => {
  console.error(
    'Fatal error starting matchmaker:',
    err
  );

  process.exit(1);
});