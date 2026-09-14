// A single game server INSTANCE. You can run many of these at once,
// each on its own port. Every instance:
//   1. Registers itself in Redis with a heartbeat (so the matchmaker
//      knows it exists and how loaded it is)
//   2. Subscribes to its own Redis pub/sub channel to receive
//      "create this session" commands from the matchmaker
//   3. Runs a WebSocket server that players connect to directly once
//      they've been assigned here
//
// Run with:  node server/gameServerProcess.js --id gs1 --port 8081

const WebSocket = require('ws');
const http = require('http');
const { GameSession } = require('./gameSession');
const { makeRedisClient } = require('./redisClient');

function parseArgs() {
  const args = { id: 'gs1', port: 8081 };
  process.argv.slice(2).forEach((arg, i, arr) => {
    if (arg === '--id') args.id = arr[i + 1];
    if (arg === '--port') args.port = parseInt(arr[i + 1], 10);
  });
  return args;
}

const { id: SERVER_ID, port: PORT } = parseArgs();
const METRICS_PORT = PORT + 1000; // e.g. game port 8081 -> metrics on 9081
const HEARTBEAT_MS = 1000;
const HEARTBEAT_TTL_SEC = 5;
const processStartedAt = Date.now();

const sessions = new Map(); // sessionId -> GameSession

async function main() {
  const redis = await makeRedisClient(`gs:${SERVER_ID}:cmd`);
  const sub = await makeRedisClient(`gs:${SERVER_ID}:sub`);

  const wss = new WebSocket.Server({ port: PORT });
  console.log(`[${SERVER_ID}] Game server listening on ws://localhost:${PORT}`);

  // ---- Observability: a tiny HTTP endpoint any dashboard can poll ----
  const metricsServer = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*'); // dashboard.html is opened from a different origin (file://)
    if (req.url === '/metrics') {
      const load = Array.from(sessions.values()).reduce((sum, s) => sum + s.currentLoad(), 0);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          id: SERVER_ID,
          port: PORT,
          uptimeSec: Math.round((Date.now() - processStartedAt) / 1000),
          load,
          activeSessions: sessions.size,
          sessions: Array.from(sessions.values()).map((s) => s.getMetrics()),
        })
      );
    } else {
      res.writeHead(404);
      res.end();
    }
  });
  metricsServer.listen(METRICS_PORT, () => {
    console.log(`[${SERVER_ID}] Metrics available at http://localhost:${METRICS_PORT}/metrics`);
  });

  // ---- Register + heartbeat ----
  async function heartbeat() {
    const load = Array.from(sessions.values()).reduce((sum, s) => sum + s.currentLoad(), 0);
    const info = { id: SERVER_ID, host: '127.0.0.1', port: PORT, load, activeSessions: sessions.size, updatedAt: Date.now() };
    await redis.set(`gameserver:${SERVER_ID}:info`, JSON.stringify(info), { EX: HEARTBEAT_TTL_SEC });
  }
  await heartbeat();
  setInterval(heartbeat, HEARTBEAT_MS);

  // ---- Listen for match assignments from the matchmaker ----
  await sub.subscribe(`gameserver:${SERVER_ID}:commands`, async (raw) => {
    let cmd;
    try {
      cmd = JSON.parse(raw);
    } catch (e) {
      return;
    }

    if (cmd.type === 'create_session') {
      console.log(`[${SERVER_ID}] Creating session ${cmd.sessionId} for players: ${cmd.players.map((p) => p.id)}`);
      const session = new GameSession(cmd.sessionId, cmd.players, async (endedId) => {
        sessions.delete(endedId);
        await redis.del(`session:${endedId}:server`);
        console.log(`[${SERVER_ID}] Session ${endedId} ended.`);
      });
      sessions.set(cmd.sessionId, session);
      await redis.set(`session:${cmd.sessionId}:server`, SERVER_ID, { EX: 60 * 10 });
    }
  });

  // ---- Player connections ----
  wss.on('connection', (ws) => {
    let boundSessionId = null;
    let boundPlayerId = null;

    ws.on('message', (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw);
      } catch (e) {
        return;
      }

      if (msg.type === 'join_session') {
        const session = sessions.get(msg.sessionId);
        if (!session) {
          ws.send(JSON.stringify({ type: 'join_failed', reason: 'session_not_found' }));
          return;
        }
        const playerSlot = session.players.get(msg.playerId);
        if (!playerSlot || playerSlot.token !== msg.token) {
          ws.send(JSON.stringify({ type: 'join_failed', reason: 'bad_token' }));
          return;
        }

        const ok = session.attachPlayer(msg.playerId, ws);
        if (ok) {
          boundSessionId = msg.sessionId;
          boundPlayerId = msg.playerId;
        }
        return;
      }

      // Any other message only makes sense once bound to a session
      if (boundSessionId != null) {
        const session = sessions.get(boundSessionId);
        if (session) session.handleMessage(boundPlayerId, msg);
      }
    });

    ws.on('close', () => {
      if (boundSessionId != null) {
        const session = sessions.get(boundSessionId);
        if (session) session.handleDisconnect(boundPlayerId);
      }
    });
  });
}

main().catch((err) => {
  console.error('Fatal error starting game server:', err);
  process.exit(1);
});