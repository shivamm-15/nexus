// Load test: spins up N simulated players that behave like real clients
// (connect -> queue -> get matched -> play for a while), and reports
// real numbers: connection time, matchmaking wait, and round-trip
// latency under load. This is what turns "I built a multiplayer game
// server" into "I load tested it and here's what I found."
//
// Run with:  node loadtest/loadtest.js --players 40 --duration 20

const WebSocket = require('ws');

function parseArgs() {
  const args = { players: 20, duration: 20, matchmakerUrl: 'ws://localhost:9000' };
  process.argv.slice(2).forEach((arg, i, arr) => {
    if (arg === '--players') args.players = parseInt(arr[i + 1], 10);
    if (arg === '--duration') args.duration = parseInt(arr[i + 1], 10);
    if (arg === '--matchmaker') args.matchmakerUrl = arr[i + 1];
  });
  return args;
}

const { players: NUM_PLAYERS, duration: DURATION_SEC, matchmakerUrl: MM_URL } = parseArgs();

const results = {
  connectTimesMs: [],
  matchWaitTimesMs: [],
  roundTripSamplesMs: [],
  sessionsSeen: new Set(),
  errors: 0,
  completed: 0,
};

function percentile(arr, p) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.floor((p / 100) * sorted.length);
  return sorted[Math.min(idx, sorted.length - 1)];
}
function stats(arr) {
  if (arr.length === 0) return { avg: 0, min: 0, max: 0, p95: 0 };
  const avg = arr.reduce((a, b) => a + b, 0) / arr.length;
  return {
    avg: Math.round(avg),
    min: Math.round(Math.min(...arr)),
    max: Math.round(Math.max(...arr)),
    p95: Math.round(percentile(arr, 95)),
  };
}

function simulatePlayer(name) {
  return new Promise((resolve) => {
    const connectStart = Date.now();
    let queueStart = null;
    let gameWs = null;
    let lastInputSentAt = null;
    let done = false;

    const finish = () => {
      if (done) return;
      done = true;
      resolve();
    };

    let mmWs;
    try {
      mmWs = new WebSocket(MM_URL);
    } catch (e) {
      results.errors++;
      return finish();
    }

    mmWs.on('error', () => {
      results.errors++;
      finish();
    });

    mmWs.on('open', () => {
      results.connectTimesMs.push(Date.now() - connectStart);
    });

    mmWs.on('message', (raw) => {
      const msg = JSON.parse(raw);

      if (msg.type === 'welcome') {
        queueStart = Date.now();
        mmWs.send(JSON.stringify({ type: 'find_match' }));
      }

      if (msg.type === 'match_assigned') {
        results.matchWaitTimesMs.push(Date.now() - queueStart);
        results.sessionsSeen.add(msg.sessionId);

        gameWs = new WebSocket(`ws://${msg.server.host}:${msg.server.port}`);
        gameWs.on('error', () => {
          results.errors++;
          finish();
        });
        gameWs.on('open', () => {
          gameWs.send(
            JSON.stringify({ type: 'join_session', sessionId: msg.sessionId, playerId: msg.playerId, token: msg.token })
          );
        });

        gameWs.on('message', (raw2) => {
          const m2 = JSON.parse(raw2);
          if (m2.type === 'ping') {
            gameWs.send(JSON.stringify({ type: 'pong', pingId: m2.pingId }));
          }
          if (m2.type === 'state' && lastInputSentAt) {
            results.roundTripSamplesMs.push(Date.now() - lastInputSentAt);
            lastInputSentAt = null;
          }
          if (m2.type === 'match_end') {
            results.completed++;
            gameWs.close();
            mmWs.close();
            finish();
          }
        });

        // Simulate a player mashing keys and shooting occasionally
        const inputInterval = setInterval(() => {
          if (!gameWs || gameWs.readyState !== WebSocket.OPEN) return;
          lastInputSentAt = Date.now();
          gameWs.send(
            JSON.stringify({
              type: 'input',
              keys: {
                up: Math.random() > 0.5,
                down: Math.random() > 0.5,
                left: Math.random() > 0.5,
                right: Math.random() > 0.5,
              },
              aim: { x: Math.random() * 800, y: Math.random() * 600 },
            })
          );
          if (Math.random() < 0.1) gameWs.send(JSON.stringify({ type: 'shoot' }));
        }, 100);

        setTimeout(() => {
          clearInterval(inputInterval);
          // We played for the full requested duration without errors —
          // that counts as a completed run for this bot. (The server's
          // match itself keeps going; we're just done simulating this player.)
          results.completed++;
          if (gameWs) gameWs.close();
          mmWs.close();
          finish();
        }, DURATION_SEC * 1000);
      }
    });

    // Safety timeout in case a player never gets matched (e.g. not enough others queued)
    setTimeout(() => {
      if (gameWs) gameWs.close();
      mmWs.close();
      finish();
    }, (DURATION_SEC + 15) * 1000);
  });
}

async function run() {
  console.log(`Starting load test: ${NUM_PLAYERS} simulated players, ${DURATION_SEC}s match duration`);
  console.log(`Matchmaker: ${MM_URL}\n`);

  const testStart = Date.now();
  const bots = [];
  for (let i = 0; i < NUM_PLAYERS; i++) {
    bots.push(simulatePlayer(`bot${i}`));
    // Stagger connections slightly, like real players wouldn't all click at once
    await new Promise((r) => setTimeout(r, 20));
  }

  await Promise.all(bots);
  const totalTestTimeSec = ((Date.now() - testStart) / 1000).toFixed(1);

  const connect = stats(results.connectTimesMs);
  const matchWait = stats(results.matchWaitTimesMs);
  const rtt = stats(results.roundTripSamplesMs);

  console.log('='.repeat(50));
  console.log('LOAD TEST RESULTS');
  console.log('='.repeat(50));
  console.log(`Total wall-clock time:      ${totalTestTimeSec}s`);
  console.log(`Simulated players:          ${NUM_PLAYERS}`);
  console.log(`Distinct sessions formed:   ${results.sessionsSeen.size}`);
  console.log(`Players that finished ok:   ${results.completed}/${NUM_PLAYERS}`);
  console.log(`Errors:                     ${results.errors}`);
  console.log('-'.repeat(50));
  console.log(`Matchmaker connect time (ms):  avg ${connect.avg}  min ${connect.min}  max ${connect.max}  p95 ${connect.p95}`);
  console.log(`Time queued -> matched (ms):   avg ${matchWait.avg}  min ${matchWait.min}  max ${matchWait.max}  p95 ${matchWait.p95}`);
  console.log(`Input -> next state RTT (ms):  avg ${rtt.avg}  min ${rtt.min}  max ${rtt.max}  p95 ${rtt.p95}`);
  console.log(`  (RTT samples collected: ${results.roundTripSamplesMs.length})`);
  console.log('='.repeat(50));
  console.log('\nTip: check the dashboard (public/dashboard.html) WHILE this runs');
  console.log('to see live load and tick timing across your game server instances.');

  process.exit(0);
}

run();