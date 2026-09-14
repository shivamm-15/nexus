// Small helper so every process connects to Redis the same way.
// Redis is our "shared brain" across processes: it's the only thing
// the matchmaker and the game servers both know how to talk to,
// even though they're separate Node processes (possibly on separate
// machines in a real deployment).

const { createClient } = require('redis');

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';

async function makeRedisClient(label) {
  const client = createClient({ url: REDIS_URL });
  client.on('error', (err) => console.error(`[redis:${label}] error`, err.message));
  await client.connect();
  return client;
}

module.exports = { makeRedisClient };