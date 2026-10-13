// Test-only boundary control for the locked actual Worker fixture. The real
// return route has read its revision before sign() and commits its D1 batch
// afterwards. Cancelling a client does not abort this fixture's Worker Request.
import { createServer } from 'node:http';

if (process.env.UNIROUTE_WALLET_WIRE_FIXTURE_PORT !== '62070') {
  throw new Error('Funding race control requires the isolated Worker port');
}
const originalSign = crypto.subtle.sign.bind(crypto.subtle);
const gates = new Map();
crypto.subtle.sign = async function (...args) {
  let payload;
  try { payload = JSON.parse(new TextDecoder().decode(args[2])); } catch {}
  const gate = Array.isArray(payload) && payload[0] === 'wallet-funding-return-v1' && payload[20] === 'close'
    ? gates.get(payload[3]) : undefined;
  if (gate && gate.arrived < 2) {
    const ordinal = ++gate.arrived;
    if (ordinal === 1) await gate.first;
    else { gate.releaseFirst(); await gate.second; }
  }
  return originalSign(...args);
};
const server = createServer(async (request, response) => {
  try {
    if (request.method !== 'POST') { response.writeHead(405); response.end(); return; }
    let raw = '', size = 0;
    for await (const chunk of request) {
      size += chunk.length;
      if (size > 2048) throw new Error('Race control body exceeds bound');
      raw += chunk;
    }
    const body = JSON.parse(raw);
    if (typeof body.lease !== 'string' || !/^[a-zA-Z0-9_:.\-]{1,128}$/.test(body.lease)) throw new Error('Invalid fixture lease');
    if (request.url === '/arm') {
      if (gates.has(body.lease)) throw new Error('Race control already armed');
      let releaseFirst, releaseSecond;
      const first = new Promise(resolve => { releaseFirst = resolve; });
      const second = new Promise(resolve => { releaseSecond = resolve; });
      gates.set(body.lease, { arrived: 0, first, second, releaseFirst, releaseSecond });
    }
    const gate = gates.get(body.lease);
    if (!gate) throw new Error('Race control unavailable');
    if (request.url === '/resume-second') gate.releaseSecond();
    else if (request.url === '/release') { gate.releaseFirst(); gate.releaseSecond(); }
    else if (!['/arm', '/state'].includes(request.url)) throw new Error('Unknown race control operation');
    response.setHeader('Content-Type', 'application/json');
    response.end(JSON.stringify({ arrived: gate.arrived }));
  } catch (error) {
    response.writeHead(400); response.end(JSON.stringify({ error: String(error) }));
  }
});
await new Promise(resolve => server.listen(62071, '127.0.0.1', resolve));

let stopping = false;
function stopRaceControls() {
  if (stopping) return;
  stopping = true;
  for (const gate of gates.values()) { gate.releaseFirst(); gate.releaseSecond(); }
  server.close();
}
process.on('SIGTERM', stopRaceControls);
process.on('SIGINT', stopRaceControls);
