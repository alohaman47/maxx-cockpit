// MAXX COCKPIT - relay server
// MT5 EA posts snapshots here; dashboard receives them via SSE.
// Stateless by design: EA resends full state, so Railway restarts are safe.

const express = require('express');
const path = require('path');

const app = express();
app.use(express.json({ limit: '256kb' }));
app.use(express.static(path.join(__dirname, 'public')));

const KEY = process.env.MAXX_KEY || '';
const snapshots = {}; // symbol -> { data, at }
const clients = new Set();

function authOk(req) {
  if (!KEY) return true; // no key set = open (set MAXX_KEY on Railway!)
  return req.get('X-MAXX-KEY') === KEY;
}

app.post('/api/snapshot', (req, res) => {
  if (!authOk(req)) return res.status(401).json({ ok: false, error: 'bad key' });
  const d = req.body;
  if (!d || !d.sym) return res.status(400).json({ ok: false, error: 'missing sym' });
  const rec = { data: d, at: Date.now() };
  snapshots[d.sym] = rec;
  const msg = 'data: ' + JSON.stringify({ sym: d.sym, at: rec.at, data: d }) + '\n\n';
  for (const c of clients) {
    try { c.write(msg); } catch (e) { clients.delete(c); }
  }
  res.json({ ok: true });
});

app.get('/api/snapshots', (req, res) => {
  const out = {};
  for (const sym of Object.keys(snapshots)) {
    out[sym] = { at: snapshots[sym].at, data: snapshots[sym].data };
  }
  res.json(out);
});

app.get('/api/stream', (req, res) => {
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });
  res.flushHeaders();
  for (const sym of Object.keys(snapshots)) {
    res.write('data: ' + JSON.stringify({
      sym, at: snapshots[sym].at, data: snapshots[sym].data
    }) + '\n\n');
  }
  clients.add(res);
  const ping = setInterval(() => {
    try { res.write(': ping\n\n'); } catch (e) {}
  }, 15000);
  req.on('close', () => { clearInterval(ping); clients.delete(res); });
});

app.get('/health', (req, res) => {
  res.json({ ok: true, symbols: Object.keys(snapshots), key: KEY ? 'set' : 'OPEN - set MAXX_KEY!' });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('MAXX COCKPIT relay on :' + PORT));
