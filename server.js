// MAXX COCKPIT - relay server + AI analyst (Kimi / Moonshot)
// MT5 EA posts snapshots; dashboard receives via SSE; Kimi narrates on demand.
// Env vars: MAXX_KEY (EA auth), KIMI_API_KEY, KIMI_BASE_URL, KIMI_MODEL, AI_PIN

const express = require('express');
const path = require('path');

const app = express();
app.use(express.json({ limit: '256kb' }));
app.use(express.static(path.join(__dirname, 'public')));

const KEY = process.env.MAXX_KEY || '';
const AI_PIN = process.env.AI_PIN || '';
const KIMI_KEY = process.env.KIMI_API_KEY || '';
const KIMI_URL = (process.env.KIMI_BASE_URL || 'https://api.moonshot.ai/v1').replace(/\/+$/, '');
const KIMI_MODEL = process.env.KIMI_MODEL || 'moonshot-v1-8k';

const snapshots = {}; // symbol -> { data, at }
const clients = new Set();
const aiState = {};   // symbol -> { lastAutoAt }

function authOk(req) { if (!KEY) return true; return req.get('X-MAXX-KEY') === KEY; }
function pinOk(req)  { if (!AI_PIN) return true; return req.get('X-DASH-PIN') === AI_PIN; }

function broadcast(obj) {
  const msg = 'data: ' + JSON.stringify(obj) + '\n\n';
  for (const c of clients) { try { c.write(msg); } catch (e) { clients.delete(c); } }
}

// ---------- session stats in ET (for AI context) ----------
const hourFmt = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', hour: 'numeric', hour12: false });
function sessOf(ts) {
  const h = parseInt(hourFmt.format(new Date(ts * 1000)), 10) % 24;
  if (h >= 19 || h < 3) return 'ASIA';
  if (h < 8) return 'LONDON';
  if (h < 17) return 'NY';
  return 'OFF';
}
function sessionStats(bars) {
  if (!bars || bars.length < 4) return '';
  const segs = [];
  bars.forEach((b, i) => {
    const s = sessOf(b[0]);
    if (!segs.length || segs[segs.length - 1].s !== s) segs.push({ s, a: i, b: i });
    else segs[segs.length - 1].b = i;
  });
  const latest = {};
  segs.forEach(g => { if (g.s !== 'OFF') latest[g.s] = g; });
  const lines = [];
  for (const nm of ['ASIA', 'LONDON', 'NY']) {
    const g = latest[nm]; if (!g) continue;
    const open = g.a > 0 ? bars[g.a - 1][3] : bars[g.a][3];
    let hi = -1e18, lo = 1e18;
    for (let j = g.a; j <= g.b; j++) { hi = Math.max(hi, bars[j][1]); lo = Math.min(lo, bars[j][2]); }
    const net = bars[g.b][3] - open;
    lines.push(nm + ': net ' + (net >= 0 ? '+' : '') + net.toFixed(2)
      + ', max up +' + Math.max(0, hi - open).toFixed(2)
      + ', max down -' + Math.max(0, open - lo).toFixed(2)
      + (g.b === bars.length - 1 ? ' (LIVE now)' : ' (finished)'));
  }
  return lines.join('\n');
}

// ---------- compact live context for the AI ----------
function snapshotContext(sym) {
  const rec = snapshots[sym]; if (!rec) return null;
  const d = rec.data, c = d.checks || {}, dg = d.digits || 2;
  const f = v => Number(v || 0).toFixed(dg);
  const evs = (d.events || []).slice(0, 8).map(e =>
    e.t + ' ' + e.line + ' ' + e.type + (e.pts ? ' ' + f(e.pts) : '') + (e.running ? ' (running)' : '')
  ).join('\n');
  const compsOk = c.stackOk && c.emaOk && c.sarOk;
  const state = compsOk
    ? (c.inZone100 ? (c.bounceConfirm ? 'READY (all checklist passed)' : 'IN ZONE - waiting bounce confirmation candle')
                   : 'ZONE WATCH - waiting pullback to WMA100')
    : 'WAIT - components not aligned';
  return [
    'SYMBOL: ' + sym + '  BID: ' + f(d.bid) + '  (broker server time ' + d.time + ')',
    'H4 DAILY BIAS: ' + (d.h4.biasBuy ? 'BUY only (price closed above H4 WMA50)' : 'SELL only (price closed below H4 WMA50)')
      + '  H4WMA50=' + f(d.h4.wma50) + '  dist=' + f(d.h4.dist),
    'M15 LINES: WMA50=' + f(d.lines.WMA50) + ' WMA89=' + f(d.lines.WMA89) + ' WMA100=' + f(d.lines.WMA100)
      + ' WMA144=' + f(d.lines.WMA144) + ' EMA200=' + f(d.lines.EMA200) + ' WMA800=' + f(d.lines.WMA800),
    'SAR: ' + (d.sar.up ? 'below price (uptrend)' : 'above price (downtrend)') + ' val=' + f(d.sar.val) + ' bars since flip=' + d.sar.bars,
    'CHECKLIST: stackOk=' + !!c.stackOk + ' emaOk=' + !!c.emaOk + ' sarOk=' + !!c.sarOk
      + ' inZoneWMA100=' + !!c.inZone100 + ' bounceConfirm=' + !!c.bounceConfirm
      + ' distToWMA100=' + f(c.dist100) + ' zoneTol=' + f(d.zoneTol),
    'SYSTEM STATE: ' + state,
    'SESSIONS (ET):\n' + (sessionStats(d.bars) || 'no bar data (EA v0.3+ required)'),
    'LINE TOUCH HISTORY (broker server time, newest first):\n' + (evs || 'none')
  ].join('\n');
}

const SYSTEM_PROMPT = [
  'คุณคือผู้ช่วยนักวิเคราะห์ประจำ MAXX COCKPIT (ระบบเทรดมือ WMA Bundle + SAR:',
  'bias รายวันมาจากราคาปิดแท่ง H4 เทียบ H4 WMA50, setup หลักคือรอราคาย่อแตะโซน WMA100 บน M15 แล้วมีแท่งเด้งยืนยัน)',
  'กติกาเหล็ก:',
  '1) คุณเป็นผู้บรรยายข้อมูล ไม่ใช่ผู้ตัดสินใจ ห้ามแนะนำให้เข้าออเดอร์ถ้า checklist ยังไม่ครบ และห้ามแนะนำสวนทิศ bias เด็ดขาด',
  '2) ถ้า SYSTEM STATE เป็น WAIT ให้สรุปสถานการณ์และย้ำว่ารอ',
  '3) ใช้เฉพาะตัวเลขจากข้อมูลที่ได้รับ ห้ามเดาหรือแต่งข้อมูล ถ้าไม่มีข้อมูลให้บอกว่าไม่มี',
  '4) ตอบภาษาไทย กระชับ ตรงประเด็น'
].join('\n');

async function askKimi(messages, maxTokens) {
  if (!KIMI_KEY) throw new Error('KIMI_API_KEY not set (Railway > Variables)');
  const thinkingOff = (process.env.KIMI_THINKING || 'disabled').toLowerCase() === 'disabled';
  const payload = {
    model: KIMI_MODEL, messages, max_tokens: maxTokens,
    // K2.6 enforces exact values: 0.6 in instant mode, 1 in thinking mode
    temperature: Number(process.env.KIMI_TEMPERATURE || (thinkingOff ? 0.6 : 1))
  };
  // Our job is narration of supplied numbers - no deep reasoning needed.
  // Disable thinking so K2.6 answers directly (KIMI_THINKING=enabled to turn back on).
  if (thinkingOff)
    payload.thinking = { type: 'disabled' };

  async function call(body) {
    return fetch(KIMI_URL + '/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + KIMI_KEY },
      body: JSON.stringify(body)
    });
  }

  let r = await call(payload);
  if (!r.ok && r.status === 400 && payload.thinking) {
    // account/endpoint may not accept the thinking param - retry without it
    const t = await r.text();
    if (/thinking/i.test(t)) { delete payload.thinking; r = await call(payload); }
    else throw new Error('Kimi API 400: ' + t.slice(0, 200));
  }
  if (!r.ok) {
    const t = await r.text();
    throw new Error('Kimi API ' + r.status + ': ' + t.slice(0, 200));
  }
  const j = await r.json();
  const msg = (j.choices && j.choices[0] && j.choices[0].message) || {};
  let text = '';
  if (typeof msg.content === 'string') text = msg.content;
  else if (Array.isArray(msg.content)) text = msg.content.map(p => (p && p.text) || '').join('');
  text = (text || '').trim();
  if (!text) {
    const fr = (j.choices && j.choices[0] && j.choices[0].finish_reason) || '?';
    throw new Error('empty answer (finish_reason=' + fr + (msg.reasoning_content ? ', model spent budget thinking' : '') + ') - try again');
  }
  return text;
}

// ---------- MT5 snapshot in ----------
app.post('/api/snapshot', (req, res) => {
  if (!authOk(req)) return res.status(401).json({ ok: false, error: 'bad key' });
  const d = req.body;
  if (!d || !d.sym) return res.status(400).json({ ok: false, error: 'missing sym' });
  const prev = snapshots[d.sym] && snapshots[d.sym].data;
  const rec = { data: d, at: Date.now() };
  snapshots[d.sym] = rec;
  broadcast({ sym: d.sym, at: rec.at, data: d });
  res.json({ ok: true });
  maybeAutoComment(d.sym, prev, d); // fire and forget
});

// ---------- AI auto commentary on key transitions ----------
function transition(prev, d) {
  if (!prev) return null;
  const p = prev.checks || {}, c = d.checks || {};
  if (prev.h4 && d.h4 && prev.h4.biasBuy !== d.h4.biasBuy)
    return 'H4 candle closed and daily bias flipped to ' + (d.h4.biasBuy ? 'BUY only' : 'SELL only');
  if (prev.sar && d.sar && prev.sar.up !== d.sar.up)
    return 'Parabolic SAR flipped to ' + (d.sar.up ? 'below price (uptrend)' : 'above price (downtrend)');
  if (!p.inZone100 && c.inZone100)
    return 'Price entered the WMA100 zone';
  if (!p.bounceConfirm && c.bounceConfirm && c.inZone100)
    return 'Bounce confirmation candle formed at WMA100 - checklist setup complete';
  return null;
}

async function maybeAutoComment(sym, prev, d) {
  if (!KIMI_KEY) return;
  const ev = transition(prev, d);
  if (!ev) return;
  const st = aiState[sym] || (aiState[sym] = { lastAutoAt: 0 });
  if (Date.now() - st.lastAutoAt < 90000) return; // 90s cooldown per symbol
  st.lastAutoAt = Date.now();
  try {
    const text = await askKimi([
      { role: 'system', content: SYSTEM_PROMPT },
      { role: 'user', content: 'เหตุการณ์ที่เพิ่งเกิด: ' + ev + '\n\nข้อมูลปัจจุบัน:\n' + snapshotContext(sym)
        + '\n\nเขียนคำบรรยายเหตุการณ์นี้สำหรับ feed ยาว 1-2 ประโยคเท่านั้น' }
    ], 800);
    if (text) broadcast({ type: 'ai', sym, at: Date.now(), text });
  } catch (e) { console.error('auto comment failed:', e.message); }
}

// ---------- AI on-demand endpoints ----------
app.post('/api/ai/summary', async (req, res) => {
  if (!pinOk(req)) return res.status(401).json({ ok: false, error: 'bad pin' });
  const sym = (req.body && req.body.sym) || Object.keys(snapshots)[0];
  const ctx = snapshotContext(sym);
  if (!ctx) return res.status(400).json({ ok: false, error: 'no live data for ' + sym });
  try {
    const text = await askKimi([
      { role: 'system', content: SYSTEM_PROMPT },
      { role: 'user', content: 'สรุปสถานการณ์ตอนนี้ 4-6 ประโยค: แต่ละ session ที่ผ่านมาเป็นยังไง ตอนนี้ราคาอยู่ตรงไหนเทียบกับเส้นและ bias ระบบอยู่สถานะไหน และต้องรออะไรต่อ\n\n' + ctx }
    ], 2000);
    res.json({ ok: true, text });
  } catch (e) { res.status(502).json({ ok: false, error: e.message }); }
});

app.post('/api/ai/chat', async (req, res) => {
  if (!pinOk(req)) return res.status(401).json({ ok: false, error: 'bad pin' });
  const b = req.body || {};
  const sym = b.sym || Object.keys(snapshots)[0];
  const ctx = snapshotContext(sym) || 'no live data yet';
  let hist = Array.isArray(b.messages) ? b.messages.slice(-12) : [];
  hist = hist
    .filter(m => m && (m.role === 'user' || m.role === 'assistant') && typeof m.content === 'string')
    .map(m => ({ role: m.role, content: m.content.slice(0, 2000) }))
    .filter(m => m.content.trim().length > 0);
  if (!hist.length || hist[hist.length - 1].role !== 'user')
    return res.status(400).json({ ok: false, error: 'last message must be from user' });
  try {
    const text = await askKimi([
      { role: 'system', content: SYSTEM_PROMPT + '\n\nข้อมูลสดตอนนี้:\n' + ctx },
      ...hist
    ], 2000);
    res.json({ ok: true, text });
  } catch (e) { res.status(502).json({ ok: false, error: e.message }); }
});

app.get('/api/ai/status', (req, res) =>
  res.json({ enabled: !!KIMI_KEY, model: KIMI_MODEL, pinRequired: !!AI_PIN }));

// ---------- dashboard data ----------
app.get('/api/snapshots', (req, res) => {
  const out = {};
  for (const sym of Object.keys(snapshots)) out[sym] = { at: snapshots[sym].at, data: snapshots[sym].data };
  res.json(out);
});

app.get('/api/stream', (req, res) => {
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' });
  res.flushHeaders();
  for (const sym of Object.keys(snapshots))
    res.write('data: ' + JSON.stringify({ sym, at: snapshots[sym].at, data: snapshots[sym].data }) + '\n\n');
  clients.add(res);
  const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch (e) {} }, 15000);
  req.on('close', () => { clearInterval(ping); clients.delete(res); });
});

app.get('/health', (req, res) => res.json({
  ok: true,
  symbols: Object.keys(snapshots),
  key: KEY ? 'set' : 'OPEN - set MAXX_KEY!',
  ai: KIMI_KEY ? ('enabled (' + KIMI_MODEL + ')') : 'disabled - set KIMI_API_KEY'
}));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('MAXX COCKPIT relay on :' + PORT));
