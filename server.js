// MAXX COCKPIT - relay server + AI analyst (Kimi / Moonshot)
// MT5 EA posts snapshots; dashboard receives via SSE; Kimi narrates on demand.
// Env vars: MAXX_KEY (EA auth), KIMI_API_KEY, KIMI_BASE_URL, KIMI_MODEL, AI_PIN

const express = require('express');
const path = require('path');
const fs = require('fs');

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

// global AI rate limit - caps quota burn if the URL leaks (AI_HOURLY_LIMIT to adjust)
const aiCalls = [];
function aiRateOk() {
  const now = Date.now();
  while (aiCalls.length && now - aiCalls[0] > 3600000) aiCalls.shift();
  if (aiCalls.length >= Number(process.env.AI_HOURLY_LIMIT || 40)) return false;
  aiCalls.push(now);
  return true;
}

function broadcast(obj) {
  const msg = 'data: ' + JSON.stringify(obj) + '\n\n';
  for (const c of clients) { try { c.write(msg); } catch (e) { clients.delete(c); } }
}

// ---------- persistent research log (Railway Volume at DATA_DIR) ----------
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (e) {}

function logAppend(file, obj) {
  try { fs.appendFileSync(path.join(DATA_DIR, file), JSON.stringify(obj) + '\n'); }
  catch (e) { console.error('log append failed:', e.message); }
}
function readTail(file, maxLines) {
  try {
    const lines = fs.readFileSync(path.join(DATA_DIR, file), 'utf8').split('\n').filter(Boolean);
    return lines.slice(-maxLines).map(l => { try { return JSON.parse(l); } catch (e) { return null; } }).filter(Boolean);
  } catch (e) { return []; }
}

const recSt = {}; // per-symbol recorder state
function recState(sym) {
  if (!recSt[sym]) {
    const seen = new Set();
    for (const e of readTail('events.jsonl', 600)) if (e.sym === sym && e.key) seen.add(e.key);
    recSt[sym] = { seen, sarFlip: null, biasFlip: null, lastStructHour: -1 };
  }
  return recSt[sym];
}
function stackStr(d) {
  const rows = Object.keys(d.lines || {}).map(nm => ({ nm, v: d.lines[nm] }));
  rows.push({ nm: 'PRICE', v: d.bid });
  rows.sort((a, b) => b.v - a.v);
  return rows.map(r => r.nm).join('>');
}
function record(sym, prev, d) {
  try {
    const st = recState(sym);
    const now = Date.now();
    // finalized line-touch events with market structure at record time
    for (const ev of (d.events || [])) {
      if (!ev.ts || ev.running || ev.type === 'testing') continue;
      const key = ev.ts + '|' + ev.line + '|' + ev.type;
      if (st.seen.has(key)) continue;
      st.seen.add(key);
      if (st.seen.size > 1200) st.seen = new Set(Array.from(st.seen).slice(-600));
      logAppend('events.jsonl', { at: now, sym, key, ts: ev.ts, line: ev.line, type: ev.type, pts: ev.pts,
        ctx: { biasBuy: !!(d.h4 && d.h4.biasBuy), sarUp: !!(d.sar && d.sar.up), stack: stackStr(d), bid: d.bid } });
    }
    // SAR regimes: from first dot to next flip -> was the first dot right?
    if (d.sar) {
      if (!st.sarFlip) st.sarFlip = { at: now, price: d.bid, up: d.sar.up };
      else if (prev && prev.sar && prev.sar.up !== d.sar.up) {
        const f = st.sarFlip;
        const net = f.up ? (d.bid - f.price) : (f.price - d.bid);
        logAppend('sar.jsonl', { at: now, sym, start: f.at, up: f.up, entry: f.price, exit: d.bid,
          bars: (prev.sar.bars || 0), net: Number(net.toFixed(5)), win: net > 0 });
        st.sarFlip = { at: now, price: d.bid, up: d.sar.up };
      }
    }
    // bias regimes
    if (d.h4) {
      if (!st.biasFlip) st.biasFlip = { at: now, price: d.bid, buy: d.h4.biasBuy };
      else if (prev && prev.h4 && prev.h4.biasBuy !== d.h4.biasBuy) {
        const f = st.biasFlip;
        const net = f.buy ? (d.bid - f.price) : (f.price - d.bid);
        logAppend('bias.jsonl', { at: now, sym, start: f.at, buy: f.buy, entry: f.price, exit: d.bid,
          net: Number(net.toFixed(5)), win: net > 0 });
        st.biasFlip = { at: now, price: d.bid, buy: d.h4.biasBuy };
      }
    }
    // hourly market-structure snapshot
    const hour = Math.floor(now / 3600000);
    if (hour !== st.lastStructHour) {
      st.lastStructHour = hour;
      logAppend('structure.jsonl', { at: now, sym, bid: d.bid, stack: stackStr(d),
        biasBuy: !!(d.h4 && d.h4.biasBuy), sarUp: !!(d.sar && d.sar.up),
        sarBars: (d.sar && d.sar.bars) || 0, dist100: (d.checks && d.checks.dist100) });
    }
  } catch (e) { console.error('record failed:', e.message); }
}

function statsSummary(sym, days) {
  const cutoff = Date.now() - days * 86400000;
  const evs = readTail('events.jsonl', 4000).filter(e => e.sym === sym && e.at >= cutoff);
  const perLine = {};
  for (const e of evs) {
    const L = perLine[e.line] || (perLine[e.line] = { bounce: 0, brk: 0, ptsSum: 0 });
    if (e.type === 'bounce') { L.bounce++; L.ptsSum += (e.pts || 0); } else L.brk++;
  }
  const lineText = Object.keys(perLine).map(nm => {
    const L = perLine[nm];
    const total = L.bounce + L.brk;
    return nm + ': bounce ' + L.bounce + ', break ' + L.brk
      + ', hold ' + (total ? Math.round(100 * L.bounce / total) : 0) + '%'
      + ', avg bounce ' + (L.bounce ? (L.ptsSum / L.bounce).toFixed(2) : '0');
  });
  const sar = readTail('sar.jsonl', 2000).filter(e => e.sym === sym && e.at >= cutoff);
  const bias = readTail('bias.jsonl', 500).filter(e => e.sym === sym && e.at >= cutoff);
  const avg = (arr, k) => arr.length ? arr.reduce((a, e) => a + (e[k] || 0), 0) / arr.length : 0;
  return {
    sym, days, lines: perLine, lineText,
    sar: { regimes: sar.length,
      firstDotWinRate: sar.length ? Math.round(100 * sar.filter(e => e.win).length / sar.length) : null,
      avgBars: Math.round(avg(sar, 'bars')), avgNet: Number(avg(sar, 'net').toFixed(2)) },
    bias: { regimes: bias.length,
      winRate: bias.length ? Math.round(100 * bias.filter(e => e.win).length / bias.length) : null,
      avgNet: Number(avg(bias, 'net').toFixed(2)) },
    structureSamples: readTail('structure.jsonl', 3000).filter(e => e.sym === sym && e.at >= cutoff).length,
    persisted: !!process.env.DATA_DIR
  };
}
function statsText(sym, days) {
  const t = statsSummary(sym, days);
  return [
    'LINE STATS: ' + (t.lineText.length ? t.lineText.join(' | ') : 'no records yet'),
    'SAR REGIMES: ' + (t.sar.regimes ? (t.sar.regimes + ' flips, first-dot win rate ' + t.sar.firstDotWinRate + '%, avg ' + t.sar.avgBars + ' bars/regime, avg net ' + t.sar.avgNet) : 'no records yet'),
    'H4 BIAS REGIMES: ' + (t.bias.regimes ? (t.bias.regimes + ' flips, win rate ' + t.bias.winRate + '%, avg net ' + t.bias.avgNet) : 'no records yet')
  ].join('\n');
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
    (d.pd && d.pd.h ? ('PREV DAY LEVELS: PDH=' + f(d.pd.h) + ' PDL=' + f(d.pd.l) + ' - price ' + (d.bid >= d.pd.h ? 'ABOVE PDH by ' + f(d.bid - d.pd.h) : (d.bid <= d.pd.l ? 'BELOW PDL by ' + f(d.pd.l - d.bid) : 'inside range (' + f(d.bid - d.pd.l) + ' above PDL, ' + f(d.pd.h - d.bid) + ' below PDH)'))) : 'PREV DAY LEVELS: not available (EA v0.4 needed)'),
    'CHECKLIST: stackOk=' + !!c.stackOk + ' emaOk=' + !!c.emaOk + ' sarOk=' + !!c.sarOk
      + ' inZoneWMA100=' + !!c.inZone100 + ' bounceConfirm=' + !!c.bounceConfirm
      + ' distToWMA100=' + f(c.dist100) + ' zoneTol=' + f(d.zoneTol),
    'SYSTEM STATE: ' + state,
    'SESSIONS (ET):\n' + (sessionStats(d.bars) || 'no bar data (EA v0.3+ required)'),
    'LINE TOUCH HISTORY (broker server time, newest first):\n' + (evs || 'none'),
    'RECORDED RESEARCH STATS (last 7 days, logged by this cockpit):\n' + statsText(sym, 7)
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
  record(d.sym, prev, d);
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
  if (!aiRateOk()) return res.status(429).json({ ok: false, error: 'AI rate limit reached - try again later' });
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
  if (!aiRateOk()) return res.status(429).json({ ok: false, error: 'AI rate limit reached - try again later' });
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

app.get('/api/stats', (req, res) => {
  const sym = req.query.sym || Object.keys(snapshots)[0] || '';
  const days = Math.min(90, Math.max(1, Number(req.query.days) || 7));
  res.json(statsSummary(sym, days));
});

app.get('/health', (req, res) => res.json({
  ok: true,
  symbols: Object.keys(snapshots),
  key: KEY ? 'set' : 'OPEN - set MAXX_KEY!',
  ai: KIMI_KEY ? ('enabled (' + KIMI_MODEL + ')') : 'disabled - set KIMI_API_KEY',
  research: process.env.DATA_DIR ? ('persisted at ' + DATA_DIR) : 'EPHEMERAL - attach Railway Volume and set DATA_DIR=/data'
}));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('MAXX COCKPIT relay on :' + PORT));
