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

// ---------- economic calendar (ForexFactory weekly feed) ----------
let newsCache = { at: 0, events: [] };
async function fetchNews() {
  if (Date.now() - newsCache.at < 45 * 60000 && newsCache.events.length) return;
  try {
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), 10000);
    const r = await fetch('https://nfs.faireconomy.media/ff_calendar_thisweek.json', { signal: ac.signal });
    clearTimeout(timer);
    if (!r.ok) return;
    const j = await r.json();
    newsCache = {
      at: Date.now(),
      events: (Array.isArray(j) ? j : []).map(e => ({
        t: Date.parse(e.date), title: e.title, ccy: e.country, impact: e.impact
      })).filter(e => !isNaN(e.t) && (e.impact === 'High' || e.impact === 'Medium'))
    };
    console.log('news calendar loaded:', newsCache.events.length, 'events');
  } catch (e) { console.error('news fetch failed:', e.message); }
}
setInterval(fetchNews, 10 * 60000);
fetchNews();

function newsCcy(sym) {
  const su = (sym || '').toUpperCase();
  const out = new Set(['USD']);
  ['EUR', 'GBP', 'JPY', 'AUD', 'NZD', 'CAD', 'CHF'].forEach(c => { if (su.includes(c)) out.add(c); });
  return out;
}
function newsFor(sym, horizonH) {
  const now = Date.now(), ccy = newsCcy(sym);
  return newsCache.events
    .filter(e => ccy.has(e.ccy) && e.t > now - 30 * 60000 && e.t < now + horizonH * 3600000)
    .sort((a, b) => a.t - b.t);
}
function newsLockFor(sym) {
  const now = Date.now();
  return newsFor(sym, 1).find(e => e.impact === 'High' && now >= e.t - 15 * 60000 && now <= e.t + 15 * 60000) || null;
}
// signed minutes to nearest High-impact event within +/-120min, else null
function newsProximity(sym) {
  const now = Date.now(), ccy = newsCcy(sym);
  let best = null;
  for (const e of newsCache.events) {
    if (e.impact !== 'High' || !ccy.has(e.ccy)) continue;
    const m = Math.round((e.t - now) / 60000);
    if (Math.abs(m) <= 120 && (best === null || Math.abs(m) < Math.abs(best))) best = m;
  }
  return best;
}

// ---------- confluence score (same formula as dashboard) ----------
function confluenceOf(sym, d) {
  const c = d.checks || {}, bb = d.h4 && d.h4.biasBuy;
  let sc = 0;
  if (c.stackOk) sc += 25;
  if (c.emaOk) sc += 10;
  if (c.sarOk) sc += 15;
  const tol = d.zoneTol || 2, ad = Math.abs(c.dist100 || 999);
  if (c.inZone100) sc += 20; else if (ad <= 2 * tol) sc += 10; else if (ad <= 4 * tol) sc += 5;
  const evs = (d.events || []).filter(e => e.line === 'WMA100' && e.type !== 'testing');
  if (evs.length) sc += Math.round(15 * evs.filter(e => e.type === 'bounce').length / evs.length);
  else sc += 7;
  if (d.pd && d.pd.h && d.lines && d.lines.WMA100) {
    const near = bb ? Math.abs(d.lines.WMA100 - d.pd.l) : Math.abs(d.lines.WMA100 - d.pd.h);
    if (near <= 3 * tol) sc += 10;
  }
  if (newsLockFor(sym)) sc = Math.max(0, sc - 25);
  return Math.min(100, sc);
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
    const conf = confluenceOf(sym, d);
    // finalized line-touch events with market structure at record time
    for (const ev of (d.events || [])) {
      if (!ev.ts || ev.running || ev.type === 'testing') continue;
      const key = ev.ts + '|' + ev.line + '|' + ev.type;
      if (st.seen.has(key)) continue;
      st.seen.add(key);
      if (st.seen.size > 1200) st.seen = new Set(Array.from(st.seen).slice(-600));
      logAppend('events.jsonl', { at: now, sym, key, ts: ev.ts, line: ev.line, type: ev.type, pts: ev.pts,
        ctx: { biasBuy: !!(d.h4 && d.h4.biasBuy), sarUp: !!(d.sar && d.sar.up), stack: stackStr(d), bid: d.bid, conf,
               spread: (typeof d.spread === 'number' ? d.spread : null),
               newsMin: newsProximity(sym), newsLock: !!newsLockFor(sym) } });
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
    // account deals -> trade attribution log (real trades with setup context)
    if (Array.isArray(d.deals) && d.deals.length) {
      if (!st.dealSeen)
        st.dealSeen = new Set(readTail('trades.jsonl', 800).map(t => t.id).filter(Boolean));
      for (const dl of d.deals) {
        if (!dl.id || st.dealSeen.has(dl.id)) continue;
        st.dealSeen.add(dl.id);
        const base = { at: now, id: dl.id, pos: dl.pos, sym: dl.symd || sym, dir: dl.dir,
                       price: dl.price, lot: dl.lot, t: dl.t };
        if (dl.e === 'in') {
          if (!st.openTrades) st.openTrades = {};
          st.openTrades[dl.pos] = { t: dl.t, price: dl.price, dir: dl.dir };
          logAppend('trades.jsonl', Object.assign(base, { kind: 'open', ctx: {
            conf, grade: conf >= 80 ? 'A' : conf >= 65 ? 'B' : conf >= 50 ? 'C' : 'D',
            biasBuy: !!(d.h4 && d.h4.biasBuy), session: sessOf(Math.floor(now / 1000)),
            stackOk: !!(d.checks && d.checks.stackOk), sarOk: !!(d.checks && d.checks.sarOk),
            inZone: !!(d.checks && d.checks.inZone100), bounce: !!(d.checks && d.checks.bounceConfirm)
          } }));
          broadcast({ type: 'trade', sym, at: now,
            txt: 'เปิดไม้ ' + dl.dir.toUpperCase() + ' ' + dl.lot + ' ' + (dl.symd || sym) + ' @ ' + dl.price + ' (grade ' + (conf >= 80 ? 'A' : conf >= 65 ? 'B' : conf >= 50 ? 'C' : 'D') + ')', pl: 0 });
        } else {
          let src = st.openTrades && st.openTrades[dl.pos];
          if (!src) {
            const o = readTail('trades.jsonl', 400).find(e => e.kind === 'open' && e.pos === dl.pos);
            if (o) src = { t: o.t, price: o.price, dir: o.dir };
          }
          let mfe = null, mae = null;
          if (src && Array.isArray(d.bars) && d.bars.length) {
            const nb = d.bars.map(b => b.length >= 5 ? { t: b[0], h: b[2], l: b[3] } : { t: b[0], h: b[1], l: b[2] });
            const endT = dl.t || Math.floor(now / 1000);
            const seg = nb.filter(b => b.t >= (src.t || 0) - 900 && b.t <= endT + 900);
            if (seg.length) {
              let hi = -1e18, lo = 1e18;
              seg.forEach(b => { hi = Math.max(hi, b.h); lo = Math.min(lo, b.l); });
              if (src.dir === 'buy') { mfe = +(hi - src.price).toFixed(5); mae = +(src.price - lo).toFixed(5); }
              else { mfe = +(src.price - lo).toFixed(5); mae = +(hi - src.price).toFixed(5); }
            }
          }
          if (st.openTrades) delete st.openTrades[dl.pos];
          logAppend('trades.jsonl', Object.assign(base, { kind: 'close', pl: dl.pl, mfe, mae }));
          broadcast({ type: 'trade', sym, at: now,
            txt: 'ปิดไม้ ' + (dl.symd || sym) + ' ' + (dl.pl >= 0 ? '+' : '') + dl.pl + ' USD', pl: dl.pl });
        }
      }
    }

    // hourly market-structure snapshot
    const hour = Math.floor(now / 3600000);
    if (hour !== st.lastStructHour) {
      st.lastStructHour = hour;
      logAppend('structure.jsonl', { at: now, sym, bid: d.bid, stack: stackStr(d),
        biasBuy: !!(d.h4 && d.h4.biasBuy), sarUp: !!(d.sar && d.sar.up),
        sarBars: (d.sar && d.sar.bars) || 0, dist100: (d.checks && d.checks.dist100),
        spread: (typeof d.spread === 'number' ? d.spread : null) });
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
  bars = bars.map(b => b.length >= 5 ? [b[0], b[2], b[3], b[4]] : b);
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
    (d.pos ? ('OPEN POSITIONS: ' + (d.pos.length ? d.pos.map(pp => pp.dir.toUpperCase() + ' ' + pp.lot + ' ' + pp.symp + ' @ ' + pp.entry + ' (floating P/L ' + pp.pl + ')').join(' | ') : 'none')) : 'OPEN POSITIONS: unknown (EA v0.61 needed)'),
    'SESSIONS (ET):\n' + (sessionStats(d.bars) || 'no bar data (EA v0.3+ required)'),
    'LINE TOUCH HISTORY (broker server time, newest first):\n' + (evs || 'none'),
    (d.h1 ? ('H1 CONFIRM: price ' + (d.bid > d.h1.wma50 ? 'above' : 'below') + ' H1 WMA50 (' + f(d.h1.wma50) + '), ' + (d.bid > d.h1.wma100 ? 'above' : 'below') + ' H1 WMA100 (' + f(d.h1.wma100) + ')') : 'H1 CONFIRM: not available (EA v0.5 needed)'),
    (d.acct ? ('RISK DESK: equity ' + d.acct.eq + ', today P/L ' + d.acct.dayPL + ', trades ' + d.acct.trades + '/' + d.acct.limTrades + ', loss streak ' + d.acct.streak + '/' + d.acct.limStreak + ', open positions ' + d.acct.openPos + ((d.acct.trades >= d.acct.limTrades || d.acct.streak >= d.acct.limStreak || d.acct.dayPL <= -(d.acct.bal * d.acct.limLossPct / 100)) ? ' - COOLDOWN ACTIVE, no more trades today per risk rules' : '')) : 'RISK DESK: not available (EA v0.6 needed)'),
    'CONFLUENCE SCORE: ' + confluenceOf(sym, d) + '/100' + (newsLockFor(sym) ? ' (NEWS LOCK active - no trading during news window)' : ''),
    'UPCOMING NEWS (' + Array.from(newsCcy(sym)).join('/') + '): ' + (newsFor(sym, 48).slice(0, 3).map(e => e.ccy + ' ' + e.title + ' [' + e.impact + '] in ' + Math.round((e.t - Date.now()) / 60000) + 'min').join(' | ') || 'none in 48h'),
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

app.post('/api/ai/devil', async (req, res) => {
  if (!pinOk(req)) return res.status(401).json({ ok: false, error: 'bad pin' });
  if (!aiRateOk()) return res.status(429).json({ ok: false, error: 'AI rate limit reached - try again later' });
  const sym = (req.body && req.body.sym) || Object.keys(snapshots)[0];
  const target = (req.body && req.body.target) || 'system';
  const snap = snapshots[sym] && snapshots[sym].data;
  const ctx = snapshotContext(sym);
  if (!ctx || !snap) return res.status(400).json({ ok: false, error: 'no live data for ' + sym });
  let desc;
  if (target === 'buy') desc = 'ไม้สมมติ: BUY ' + sym + ' ที่ราคาตลาดตอนนี้ทันที โดยไม่รอย่อถึงโซน WMA100';
  else if (target === 'sell') desc = 'ไม้สมมติ: SELL ' + sym + ' ที่ราคาตลาดตอนนี้ทันที (ถ้าสวนทิศ bias ให้ชี้ให้ชัดว่าผิดกติการะบบข้อไหน)';
  else if (target === 'open') {
    if (!snap.pos) return res.status(400).json({ ok: false, error: 'ต้องอัพ EA v0.61 เพื่อส่งรายละเอียดไม้ที่เปิดอยู่' });
    if (!snap.pos.length) return res.status(400).json({ ok: false, error: 'ไม่มีไม้เปิดค้างอยู่ตอนนี้' });
    desc = 'ไม้ที่เปิดค้างอยู่จริงตอนนี้ (ดูบรรทัด OPEN POSITIONS ในข้อมูล) — โจมตีความเสี่ยงของการถือต่อ';
  } else desc = 'ไม้มาตรฐานตามระบบ: ' + ((snap.h4 && snap.h4.biasBuy) ? 'BUY' : 'SELL') + ' ที่โซน WMA100 ตามเทคนิค';
  try {
    const text = await askKimi([
      { role: 'system', content: SYSTEM_PROMPT + '\n\nโหมดพิเศษ: ตอนนี้คุณสวมบท DEVIL\'S ADVOCATE ของ Risk Desk — หน้าที่คือพยายามฆ่าไอเดียเทรดที่ระบุ ห้ามอวย ห้ามหาข้อดี ห้ามปลอบ' },
      { role: 'user', content: 'เป้าหมายที่ต้องโจมตี: ' + desc + '\n\nหาเหตุผล 3 ข้อที่ชัดเจนที่สุดว่าทำไมไม้นี้อาจเสีย โดยทุกข้อต้องอ้างตัวเลขจริงจากข้อมูล เช่น ข่าวที่ใกล้เข้ามา ระยะห่างจากโซน สภาพ stack อายุ SAR สถิติที่บันทึกไว้ session และสถานะ Risk Desk แล้วปิดท้ายด้วยประโยคเดียว: ' + (target === 'open' ? 'ควรถือต่อหรือควรจัดการยังไงตามระบบ' : 'ต้องเห็นอะไรก่อนถึงจะสมควรกด') + '\n\n' + ctx }
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

// ---------- Quant Lab ----------
function pctl(arr, p) {
  if (!arr.length) return 0;
  const a = [...arr].sort((x, y) => x - y);
  return a[Math.min(a.length - 1, Math.floor(p * a.length))];
}
function quantSummary(sym, days) {
  const cutoff = Date.now() - days * 86400000;
  const evs = readTail('events.jsonl', 8000).filter(e => e.sym === sym && e.at >= cutoff);
  const sar = readTail('sar.jsonl', 4000).filter(e => e.sym === sym && e.at >= cutoff);
  const bias = readTail('bias.jsonl', 1000).filter(e => e.sym === sym && e.at >= cutoff);

  const lines = {};
  for (const e of evs) {
    const L = lines[e.line] || (lines[e.line] = { bounces: [], breaks: [], spreads: [] });
    if (e.type === 'bounce') L.bounces.push(e.pts || 0); else L.breaks.push(e.pts || 0);
    if (e.ctx && typeof e.ctx.spread === 'number') L.spreads.push(e.ctx.spread);
  }
  const lineStats = {};
  for (const nm of Object.keys(lines)) {
    const L = lines[nm], nb = L.bounces.length, nx = L.breaks.length, n = nb + nx;
    const avgB = nb ? L.bounces.reduce((a, b) => a + b, 0) / nb : 0;
    const avgX = nx ? L.breaks.reduce((a, b) => a + b, 0) / nx : 0;
    const hold = n ? nb / n : 0;
    lineStats[nm] = {
      n, bounces: nb, breaks: nx, holdRate: Math.round(hold * 100),
      avgBounce: +avgB.toFixed(2), medBounce: +pctl(L.bounces, .5).toFixed(2),
      avgBreakDepth: +avgX.toFixed(2),
      expectancy: +(hold * avgB - (1 - hold) * avgX).toFixed(2),
      avgSpread: (function(){ const sp = L.spreads || []; return sp.length ? +(sp.reduce((a,b)=>a+b,0)/sp.length).toFixed(2) : null; })()
    };
  }

  const w = evs.filter(e => e.line === 'WMA100');
  function split(arr, keyFn) {
    const out = {};
    for (const e of arr) {
      const k = keyFn(e); if (k == null) continue;
      const o = out[k] || (out[k] = { n: 0, b: 0 });
      o.n++; if (e.type === 'bounce') o.b++;
    }
    for (const k of Object.keys(out)) out[k].holdRate = Math.round(100 * out[k].b / out[k].n);
    return out;
  }
  const alignedFn = e => {
    const st = (e.ctx && e.ctx.stack) || '';
    const i1 = st.indexOf('WMA50'), i2 = st.indexOf('WMA89'), i3 = st.indexOf('WMA100'), i4 = st.indexOf('WMA144');
    if (i1 < 0 || i2 < 0 || i3 < 0 || i4 < 0) return null;
    const up = i1 < i2 && i2 < i3 && i3 < i4, dn = i1 > i2 && i2 > i3 && i3 > i4;
    return (up || dn) ? 'aligned' : 'scrambled';
  };
  const w100 = {
    n: w.length,
    bySession: split(w, e => e.ts ? sessOf(e.ts) : null),
    byStack: split(w, alignedFn),
    byBias: split(w, e => e.ctx ? (e.ctx.biasBuy ? 'BUY bias' : 'SELL bias') : null),
    byGrade: split(w, e => (e.ctx && typeof e.ctx.conf === 'number')
      ? (e.ctx.conf >= 80 ? 'A' : e.ctx.conf >= 65 ? 'B' : e.ctx.conf >= 50 ? 'C' : 'D') : null),
    byNews: split(w, e => (e.ctx && e.ctx.newsMin !== undefined)
      ? ((e.ctx.newsMin !== null && Math.abs(e.ctx.newsMin) <= 30) ? 'ใกล้ข่าว ±30น' : 'ห่างข่าว') : null)
  };

  const sarBy = a => ({
    n: a.length,
    winRate: a.length ? Math.round(100 * a.filter(e => e.win).length / a.length) : null,
    avgBars: a.length ? Math.round(a.reduce((x, e) => x + (e.bars || 0), 0) / a.length) : 0,
    avgNet: a.length ? +(a.reduce((x, e) => x + (e.net || 0), 0) / a.length).toFixed(2) : 0
  });
  const sarSess = {};
  for (const e of sar) {
    const k = sessOf(Math.floor(e.start / 1000));
    const o = sarSess[k] || (sarSess[k] = { n: 0, w: 0 });
    o.n++; if (e.win) o.w++;
  }
  for (const k of Object.keys(sarSess)) sarSess[k].winRate = Math.round(100 * sarSess[k].w / sarSess[k].n);

  // real trade attribution (from account deals)
  const trRaw = readTail('trades.jsonl', 2000).filter(e => e.at >= cutoff && e.sym === sym);
  const opens = {}, plByPos = {}, mfeByPos = {}, maeByPos = {};
  for (const e of trRaw) {
    if (e.kind === 'open') opens[e.pos] = e;
    else if (e.kind === 'close') {
      plByPos[e.pos] = (plByPos[e.pos] || 0) + (e.pl || 0);
      if (typeof e.mfe === 'number') mfeByPos[e.pos] = Math.max(mfeByPos[e.pos] || -1e18, e.mfe);
      if (typeof e.mae === 'number') maeByPos[e.pos] = Math.max(maeByPos[e.pos] || -1e18, e.mae);
    }
  }
  const closed = Object.keys(plByPos).map(pos => ({
    pos, pl: +plByPos[pos].toFixed(2),
    grade: opens[pos] && opens[pos].ctx ? opens[pos].ctx.grade : null,
    session: opens[pos] && opens[pos].ctx ? opens[pos].ctx.session : null,
    dir: opens[pos] ? opens[pos].dir : null
  }));
  const wins = closed.filter(t => t.pl > 0), losses = closed.filter(t => t.pl <= 0);
  const gsum = a => a.reduce((x, t) => x + t.pl, 0);
  function tradeSplit(keyFn) {
    const out = {};
    for (const t of closed) {
      const k = keyFn(t); if (k == null) continue;
      const o = out[k] || (out[k] = { n: 0, w: 0, pl: 0 });
      o.n++; if (t.pl > 0) o.w++; o.pl = +(o.pl + t.pl).toFixed(2);
    }
    for (const k of Object.keys(out)) out[k].winRate = Math.round(100 * out[k].w / out[k].n);
    return out;
  }
  const trades = {
    n: closed.length,
    winRate: closed.length ? Math.round(100 * wins.length / closed.length) : null,
    avgWin: wins.length ? +(gsum(wins) / wins.length).toFixed(2) : 0,
    avgLoss: losses.length ? +(gsum(losses) / losses.length).toFixed(2) : 0,
    profitFactor: (losses.length && gsum(losses) !== 0) ? +Math.abs(gsum(wins) / gsum(losses)).toFixed(2) : null,
    netPL: +gsum(closed).toFixed(2),
    avgMFE: (function(){ const v = Object.values(mfeByPos).filter(x => x > -1e17); return v.length ? +(v.reduce((a,b)=>a+b,0)/v.length).toFixed(2) : null; })(),
    avgMAE: (function(){ const v = Object.values(maeByPos).filter(x => x > -1e17); return v.length ? +(v.reduce((a,b)=>a+b,0)/v.length).toFixed(2) : null; })(),
    byGrade: tradeSplit(t => t.grade),
    bySession: tradeSplit(t => t.session)
  };

  const stamps = [...evs, ...sar, ...bias].map(e => e.at);
  const firstAt = stamps.length ? Math.min(...stamps) : Date.now();
  return {
    sym, days,
    recordedDays: +((Date.now() - firstAt) / 86400000).toFixed(1),
    totals: { events: evs.length, sarRegimes: sar.length, biasRegimes: bias.length },
    lineStats, w100, trades,
    sar: { all: sarBy(sar), up: sarBy(sar.filter(e => e.up)), down: sarBy(sar.filter(e => !e.up)), bySession: sarSess },
    bias: {
      n: bias.length,
      winRate: bias.length ? Math.round(100 * bias.filter(e => e.win).length / bias.length) : null,
      avgNet: bias.length ? +(bias.reduce((a, e) => a + (e.net || 0), 0) / bias.length).toFixed(2) : 0
    },
    lowSample: evs.length < 30
  };
}
app.get('/api/quant', (req, res) => {
  const sym = req.query.sym || Object.keys(snapshots)[0] || 'XAUUSD';
  const days = Math.min(180, Math.max(1, Number(req.query.days) || 30));
  res.json(quantSummary(sym, days));
});

app.get('/api/news', (req, res) => {
  const sym = req.query.sym || Object.keys(snapshots)[0] || 'XAUUSD';
  res.json({ at: newsCache.at, events: newsFor(sym, 72) });
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
