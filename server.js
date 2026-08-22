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
        t: Date.parse(e.date), title: e.title, ccy: e.country, impact: e.impact,
        forecast: e.forecast || null, previous: e.previous || null, actual: e.actual || null
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

// ---------- resident engineer (SRE): incident log + watchdog ----------
const SYS_START = Date.now();
let lastAiErr = 0, newsStaleFlag = false;
function logIncident(type, detail, extra) {
  logAppend('health.jsonl', Object.assign({ at: Date.now(), type, detail }, extra || {}));
}
function noteAiError(msg) {
  if (Date.now() - lastAiErr > 600000) {
    lastAiErr = Date.now();
    logIncident('ai_error', 'Kimi: ' + String(msg).slice(0, 140));
  }
}
logIncident('server_restart', 'server started (deploy or restart)');

const feedState = {}; // sym -> { down, since }
setInterval(() => {
  const now = Date.now();
  for (const sym of Object.keys(snapshots)) {
    const age = now - snapshots[sym].at;
    const st = feedState[sym] || (feedState[sym] = { down: false, since: 0 });
    if (!st.down && age > 120000) { st.down = true; st.since = snapshots[sym].at; }
    else if (st.down && age < 30000) {
      st.down = false;
      logIncident('feed_gap', sym + ' feed ขาดช่วง (data มีรู)', {
        sym, start: st.since, durMin: +(((now - st.since) / 60000).toFixed(1))
      });
    }
  }
  if (newsCache.at && now - newsCache.at > 3 * 3600000) {
    if (!newsStaleFlag) { newsStaleFlag = true; logIncident('news_stale', 'ปฏิทินข่าวไม่อัพเดตเกิน 3 ชม.'); }
  } else newsStaleFlag = false;
}, 30000);

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
const dayFmtET = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' });
const timeFmtET = new Intl.DateTimeFormat('en-GB', { timeZone: 'America/New_York', hour: '2-digit', minute: '2-digit', hour12: false });
function etDayKey(ms) { return dayFmtET.format(new Date(ms)); }
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
  const tradeNotes = {};
  for (const e of readTail('journal.jsonl', 1500)) if (e.pos) tradeNotes[e.pos] = { text: e.text, setup: e.setup || null };
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
    (d.m5 ? ('M5 TRIGGER at WMA100 zone: ' + (d.m5.trigBuy ? 'BUY confirm candle on last closed M5' : d.m5.trigSell ? 'SELL confirm candle on last closed M5' : 'none yet')) : 'M5 TRIGGER: not available (EA v0.70 needed)'),
    (d.pos ? ('OPEN POSITIONS: ' + (d.pos.length ? d.pos.map(pp => pp.dir.toUpperCase() + ' ' + pp.lot + ' ' + pp.symp + ' @ ' + pp.entry + ' (floating P/L ' + pp.pl + ')').join(' | ') : 'none')) : 'OPEN POSITIONS: unknown (EA v0.61 needed)'),
    'SESSIONS (ET):\n' + (sessionStats(d.bars) || 'no bar data (EA v0.3+ required)'),
    'LINE TOUCH HISTORY (broker server time, newest first):\n' + (evs || 'none'),
    (function(){
      if (!d.m5 || !d.m5.bars || d.m5.bars.length < 3) return 'M5 TRIGGER: not available (EA v0.70 needed)';
      const lb = d.m5.bars[d.m5.bars.length - 2];
      const bull = lb[4] > lb[1];
      const bb2 = d.h4 && d.h4.biasBuy;
      const inZ = !!(d.checks && d.checks.inZone100);
      if (!inZ) return 'M5 TRIGGER: waiting - price not in M15 WMA100 zone yet';
      return 'M5 TRIGGER: price IN M15 zone, last closed M5 candle is ' + (bull ? 'bullish' : 'bearish') + ' -> ' + ((bb2 ? bull : !bull) ? 'TRIGGER READY (M5 closed back in bias direction)' : 'no trigger yet (M5 still against bias)');
    })(),
    (d.h1 ? ('H1 CONFIRM: price ' + (d.bid > d.h1.wma50 ? 'above' : 'below') + ' H1 WMA50 (' + f(d.h1.wma50) + '), ' + (d.bid > d.h1.wma100 ? 'above' : 'below') + ' H1 WMA100 (' + f(d.h1.wma100) + ')') : 'H1 CONFIRM: not available (EA v0.5 needed)'),
    (d.acct ? ('RISK DESK: equity ' + d.acct.eq + ', today P/L ' + d.acct.dayPL + ', trades ' + d.acct.trades + '/' + d.acct.limTrades + ', loss streak ' + d.acct.streak + '/' + d.acct.limStreak + ', open positions ' + d.acct.openPos + ((d.acct.trades >= d.acct.limTrades || d.acct.streak >= d.acct.limStreak || d.acct.dayPL <= -(d.acct.bal * d.acct.limLossPct / 100)) ? ' - COOLDOWN ACTIVE, no more trades today per risk rules' : '')) : 'RISK DESK: not available (EA v0.6 needed)'),
    'CONFLUENCE SCORE: ' + confluenceOf(sym, d) + '/100' + (newsLockFor(sym) ? ' (NEWS LOCK active - no trading during news window)' : ''),
    'UPCOMING NEWS (' + Array.from(newsCcy(sym)).join('/') + '): ' + (newsFor(sym, 48).filter(e => e.t > Date.now()).slice(0, 3).map(e => e.ccy + ' ' + e.title + ' [' + e.impact + '] in ' + Math.round((e.t - Date.now()) / 60000) + 'min').join(' | ') || 'none in 48h'),
    (function () {
      const up = newsFor(sym, 48).filter(e => e.t > Date.now() && e.impact === 'High')[0];
      if (!up) return 'NEWS REACTION HISTORY: no upcoming high-impact event in 48h';
      const h = newsHistFor(sym, up.title, up.ccy);
      return 'NEWS REACTION HISTORY for next event (' + up.title + '): ' + (h
        ? h.n + ' past releases recorded by this system - avg move ' + (h.avg15 > 0 ? '+' : '') + h.avg15 + ' pts in 15min (avg magnitude ' + h.avgAbs15 + '), up ' + h.upPct + '% of the time' + (h.avg30 != null ? ', avg ' + (h.avg30 > 0 ? '+' : '') + h.avg30 + ' in 30min' : '')
        : 'none recorded yet (history builds from today)');
    })(),
    'RECENT NEWS REACTIONS (measured from price): ' + (newsCache.events.filter(e => newsCcy(sym).has(e.ccy) && e.impact === 'High' && e.t <= Date.now() && e.t > Date.now() - 12 * 3600000).map(e => { const r = newsReaction(sym, e.t); return e.title + (e.actual ? ' actual ' + e.actual + ' vs fc ' + (e.forecast || '?') : '') + (r ? ' -> gold ' + (r.p15 > 0 ? '+' : '') + r.p15 + ' in 15min' + (r.p30 != null ? ', ' + (r.p30 > 0 ? '+' : '') + r.p30 + ' in 30min' : '') : ''); }).join(' | ') || 'none'),
    'RECORDED RESEARCH STATS (last 7 days, logged by this cockpit):\n' + statsText(sym, 7),
    'RECENT REAL TRADES (auto-captured from his MT5 account by this cockpit):\n' + (pairTrades(400).filter(r => !r.sym || r.sym === sym).slice(-8).map(r => {
      const sys = r.ctx ? (r.ctx.stackOk && r.ctx.sarOk && r.ctx.inZone && r.ctx.bounce) : null;
      return (r.openAt ? etDayKey(r.openAt) + ' ' + timeFmtET.format(new Date(r.openAt)) + ' ET ' : '') + String(r.dir || '?').toUpperCase() + ' ' + (r.lot || '') +
        (r.openPrice ? ' @' + r.openPrice : '') + (r.closePrice ? ' -> ' + r.closePrice : ' (still open)') +
        (r.pl != null ? ' pl ' + (r.pl >= 0 ? '+' : '') + r.pl : '') +
        (r.ctx && r.ctx.grade ? ' grade ' + r.ctx.grade : '') +
        (sys === null ? '' : sys ? ' [system entry]' : ' [off-system entry]') +
        (r.mfe != null ? ' mfe +' + r.mfe + ' mae -' + r.mae : '') +
        (r.ctx && r.ctx.session ? ' ' + r.ctx.session : '') +
        (tradeNotes[r.pos] ? ' | his note' + (tradeNotes[r.pos].setup ? ' (' + tradeNotes[r.pos].setup + ')' : '') + ': "' + String(tradeNotes[r.pos].text || '').slice(0, 150) + '"' : '');
    }).join('\n') || 'none recorded yet'),
    'AIO BAND TECHNIQUE #2 LIVE STATE: ' + (function(){ const nw = aioNow(sym); if (!nw) return 'no band data yet';
      return 'band ' + nw.lower + '-' + nw.upper + ', price ' + nw.pos + ' band' +
        (nw.armed ? ', ARMED for ' + nw.armed.toUpperCase() + ' on next touch' : ', not armed (needs 5 clean bars outside)') +
        ', H4 bias ' + (nw.h4 === 1 ? 'BUY' : nw.h4 === -1 ? 'SELL' : '-') +
        ', D1 daily bias ' + (nw.d1 === 1 ? 'BUY' : nw.d1 === -1 ? 'SELL' : nw.d1 === 0 ? 'NEUTRAL (all its signals blocked today)' : 'n/a'); })(),
    'AIO BAND TECHNIQUE #2 (parallel test system from his own backtest: WMA87-100 band touch after >=5 bars clean outside, SL opposite band edge +-0.25 ATR, TP 2R, 40-bar horizon, H4-bias filter; both passed and blocked signals are forward-logged): last 30d ' + (function(){ const s = aioStats(sym, 30); if (!s.all.n) return 'no results yet - collecting'; return s.all.n + ' signals, win ' + s.all.winRate + '%, avgR ' + s.all.avgR + (s.all.pf ? ', PF ' + s.all.pf : '') + ' | passed-filter: ' + (s.passed.n ? s.passed.n + ' avgR ' + s.passed.avgR : 'none') + ' | blocked: ' + (s.blocked.n ? s.blocked.n + ' avgR ' + s.blocked.avgR : 'none'); })(),
    'GRADE RUBRIC (how this cockpit grades each trade at the moment of entry): confluence 0-100 = stack aligned +25, right side of EMA200 +10, SAR right side +15, in WMA100 zone +20 (near zone +10/+5), WMA100 bounce history today up to +15, PDH/PDL confluence +10, NEWS LOCK -25; grade A>=80 B>=65 C>=50 else D. IMPORTANT: grade measures alignment with the WMA+SAR system ONLY, not trade quality - his personal PA setups (PA+Market Structure, PA+Momentum) will normally read C/D and that is expected; never scold an off-system trade for being off-system, judge it by the setup named in his journal instead.',
    'TRADER JOURNAL (the trader\'s own notes; his personal setups like PA+Market Structure or PA+Momentum are separate techniques from this cockpit\'s WMA+SAR system - judge each entry against the setup it names, not against the system checklist):\n' + (readTail('journal.jsonl', 80).filter(e => !e.sym || e.sym === sym).slice(-10).map(e =>
      etDayKey(e.at) + ' ' + timeFmtET.format(new Date(e.at)) + ' ET [' + e.tag + (e.setup ? '/' + e.setup : '') + '] ' + String(e.text || '').slice(0, 200) +
      (e.ctx ? ' {' + [e.ctx.bid ? '@' + e.ctx.bid : '', e.ctx.state, e.ctx.heat, e.ctx.lock ? 'NEWS LOCK' : ''].filter(Boolean).join(', ') + '}' : '')
    ).join('\n') || 'no entries yet')
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

// ---------- AIO band engine (technique #2: WMA87-100 band touch, from his Pine backtest) ----------
// Stateless replay over the M15 bar window each new bar; dedupe by key so restarts are safe.
// Filter = H4 bias from EA (D1 band bias needs an EA field - not computable from 75h of M15).
// Both passed AND blocked signals are logged with outcomes, so the filter itself gets forward-tested.
const AIO_CFG = { fast: 87, slow: 100, minAway: 5, atrLen: 14, bufK: 0.25, tpR: 2.0, horizon: 40 };
const AIO = { lastBar: {}, logged: null };
function aioSeed() {
  AIO.logged = new Set();
  for (const e of readTail('aio.jsonl', 6000)) if (e.key) AIO.logged.add(e.key);
}
function wmaArr(vals, len) {
  const out = new Array(vals.length).fill(null);
  const den = len * (len + 1) / 2;
  for (let i = len - 1; i < vals.length; i++) {
    let s = 0;
    for (let k = 0; k < len; k++) s += vals[i - k] * (len - k);
    out[i] = s / den;
  }
  return out;
}
function atrArr(bars, len) {
  const out = new Array(bars.length).fill(null);
  let rma = null;
  for (let i = 1; i < bars.length; i++) {
    const tr = Math.max(bars[i][2] - bars[i][3], Math.abs(bars[i][2] - bars[i - 1][4]), Math.abs(bars[i][3] - bars[i - 1][4]));
    rma = rma === null ? tr : (rma * (len - 1) + tr) / len;
    out[i] = rma;
  }
  return out;
}
function aioProcess(sym, d) {
  try {
    const bars = d.bars;
    if (!bars || bars.length < AIO_CFG.slow + AIO_CFG.minAway + 10 || bars[bars.length - 1].length < 5) return;
    const lastTs = bars[bars.length - 1][0];
    if (AIO.lastBar[sym] === lastTs) return;
    AIO.lastBar[sym] = lastTs;
    if (!AIO.logged) aioSeed();
    const n = bars.length, closes = bars.map(b => b[4]);
    const wf = wmaArr(closes, AIO_CFG.fast), ws = wmaArr(closes, AIO_CFG.slow), atr = atrArr(bars, AIO_CFG.atrLen);
    const H4 = d.h4 ? (d.h4.biasBuy ? 1 : -1) : 0;
    const D1 = (d.aioD1 && typeof d.aioD1.bias === 'number') ? d.aioD1.bias : null;
    const filt = D1 === null ? 'H4' : 'H4+D1';
    const setups = [];
    let lastEvIdx = -Infinity;
    for (let i = AIO_CFG.slow + AIO_CFG.minAway; i < n; i++) {
      const up = Math.max(wf[i], ws[i]), lo = Math.min(wf[i], ws[i]);
      let above = true, below = true;
      for (let k = i - AIO_CFG.minAway; k < i; k++) {
        const u = Math.max(wf[k], ws[k]), l = Math.min(wf[k], ws[k]);
        if (closes[k] - u <= 0) above = false;
        if (closes[k] - l >= 0) below = false;
      }
      const touch = bars[i][3] <= up && bars[i][2] >= lo;
      if (!(touch && (above || below))) continue;
      if (i - lastEvIdx < AIO_CFG.minAway) continue;
      lastEvIdx = i;
      const dir = above ? 1 : -1;
      let eP = dir === 1 ? up : lo;
      if (dir === 1 && bars[i][1] < eP) eP = bars[i][1];
      if (dir === -1 && bars[i][1] > eP) eP = bars[i][1];
      const slP = dir === 1 ? lo - AIO_CFG.bufK * atr[i] : up + AIO_CFG.bufK * atr[i];
      const risk = Math.abs(eP - slP);
      if (!risk) continue;
      const tpP = eP + dir * AIO_CFG.tpR * risk;
      const rng = Math.max(bars[i][2] - bars[i][3], 1e-9), width = Math.max(up - lo, 1e-9);
      const pen = dir === 1 ? (up - bars[i][3]) / width : (bars[i][2] - lo) / width;
      const cloc = dir === 1 ? (closes[i] - bars[i][3]) / rng : (bars[i][2] - closes[i]) / rng;
      const pat = (pen < 0.5 && cloc >= 0.6) ? 'strong' : (pen > 1.0 && cloc < 0.3) ? 'risk' : 'plain';
      const pass = D1 === null ? (H4 !== 0 && H4 === dir) : (H4 === dir && D1 === dir);
      setups.push({ i, ts: bars[i][0], dir, eP, slP, tpP, risk, pat, pass });
    }
    for (const s of setups) {
      let outcome = null, exitReason = null, endIdx = null;
      for (let j = s.i + 1; j < n && j <= s.i + AIO_CFG.horizon; j++) {
        if (s.dir === 1 && bars[j][3] <= s.slP) { outcome = -1; exitReason = 'sl'; endIdx = j; break; }
        if (s.dir === -1 && bars[j][2] >= s.slP) { outcome = -1; exitReason = 'sl'; endIdx = j; break; }
        if (s.dir === 1 && bars[j][2] >= s.tpP) { outcome = AIO_CFG.tpR; exitReason = 'tp'; endIdx = j; break; }
        if (s.dir === -1 && bars[j][3] <= s.tpP) { outcome = AIO_CFG.tpR; exitReason = 'tp'; endIdx = j; break; }
      }
      if (outcome === null && n - 1 >= s.i + AIO_CFG.horizon) {
        const j = s.i + AIO_CFG.horizon;
        outcome = s.dir * (closes[j] - s.eP) / s.risk; exitReason = 'expire'; endIdx = j;
      }
      const kSet = sym + '|set|' + s.ts;
      if (!AIO.logged.has(kSet)) {
        AIO.logged.add(kSet);
        logAppend('aio.jsonl', { at: Date.now(), key: kSet, kind: 'setup', sym, ts: s.ts, dir: s.dir,
          entry: +s.eP.toFixed(2), sl: +s.slP.toFixed(2), tp: +s.tpP.toFixed(2), risk: +s.risk.toFixed(2),
          pat: s.pat, pass: s.pass, h4: H4, d1: D1, filt, session: sessOf(s.ts) });
        if (s.ts === lastTs) {
          broadcast({ type: 'aio', sym, pass: s.pass, txt: 'WMA BAND #2: แตะแถบ ' + (s.dir === 1 ? 'จากบน (BUY)' : 'จากล่าง (SELL)')
            + ' @' + s.eP.toFixed(2) + ' SL ' + s.slP.toFixed(2) + ' TP ' + s.tpP.toFixed(2)
            + (s.pass ? ' · ผ่านฟิลเตอร์' : ' · โดนฟิลเตอร์บล็อก (เก็บสถิติอย่างเดียว)')
            + (s.pat === 'risk' ? ' · แท่งปิดแย่ ระวัง' : s.pat === 'strong' ? ' · แท่งปิดสวย' : '') });
          if (s.pass) autoCommentEvent(sym, 'Technique #2 (WMA Band) fired a ' + (s.dir === 1 ? 'BUY' : 'SELL')
            + ' touch signal at ' + s.eP.toFixed(2) + ' (SL ' + s.slP.toFixed(2) + ', TP ' + s.tpP.toFixed(2)
            + ', candle pattern: ' + s.pat + ') that PASSED the trend filter - a simulated forward-test trade is now being tracked. This is the parallel test system, separate from the main WMA100+SAR checklist.');
        }
      }
      if (outcome !== null) {
        const kRes = sym + '|res|' + s.ts;
        if (!AIO.logged.has(kRes)) {
          AIO.logged.add(kRes);
          logAppend('aio.jsonl', { at: Date.now(), key: kRes, kind: 'result', sym, ts: s.ts, endTs: bars[endIdx][0],
            dir: s.dir, r: +outcome.toFixed(3), exit: exitReason, pat: s.pat, pass: s.pass, h4: H4, d1: D1, filt,
            session: sessOf(s.ts), bars: endIdx - s.i });
          if (bars[endIdx][0] === lastTs) {
            broadcast({ type: 'aio', sym, pass: s.pass, txt: 'WMA BAND #2: จบไม้ ' + (s.dir === 1 ? 'BUY' : 'SELL')
              + ' ' + (outcome > 0 ? '+' : '') + outcome.toFixed(2) + 'R (' + exitReason + ')'
              + (s.pass ? '' : ' [ไม้ที่ถูกบล็อก - นับสถิติเทียบ]') });
          }
        }
      }
    }
  } catch (e) {}
}
function aioStats(sym, days) {
  const cutoff = Date.now() - days * 86400000;
  const res = readTail('aio.jsonl', 8000).filter(e => e.sym === sym && e.kind === 'result' && e.at >= cutoff);
  function agg(rows) {
    const n = rows.length;
    if (!n) return { n: 0 };
    const sum = rows.reduce((a, e) => a + e.r, 0);
    const wins = rows.filter(e => e.r > 0);
    const loss = rows.filter(e => e.r < 0).reduce((a, e) => a + e.r, 0);
    return { n, winRate: Math.round(wins.length / n * 100), avgR: +(sum / n).toFixed(3), sumR: +sum.toFixed(1),
      pf: loss < 0 ? +(wins.reduce((a, e) => a + e.r, 0) / Math.abs(loss)).toFixed(2) : null };
  }
  const bySess = {};
  for (const e of res) { (bySess[e.session] = bySess[e.session] || []).push(e); }
  const sessOut = {};
  for (const k of Object.keys(bySess)) sessOut[k] = agg(bySess[k]);
  return { all: agg(res), passed: agg(res.filter(e => e.pass)), blocked: agg(res.filter(e => !e.pass)),
    byPat: { strong: agg(res.filter(e => e.pat === 'strong')), plain: agg(res.filter(e => e.pat === 'plain')), risk: agg(res.filter(e => e.pat === 'risk')) },
    bySession: sessOut };
}
function aioNow(sym) {
  const snap = snapshots[sym] && snapshots[sym].data;
  if (!(snap && snap.bars && snap.bars.length > AIO_CFG.slow + AIO_CFG.minAway && snap.bars[snap.bars.length - 1].length >= 5)) return null;
  const bars = snap.bars, n = bars.length, closes = bars.map(b => b[4]);
  const wf = wmaArr(closes, AIO_CFG.fast), ws = wmaArr(closes, AIO_CFG.slow);
  const up = Math.max(wf[n - 1], ws[n - 1]), lo = Math.min(wf[n - 1], ws[n - 1]);
  let above = true, below = true;
  for (let k = n - AIO_CFG.minAway; k < n; k++) {
    const u = Math.max(wf[k], ws[k]), l = Math.min(wf[k], ws[k]);
    if (closes[k] - u <= 0) above = false;
    if (closes[k] - l >= 0) below = false;
  }
  return { upper: +up.toFixed(2), lower: +lo.toFixed(2),
    pos: snap.bid > up ? 'above' : snap.bid < lo ? 'below' : 'inside',
    armed: above ? 'buy' : below ? 'sell' : null,
    h4: snap.h4 ? (snap.h4.biasBuy ? 1 : -1) : 0,
    d1: (snap.aioD1 && typeof snap.aioD1.bias === 'number') ? snap.aioD1.bias : null };
}
// ---------- RESEARCH DESK (book #12: hypo.jsonl) ----------
// Kimi proposes ONE machine-testable hypothesis; the server tests it against real books;
// forward performance (rows AFTER proposal time only) is the only judge - anti curve-fit by design.
const HYPO_FIELDS = {
  events: ['line', 'session', 'biasBuy', 'sarUp', 'gradeMin', 'newsNear'],
  aio: ['session', 'pat', 'pass', 'dir']
};
function hypoEventStats(rows) {
  const b = rows.filter(e => e.type === 'bounce'), x = rows.filter(e => e.type === 'break');
  const n = b.length + x.length;
  if (!n) return { n: 0 };
  const hold = b.length / n;
  const avgB = b.length ? b.reduce((a, e) => a + e.pts, 0) / b.length : 0;
  const avgX = x.length ? x.reduce((a, e) => a + e.pts, 0) / x.length : 0;
  return { n, holdRate: Math.round(hold * 100), avgBounce: +avgB.toFixed(2), avgBreak: +avgX.toFixed(2),
    expectancy: +(hold * avgB - (1 - hold) * avgX).toFixed(2) };
}
function hypoAioStats(rows) {
  const n = rows.length;
  if (!n) return { n: 0 };
  const sum = rows.reduce((a, e) => a + e.r, 0), wins = rows.filter(e => e.r > 0);
  const loss = rows.filter(e => e.r < 0).reduce((a, e) => a + e.r, 0);
  return { n, winRate: Math.round(wins.length / n * 100), avgR: +(sum / n).toFixed(3), sumR: +sum.toFixed(1),
    pf: loss < 0 ? +(wins.reduce((a, e) => a + e.r, 0) / Math.abs(loss)).toFixed(2) : null };
}
function hypoMatch(base, cond, e) {
  if (base === 'events') {
    if (cond.line && e.line !== cond.line) return false;
    if (cond.session && (e.ts ? sessOf(e.ts) : null) !== cond.session) return false;
    if (typeof cond.biasBuy === 'boolean' && !!(e.ctx && e.ctx.biasBuy) !== cond.biasBuy) return false;
    if (typeof cond.sarUp === 'boolean' && !!(e.ctx && e.ctx.sarUp) !== cond.sarUp) return false;
    if (cond.gradeMin) {
      const c = e.ctx ? e.ctx.conf : null, min = cond.gradeMin === 'A' ? 80 : cond.gradeMin === 'B' ? 65 : 50;
      if (c == null || c < min) return false;
    }
    if (typeof cond.newsNear === 'boolean') {
      const nm = e.ctx ? e.ctx.newsMin : null;
      if ((nm != null && Math.abs(nm) <= 30) !== cond.newsNear) return false;
    }
    return true;
  }
  if (cond.session && e.session !== cond.session) return false;
  if (cond.pat && e.pat !== cond.pat) return false;
  if (typeof cond.pass === 'boolean' && !!e.pass !== cond.pass) return false;
  if (cond.dir && e.dir !== cond.dir) return false;
  return true;
}
function hypoTest(sym, base, cond, sinceMs) {
  let rows;
  if (base === 'aio') rows = readTail('aio.jsonl', 8000).filter(e => e.sym === sym && e.kind === 'result');
  else rows = readTail('events.jsonl', 8000).filter(e => e.sym === sym && (e.type === 'bounce' || e.type === 'break'));
  if (sinceMs) rows = rows.filter(e => e.at > sinceMs);
  const agg = base === 'aio' ? hypoAioStats : hypoEventStats;
  return { match: agg(rows.filter(e => hypoMatch(base, cond, e))), rest: agg(rows.filter(e => !hypoMatch(base, cond, e))) };
}
app.post('/api/research/propose', async (req, res) => {
  if (!pinOk(req)) return res.status(401).json({ ok: false, error: 'bad pin' });
  if (!KIMI_KEY) return res.status(400).json({ ok: false, error: 'AI not configured' });
  if (!aiRateOk()) return res.status(429).json({ ok: false, error: 'AI rate limit reached' });
  const sym = (req.body && req.body.sym) || 'XAUUSD';
  const prior = readTail('hypo.jsonl', 100).filter(h => h.sym === sym).slice(-8).map(h => h.title).join(' | ') || 'none';
  const digest = 'RECORDED STATS (30d):\n' + statsText(sym, 30) + '\nBAND TECHNIQUE #2 STATS: ' + JSON.stringify(aioStats(sym, 30));
  const prompt = 'You are the research analyst of this trading desk. Study the recorded statistics below and propose EXACTLY ONE testable hypothesis that might reveal a better sub-edge.\n'
    + 'Respond with STRICT JSON ONLY, no prose, no markdown fences:\n'
    + '{"title":"<short Thai title>","base":"events"|"aio","cond":{...},"rationale":"<1-2 Thai sentences: what in the data made you suspect this>"}\n'
    + 'cond rules: 1 to 3 keys MAX (more = overfitting, rejected). Allowed keys for base "events" (WMA line touches): line (WMA50|WMA89|WMA100|WMA144|EMA200|WMA800), session (ASIA|LONDON|NY|OFF), biasBuy (true|false), sarUp (true|false), gradeMin (A|B|C), newsNear (true|false = within 30min of high-impact news). Allowed keys for base "aio" (band technique #2 simulated results): session, pat (strong|plain|risk), pass (true|false), dir (1|-1).\n'
    + 'Avoid repeating these already-proposed hypotheses: ' + prior + '\n\n' + digest;
  try {
    let text = await askKimi([
      { role: 'system', content: 'You output strict JSON only. Never add explanations outside the JSON.' },
      { role: 'user', content: prompt }
    ], 1200);
    text = text.replace(/```json|```/g, '').trim();
    const m = text.match(/\{[\s\S]*\}/);
    if (!m) throw new Error('no JSON in AI reply');
    const h = JSON.parse(m[0]);
    if (h.base !== 'events' && h.base !== 'aio') throw new Error('bad base');
    const keys = Object.keys(h.cond || {});
    if (keys.length < 1 || keys.length > 3) throw new Error('cond must have 1-3 keys');
    for (const k of keys) if (!HYPO_FIELDS[h.base].includes(k)) throw new Error('field not allowed: ' + k);
    const test = hypoTest(sym, h.base, h.cond, null);
    const rec = { at: Date.now(), sym, title: String(h.title || '').slice(0, 120), base: h.base, cond: h.cond,
      rationale: String(h.rationale || '').slice(0, 400), insample: test };
    logAppend('hypo.jsonl', rec);
    res.json({ ok: true, hypo: rec });
  } catch (e) { noteAiError(e.message); res.status(502).json({ ok: false, error: e.message }); }
});
app.get('/api/research', (req, res) => {
  if (!pinOk(req)) return res.status(401).json({ ok: false, error: 'bad pin' });
  const sym = req.query.sym || 'XAUUSD';
  const list = readTail('hypo.jsonl', 200).filter(h => h.sym === sym).slice(-20).reverse();
  for (const h of list) h.forward = hypoTest(sym, h.base, h.cond, h.at);
  res.json({ ok: true, hypos: list });
});

app.get('/api/aio', (req, res) => {
  const sym = req.query.sym || 'XAUUSD';
  const days = Math.min(90, Math.max(1, parseInt(req.query.days || '30', 10)));
  const out = aioStats(sym, days);
  const nw = aioNow(sym);
  if (nw) out.now = nw;
  res.json({ ok: true, days, stats: out });
});

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
  aioProcess(d.sym, d);
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
  if (prev.aioD1 && d.aioD1 && typeof prev.aioD1.bias === 'number' && typeof d.aioD1.bias === 'number' && prev.aioD1.bias !== d.aioD1.bias)
    return 'Technique #2 (WMA Band) daily D1 bias changed to ' + (d.aioD1.bias === 1 ? 'BUY - band system looks for buys only today' : d.aioD1.bias === -1 ? 'SELL - band system looks for sells only today' : 'NEUTRAL - band system filter blocks all its signals today');
  if (!p.inZone100 && c.inZone100)
    return 'Price entered the WMA100 zone';
  if (!p.bounceConfirm && c.bounceConfirm && c.inZone100)
    return 'Bounce confirmation candle formed at WMA100 - checklist setup complete';
  return null;
}

async function autoCommentEvent(sym, ev) {
  if (!KIMI_KEY || !ev) return;
  const st = aiState[sym] || (aiState[sym] = { lastAutoAt: 0 });
  if (Date.now() - st.lastAutoAt < 90000) return;
  if (!aiRateOk()) return;
  st.lastAutoAt = Date.now();
  try {
    const text = await askKimi([
      { role: 'system', content: SYSTEM_PROMPT },
      { role: 'user', content: 'เหตุการณ์ที่เพิ่งเกิด: ' + ev + '\n\nข้อมูลปัจจุบัน:\n' + snapshotContext(sym)
        + '\n\nเขียนคำบรรยายเหตุการณ์นี้สำหรับ feed ยาว 1-2 ประโยคเท่านั้น' }
    ], 800);
    broadcast({ type: 'ai', sym, text });
  } catch (e) { noteAiError(e.message); }
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
  } catch (e) { console.error('auto comment failed:', e.message); noteAiError(e.message); }
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
  } catch (e) { noteAiError(e.message); res.status(502).json({ ok: false, error: e.message }); }
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
  } else if (target === 'band') {
    const nw = aioNow(sym);
    if (!nw) return res.status(400).json({ ok: false, error: 'ยังไม่มีข้อมูลแถบเทคนิค #2 (ต้อง EA v0.71)' });
    desc = 'ไม้เทคนิค #2 (WMA Band): ' + (nw.armed ? (nw.armed === 'buy' ? 'BUY' : 'SELL') + ' เมื่อราคาแตะแถบ ' + nw.lower + '-' + nw.upper : 'ยังไม่ armed - โจมตีสภาพแถบตอนนี้ว่าทำไมสัญญาณถัดไปอาจเป็นกับดัก') + ' (ดูบรรทัด AIO BAND ในข้อมูล - ตัดสินตามกติกาเทคนิค #2 ไม่ใช่ checklist WMA100+SAR)';
  } else desc = 'ไม้มาตรฐานตามระบบ: ' + ((snap.h4 && snap.h4.biasBuy) ? 'BUY' : 'SELL') + ' ที่โซน WMA100 ตามเทคนิค';
  try {
    const text = await askKimi([
      { role: 'system', content: SYSTEM_PROMPT + '\n\nโหมดพิเศษ: ตอนนี้คุณสวมบท DEVIL\'S ADVOCATE ของ Risk Desk — หน้าที่คือพยายามฆ่าไอเดียเทรดที่ระบุ ห้ามอวย ห้ามหาข้อดี ห้ามปลอบ' },
      { role: 'user', content: 'เป้าหมายที่ต้องโจมตี: ' + desc + '\n\nหาเหตุผล 3 ข้อที่ชัดเจนที่สุดว่าทำไมไม้นี้อาจเสีย โดยทุกข้อต้องอ้างตัวเลขจริงจากข้อมูล เช่น ข่าวที่ใกล้เข้ามา ระยะห่างจากโซน สภาพ stack อายุ SAR สถิติที่บันทึกไว้ session และสถานะ Risk Desk แล้วปิดท้ายด้วยประโยคเดียว: ' + (target === 'open' ? 'ควรถือต่อหรือควรจัดการยังไงตามระบบ' : 'ต้องเห็นอะไรก่อนถึงจะสมควรกด') + '\n\n' + ctx }
    ], 2000);
    res.json({ ok: true, text });
  } catch (e) { noteAiError(e.message); res.status(502).json({ ok: false, error: e.message }); }
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
  } catch (e) { noteAiError(e.message); res.status(502).json({ ok: false, error: e.message }); }
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

// ---------- Session Playbook: system vs AI directional calls, graded ----------
const NEXT_SESS = { ASIA: 'LONDON', LONDON: 'NY', NY: 'ASIA' };
const etHM = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', hour: 'numeric', minute: 'numeric', hour12: false });
function etNowHM() {
  let h = 0, m = 0;
  for (const p of etHM.formatToParts(new Date())) {
    if (p.type === 'hour') h = parseInt(p.value, 10) % 24;
    if (p.type === 'minute') m = parseInt(p.value, 10);
  }
  return { h, m };
}
function normBars(d) {
  return d.bars.map(b => b.length >= 5 ? { t: b[0], h: b[2], l: b[3], c: b[4] } : { t: b[0], h: b[1], l: b[2], c: b[3] });
}
function segsOf(bars) {
  const segs = [];
  bars.forEach((b, i) => {
    const s2 = sessOf(b.t);
    if (!segs.length || segs[segs.length - 1].s !== s2) segs.push({ s: s2, a: i, b: i });
    else segs[segs.length - 1].b = i;
  });
  return segs;
}

function gradePending(sym, endedSession, open, close) {
  const tail = readTail('predictions.jsonl', 400);
  const graded = new Set(tail.filter(e => e.kind === 'grade').map(e => e.id));
  const pending = tail.filter(e => e.kind === 'pred' && e.sym === sym && e.nextSession === endedSession && !graded.has(e.id));
  if (!pending.length) return;
  const p = pending[pending.length - 1];
  const nextNet = +(close - open).toFixed(5);
  const thr = p.thr || 1;
  const outcome = nextNet > thr ? 'UP' : (nextNet < -thr ? 'DOWN' : 'RANGE');
  logAppend('predictions.jsonl', {
    kind: 'grade', id: p.id, at: Date.now(), sym, session: endedSession, nextNet, outcome,
    sysWin: p.sysCall === outcome, aiWin: p.aiCall ? p.aiCall === outcome : null
  });
  broadcast({ type: 'ai', sym, at: Date.now(), text: 'PLAYBOOK ตรวจคำทาย ' + endedSession + ': ผลจริง ' + outcome + ' — ระบบ' + (p.sysCall === outcome ? 'ถูก ✓' : 'ผิด ✗ (ทาย ' + p.sysCall + ')') + (p.aiCall ? ' · AI' + (p.aiCall === outcome ? 'ถูก ✓' : 'ผิด ✗ (ทาย ' + p.aiCall + ')') : '') });
}

async function runPlaybook(sym, sessName) {
  const rec = snapshots[sym]; if (!rec) return;
  const d = rec.data;
  if (!d.bars || d.bars.length < 8) return;
  const bars = normBars(d);
  const segs = segsOf(bars);
  let idx = -1;
  for (let i = segs.length - 1; i >= 0; i--) if (segs[i].s === sessName) { idx = i; break; }
  if (idx < 0) return;
  const g = segs[idx];
  const seg = bars.slice(g.a, g.b + 1);
  const open = g.a > 0 ? bars[g.a - 1].c : seg[0].c;
  let hi = -1e18, lo = 1e18;
  seg.forEach(b => { hi = Math.max(hi, b.h); lo = Math.min(lo, b.l); });
  const close = seg[seg.length - 1].c;
  const net = close - open, range = (hi - lo) || 1;
  const closePos = (close - lo) / range;
  let pidx = idx - 1;
  while (pidx >= 0 && segs[pidx].s === 'OFF') pidx--;
  let prevHi = null, prevLo = null;
  if (pidx >= 0) {
    prevHi = -1e18; prevLo = 1e18;
    for (let j = segs[pidx].a; j <= segs[pidx].b; j++) { prevHi = Math.max(prevHi, bars[j].h); prevLo = Math.min(prevLo, bars[j].l); }
  }
  const tol = d.zoneTol || 2;
  const pdh = d.pd && d.pd.h, pdl = d.pd && d.pd.l;
  const facts = {
    open: +open.toFixed(5), hi: +hi.toFixed(5), lo: +lo.toFixed(5), close: +close.toFixed(5),
    net: +net.toFixed(5), range: +range.toFixed(5), closePos: +closePos.toFixed(2),
    brokePrevHi: prevHi != null && hi > prevHi,
    brokePrevLo: prevLo != null && lo < prevLo,
    brokePDH: !!(pdh && close > pdh),
    brokePDL: !!(pdl && close < pdl),
    rejPDH: !!(pdh && hi >= pdh - tol && close < pdh),
    rejPDL: !!(pdl && lo <= pdl + tol && close > pdl)
  };
  // transparent mechanical rule
  let sysCall = 'RANGE';
  if ((facts.brokePDH || facts.brokePrevHi) && closePos > 0.6) sysCall = 'UP';
  else if ((facts.brokePDL || facts.brokePrevLo) && closePos < 0.4) sysCall = 'DOWN';
  else if (facts.rejPDH && closePos < 0.5) sysCall = 'DOWN';
  else if (facts.rejPDL && closePos > 0.5) sysCall = 'UP';
  const thr = +(range * 0.25).toFixed(5);
  const nextSession = NEXT_SESS[sessName];
  const id = sym + '-' + seg[seg.length - 1].t;

  gradePending(sym, sessName, open, close);

  let aiCall = null, aiText = null;
  if (KIMI_KEY) {
    try {
      const factsTxt = sessName + ' (' + sym + '): open ' + facts.open + ', high ' + facts.hi + ', low ' + facts.lo + ', close ' + facts.close
        + ', net ' + facts.net + ', close at ' + Math.round(closePos * 100) + '% of session range'
        + (facts.brokePrevHi ? ', broke previous session high' : '') + (facts.brokePrevLo ? ', broke previous session low' : '')
        + (facts.brokePDH ? ', CLOSED ABOVE PDH (new high vs yesterday)' : '') + (facts.brokePDL ? ', CLOSED BELOW PDL' : '')
        + (facts.rejPDH ? ', tested PDH and REJECTED' : '') + (facts.rejPDL ? ', tested PDL and HELD' : '');
      const text = await askKimi([
        { role: 'system', content: SYSTEM_PROMPT },
        { role: 'user', content: 'session เพิ่งปิด วิเคราะห์และทายทิศ session ถัดไป (' + nextSession + ')' + String.fromCharCode(10,10) + 'FACTS: ' + factsTxt + String.fromCharCode(10,10) + 'CONTEXT:' + String.fromCharCode(10) + snapshotContext(sym) + String.fromCharCode(10,10) + 'รูปแบบคำตอบบังคับเคร่งครัด: ตัวอักษรแรกสุดของคำตอบต้องเริ่มด้วย CALL: ตามด้วย UP หรือ DOWN หรือ RANGE คำเดียว (ทายทิศ ' + nextSession + ') เช่น "CALL: UP" แล้วค่อยขึ้นบรรทัดใหม่เขียนบทวิเคราะห์ 3-4 ประโยค: เกิดอะไรขึ้น + แผนสำหรับ ' + nextSession + ' ตามเทคนิค ห้ามเกิน 5 ประโยค ถ้าไม่มีบรรทัด CALL ถือว่าคำตอบใช้ไม่ได้' }
      ], 1200);
      let mm = /CALL[:\s-]*\b(UP|DOWN|RANGE)\b/i.exec(text || '');
      if (!mm) mm = /^\s*\b(UP|DOWN|RANGE)\b/i.exec(text || '');
      if (!mm) {
        const head = (text || '').slice(0, 120);
        if (/ขึ้น|บวก|ไปต่อ.{0,10}ขึ้น/.test(head)) mm = [null, 'UP'];
        else if (/ลง|ลบ|อ่อนตัว/.test(head)) mm = [null, 'DOWN'];
      }
      if (mm) aiCall = mm[1].toUpperCase();
      aiText = (text || '').replace(/^.*CALL[:\s-]*(UP|DOWN|RANGE).*$/im, '').trim().slice(0, 900);
    } catch (e) { noteAiError(e.message); }
  }
  logAppend('predictions.jsonl', {
    kind: 'pred', id, at: Date.now(), sym, session: sessName, nextSession,
    facts, sysCall, aiCall, aiText, thr
  });
  broadcast({ type: 'ai', sym, at: Date.now(), text: 'PLAYBOOK: ' + sessName + ' จบ — ทาย ' + nextSession + ' | ระบบ: ' + sysCall + (aiCall ? ' · AI: ' + aiCall : '') });
}

const pbDone = {};
setInterval(() => {
  const { h, m } = etNowHM();
  const SESS_END = { 3: 'ASIA', 8: 'LONDON', 17: 'NY' };
  const ended = SESS_END[h];
  if (!ended || m < 3 || m > 25) return;
  const dayKey = new Date().toISOString().slice(0, 10) + '-' + h;
  for (const sym of Object.keys(snapshots)) {
    if (pbDone[sym] === dayKey) continue;
    if (Date.now() - snapshots[sym].at > 120000) continue;
    pbDone[sym] = dayKey;
    runPlaybook(sym, ended).catch(e => console.error('playbook failed:', e.message));
  }
}, 60000);

app.get('/api/playbook', (req, res) => {
  const sym = req.query.sym || Object.keys(snapshots)[0] || 'XAUUSD';
  const tail = readTail('predictions.jsonl', 500).filter(e => e.sym === sym);
  const preds = tail.filter(e => e.kind === 'pred');
  const grades = tail.filter(e => e.kind === 'grade');
  const gmap = {}; grades.forEach(gg => gmap[gg.id] = gg);
  const latest = preds.length ? preds[preds.length - 1] : null;
  let lastGraded = null;
  for (let i = preds.length - 1; i >= 0; i--) if (gmap[preds[i].id]) { lastGraded = { pred: preds[i], grade: gmap[preds[i].id] }; break; }
  const sysG = grades.filter(gg => typeof gg.sysWin === 'boolean');
  const aiG = grades.filter(gg => typeof gg.aiWin === 'boolean');
  res.json({
    latest, lastGraded,
    score: {
      n: grades.length,
      sysAcc: sysG.length ? Math.round(100 * sysG.filter(gg => gg.sysWin).length / sysG.length) : null,
      aiAcc: aiG.length ? Math.round(100 * aiG.filter(gg => gg.aiWin).length / aiG.length) : null
    }
  });
});

app.post('/api/playbook/run', async (req, res) => {
  if (!pinOk(req)) return res.status(401).json({ ok: false, error: 'bad pin' });
  const sym = (req.body && req.body.sym) || Object.keys(snapshots)[0];
  const rec = snapshots[sym];
  if (!rec || !rec.data.bars) return res.status(400).json({ ok: false, error: 'no bar data (EA v0.3+)' });
  const bars = normBars(rec.data);
  const segs = segsOf(bars).filter(g => g.s !== 'OFF');
  if (!segs.length) return res.status(400).json({ ok: false, error: 'no session data' });
  let target = segs[segs.length - 1];
  if (target.b === bars.length - 1 && segs.length > 1) target = segs[segs.length - 2]; // skip live session
  await runPlaybook(sym, target.s);
  res.json({ ok: true, session: target.s });
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
    followRate: closed.length ? Math.round(100 * closed.filter(t => {
      const o = opens[t.pos];
      return o && o.ctx && o.ctx.inZone && o.ctx.bounce;
    }).length / closed.length) : null,
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
app.get('/api/system', (req, res) => {
  const hours = Math.min(168, Math.max(1, Number(req.query.hours) || 24));
  const cutoff = Date.now() - hours * 3600000;
  const incidents = readTail('health.jsonl', 600).filter(e => e.at >= cutoff).sort((a, b) => b.at - a.at);
  const feeds = {};
  for (const sym of Object.keys(snapshots))
    feeds[sym] = { ageSec: Math.round((Date.now() - snapshots[sym].at) / 1000) };
  const files = {};
  for (const f of ['events.jsonl', 'sar.jsonl', 'bias.jsonl', 'structure.jsonl', 'trades.jsonl', 'health.jsonl', 'journal.jsonl', 'aio.jsonl', 'hypo.jsonl']) {
    try { files[f] = +(fs.statSync(path.join(DATA_DIR, f)).size / 1024).toFixed(1); }
    catch (e) { files[f] = 0; }
  }
  res.json({
    incidents, feeds, files,
    uptimeMin: Math.round((Date.now() - SYS_START) / 60000),
    memMB: Math.round(process.memoryUsage().rss / 1048576),
    newsAgeMin: newsCache.at ? Math.round((Date.now() - newsCache.at) / 60000) : null,
    newsEvents: newsCache.events.length,
    aiCallsHour: aiCalls.length,
    persisted: !!process.env.DATA_DIR
  });
});

app.get('/api/quant', (req, res) => {
  const sym = req.query.sym || Object.keys(snapshots)[0] || 'XAUUSD';
  const days = Math.min(180, Math.max(1, Number(req.query.days) || 30));
  res.json(quantSummary(sym, days));
});

// ---------- weekly review assistant ----------
function pbScore(sym) {
  const tail = readTail('predictions.jsonl', 500).filter(e => e.sym === sym);
  const grades = tail.filter(e => e.kind === 'grade');
  const sysG = grades.filter(g => typeof g.sysWin === 'boolean');
  const aiG = grades.filter(g => typeof g.aiWin === 'boolean');
  return {
    graded: grades.length,
    sysAcc: sysG.length ? Math.round(100 * sysG.filter(g => g.sysWin).length / sysG.length) : null,
    aiAcc: aiG.length ? Math.round(100 * aiG.filter(g => g.aiWin).length / aiG.length) : null
  };
}
const REVIEW_PROMPT = 'คุณคือนักวิเคราะห์ quant ประจำกองทุนส่วนตัวของ Maxx หน้าที่คือตอบคำถามรีวิวผลงานจากข้อมูล JSON ที่แนบมาเท่านั้น กติกาเคร่งครัด: 1) ทุกข้อสรุปต้องอ้างตัวเลขจริงพร้อม n 2) ถ้า n ต่ำกว่า 30 ในเรื่องไหน ต้องเขียนกำกับว่า "ข้อมูลยังน้อย ยังสรุปแน่นอนไม่ได้" 3) ห้ามแต่งตัวเลขที่ไม่มีในข้อมูลเด็ดขาด 4) expectancy เป็นค่า proxy ยังไม่รวม spread ให้ระบุทุกครั้งที่พูดถึง (ยกเว้นบอกว่าหักแล้ว) 5) ตอบภาษาไทย กระชับ เป็นข้อๆ ไม่เกิน 8 บรรทัด จบด้วยหนึ่งประโยค: สิ่งเดียวที่ควรทำต่อจากข้อมูลนี้';

app.post('/api/ai/review', async (req, res) => {
  if (!pinOk(req)) return res.status(401).json({ ok: false, error: 'bad pin' });
  if (!aiRateOk()) return res.status(429).json({ ok: false, error: 'AI rate limit reached - try again later' });
  const sym = (req.body && req.body.sym) || Object.keys(snapshots)[0] || 'XAUUSD';
  const days = Math.min(90, Math.max(1, Number(req.body && req.body.days) || 7));
  const q = req.body && req.body.q;
  if (!q) return res.status(400).json({ ok: false, error: 'no question' });
  try {
    const quant = quantSummary(sym, days);
    const ctx = 'QUANT DATA (' + days + ' days, ' + sym + '):' + String.fromCharCode(10) + JSON.stringify(quant)
      + String.fromCharCode(10) + 'PLAYBOOK PREDICTION SCORE: ' + JSON.stringify(pbScore(sym));
    const text = await askKimi([
      { role: 'system', content: REVIEW_PROMPT },
      { role: 'user', content: 'คำถามรีวิว: ' + q + String.fromCharCode(10, 10) + ctx }
    ], 2000);
    res.json({ ok: true, text });
  } catch (e) { noteAiError(e.message); res.status(502).json({ ok: false, error: e.message }); }
});

// ---------- FUND CHARTER: pre-registered decision criteria (locked) ----------
const CHARTER = {
  version: 1,
  sym: 'XAUUSD',
  start: '2026-08-12',
  decide: '2026-11-12',
  title: 'FUND CHARTER v1 — กติกาตัดสินชะตา (เขียนล็อคไว้ 12 ส.ค. 2026 ก่อนเห็นผลลัพธ์)',
  gates: [
    { id: 'edge', name: 'GATE 1 — Edge มีจริง', desc: 'expectancy ต่อการชน WMA100 หลังหัก spread เฉลี่ย >= +1.5 จุด ที่ n >= 150' },
    { id: 'filter', name: 'GATE 2 — Filter ใช้ได้', desc: 'session ที่ดีที่สุดมี hold rate >= 60% ที่ n >= 40' },
    { id: 'pilot', name: 'GATE 3 — มือทำตามระบบ', desc: 'ไม้ demo ปิดแล้ว >= 25 ไม้, เข้าตาม checklist ครบ >= 80%, profit factor >= 1.2' },
    { id: 'ops', name: 'GATE 4 — ระบบเชื่อถือได้', desc: 'feed ขาดรวมไม่เกิน 12 ชั่วโมงตลอดช่วงเก็บข้อมูล' }
  ],
  rules: [
    'ผ่านครบ 4 GATE -> เติมเงินจริง: จำนวนที่ยอมเสียได้ 100% เท่านั้น และ risk ไม่เกิน 0.5% ต่อไม้ในเดือนแรก',
    'GATE 1 ผ่าน แต่ข้ออื่นไม่ครบ -> ต่อ demo อีก 6 สัปดาห์ เฉพาะข้อที่ไม่ผ่าน แล้วประเมินใหม่',
    'GATE 1 ไม่ผ่าน (expectancy หลัง spread <= 0 ที่ n >= 150) -> ไม่เติมเงินจริงกับระบบนี้ ห้ามต่อรอง ห้ามหาข้ออ้าง — กลับไปแก้ระบบใหญ่หรือหา edge ใหม่',
    'ห้ามแก้กติกานี้ก่อนวันตัดสิน 12 พ.ย. 2026 — แก้ได้ทางเดียวคือจด amendment พร้อมเหตุผลและวันที่ ซึ่งจะถูกบันทึกถาวร'
  ]
};
app.get('/api/charter', (req, res) => {
  const sym = CHARTER.sym;
  const q = quantSummary(sym, 90);
  const w = q.lineStats && q.lineStats.WMA100;
  const expNet = (w && w.n) ? +(w.expectancy - (w.avgSpread || 0)).toFixed(2) : null;
  let bestSess = null;
  for (const k of Object.keys((q.w100 && q.w100.bySession) || {})) {
    const o = q.w100.bySession[k];
    if (!bestSess || o.holdRate > bestSess.holdRate) bestSess = Object.assign({ name: k }, o);
  }
  const feedGapMin = readTail('health.jsonl', 2000)
    .filter(e => e.type === 'feed_gap' && e.at >= Date.parse(CHARTER.start))
    .reduce((a, e) => a + (e.durMin || 0), 0);
  const t = q.trades || {};
  const daysLeft = Math.max(0, Math.ceil((Date.parse(CHARTER.decide) - Date.now()) / 86400000));
  res.json({
    charter: CHARTER, daysLeft,
    progress: {
      edge: { value: expNet, n: w ? w.n : 0, need: '>= +1.5 (n>=150)', pass: expNet != null && expNet >= 1.5 && w.n >= 150 },
      filter: { value: bestSess ? bestSess.holdRate : null, session: bestSess ? bestSess.name : null, n: bestSess ? bestSess.n : 0, need: '>= 60% (n>=40)', pass: !!(bestSess && bestSess.holdRate >= 60 && bestSess.n >= 40) },
      pilot: { closed: t.n || 0, followRate: t.followRate, pf: t.profitFactor, need: '25 ไม้ / 80% / PF 1.2', pass: !!((t.n || 0) >= 25 && (t.followRate || 0) >= 80 && (t.profitFactor || 0) >= 1.2) },
      ops: { feedGapMin: +feedGapMin.toFixed(1), need: '< 720 นาที', pass: feedGapMin < 720 }
    }
  });
});

// how each event type relates to gold (via USD): 'inverse' = higher-than-forecast is gold-negative
function goldBias(title) {
  const t = (title || '').toLowerCase();
  if (/rate decision|fomc|press conference|monetary policy|minutes|powell|speaks/.test(t)) return 'special';
  if (/unemployment claims|jobless|unemployment rate/.test(t)) return 'direct';
  return 'inverse'; // CPI, PPI, NFP, retail sales, GDP, PMI, sentiment, ADP, wages...
}
function parseNum(v) {
  if (v == null || v === '') return null;
  const m = /-?\d+(?:\.\d+)?/.exec(String(v).replace(/,/g, ''));
  if (!m) return null;
  let n = parseFloat(m[0]);
  const suf = String(v).toUpperCase();
  if (suf.includes('K')) n *= 1e3;
  else if (suf.includes('M')) n *= 1e6;
  else if (suf.includes('B')) n *= 1e9;
  return n;
}
function newsVerdict(e) {
  const bias = goldBias(e.title);
  if (bias === 'special') return null;
  const act = parseNum(e.actual), ref = parseNum(e.forecast) != null ? parseNum(e.forecast) : parseNum(e.previous);
  if (act == null || ref == null) return null;
  if (act === ref) return 'ตามคาด';
  const higher = act > ref;
  const goldPos = (bias === 'inverse') ? !higher : higher;
  return goldPos ? 'บวกทอง' : 'ลบทอง';
}
function newsRule(e) {
  const bias = goldBias(e.title);
  if (bias === 'special') return 'ดูโทนแถลง — hawkish = ลบทอง · dovish = บวกทอง';
  if (bias === 'direct') return 'ออกสูงกว่าคาด = บวกทอง · ต่ำกว่าคาด = ลบทอง';
  return 'ออกสูงกว่าคาด = ลบทอง · ต่ำกว่าคาด = บวกทอง';
}
// measured gold reaction from our own bars: pts at +15m and +30m after release
function newsReaction(sym, t) {
  const rec = snapshots[sym];
  if (!rec || !rec.data.bars) return null;
  const bars = normBars(rec.data);
  const ts = Math.floor(t / 1000);
  let idx = -1;
  for (let i = 0; i < bars.length; i++) if (bars[i].t <= ts && ts < bars[i].t + 900) { idx = i; break; }
  if (idx < 0) return null;
  const pre = idx > 0 ? bars[idx - 1].c : bars[idx].o;
  const p15 = +(bars[idx].c - pre).toFixed(2);
  const p30 = bars[idx + 1] ? +(bars[idx + 1].c - pre).toFixed(2) : null;
  return { p15, p30 };
}
// persistent per-event-type reaction history (book #8: news.jsonl)
function newsHistFor(sym, title, ccy) {
  const rows = readTail('news.jsonl', 1500).filter(e =>
    e.sym === sym && e.title === title && e.ccy === ccy && typeof e.p15 === 'number');
  if (!rows.length) return null;
  const n = rows.length;
  const avg15 = +(rows.reduce((a, e) => a + e.p15, 0) / n).toFixed(2);
  const w30 = rows.filter(e => typeof e.p30 === 'number');
  const avg30 = w30.length ? +(w30.reduce((a, e) => a + e.p30, 0) / w30.length).toFixed(2) : null;
  return {
    n, avg15, avg30,
    upPct: Math.round(100 * rows.filter(e => e.p15 > 0).length / n),
    avgAbs15: +(rows.reduce((a, e) => a + Math.abs(e.p15), 0) / n).toFixed(2)
  };
}
let newsLogSeen = null;
setInterval(() => {
  const now = Date.now();
  if (!newsCache.events.length) return;
  if (!newsLogSeen)
    newsLogSeen = new Set(readTail('news.jsonl', 1500).map(e => e.sym + '|' + e.t + '|' + e.title));
  for (const sym of Object.keys(snapshots)) {
    if (now - snapshots[sym].at > 120000) continue;
    const ccy = newsCcy(sym);
    for (const e of newsCache.events) {
      if (e.impact !== 'High' || !ccy.has(e.ccy)) continue;
      if (now < e.t + 35 * 60000 || now > e.t + 3 * 3600000) continue;
      const key = sym + '|' + e.t + '|' + e.title;
      if (newsLogSeen.has(key)) continue;
      const r = newsReaction(sym, e.t);
      if (!r || r.p30 == null) continue;
      newsLogSeen.add(key);
      logAppend('news.jsonl', {
        at: now, sym, t: e.t, title: e.title, ccy: e.ccy, impact: e.impact,
        forecast: e.forecast, previous: e.previous, actual: e.actual,
        verdict: newsVerdict(e), p15: r.p15, p30: r.p30
      });
      broadcast({ type: 'ai', sym, at: now,
        text: 'NEWS DESK บันทึกผลถาวร: ' + e.ccy + ' ' + e.title + ' -> ทอง ' + (r.p15 > 0 ? '+' : '') + r.p15 + ' จุดใน 15น / ' + (r.p30 > 0 ? '+' : '') + r.p30 + ' ใน 30น' });
    }
  }
}, 5 * 60000);

app.get('/api/news', (req, res) => {
  const sym = req.query.sym || Object.keys(snapshots)[0] || 'XAUUSD';
  const now = Date.now(), ccy = newsCcy(sym);
  const events = newsCache.events
    .filter(e => ccy.has(e.ccy) && e.t > now - 48 * 3600000 && e.t < now + 72 * 3600000)
    .sort((a, b) => a.t - b.t)
    .map(e => {
      const past = e.t <= now;
      return Object.assign({}, e, {
        rule: newsRule(e),
        verdict: newsVerdict(e),
        reaction: (past && e.impact === 'High') ? newsReaction(sym, e.t) : null,
        hist: (e.impact === 'High') ? newsHistFor(sym, e.title, e.ccy) : null
      });
    });
  res.json({ at: newsCache.at, events });
});

app.get('/api/stats', (req, res) => {
  const sym = req.query.sym || Object.keys(snapshots)[0] || '';
  const days = Math.min(90, Math.max(1, Number(req.query.days) || 7));
  res.json(statsSummary(sym, days));
});

// ---------- trader journal (book #10: journal.jsonl + editable setup list) ----------
const JOURNAL_TAGS = ['entry', 'exit', 'lesson', 'emotion', 'note'];
const SETUPS_FILE = 'journal_setups.json';
const DEFAULT_SETUPS = ['WMA+SAR ตามระบบ', 'PA + Market Structure', 'PA + Momentum'];
function loadSetups() {
  try {
    const l = JSON.parse(fs.readFileSync(path.join(DATA_DIR, SETUPS_FILE), 'utf8'));
    return Array.isArray(l) && l.length ? l : DEFAULT_SETUPS.slice();
  } catch (e) { return DEFAULT_SETUPS.slice(); }
}
function saveSetups(list) {
  try { fs.writeFileSync(path.join(DATA_DIR, SETUPS_FILE), JSON.stringify(list.slice(0, 40))); } catch (e) {}
}
app.post('/api/journal', (req, res) => {
  if (!pinOk(req)) return res.status(401).json({ ok: false, error: 'bad pin' });
  const b = req.body || {};
  const text = String(b.text || '').trim().slice(0, 2000);
  if (!text) return res.status(400).json({ ok: false, error: 'empty text' });
  const tag = JOURNAL_TAGS.includes(b.tag) ? b.tag : 'note';
  const setup = String(b.setup || '').trim().slice(0, 60);
  const c = b.ctx && typeof b.ctx === 'object' ? b.ctx : null;
  const entry = {
    at: Date.now(), sym: String(b.sym || '').slice(0, 12) || null, tag, setup: setup || null, text,
    pos: b.pos != null ? String(b.pos).slice(0, 24) : null,
    ctx: c ? {
      bid: Number(c.bid) || null,
      state: String(c.state || '').slice(0, 90) || null,
      heat: String(c.heat || '').slice(0, 60) || null,
      lock: !!c.lock
    } : null
  };
  logAppend('journal.jsonl', entry);
  if (setup) {
    const list = loadSetups();
    if (!list.includes(setup)) { list.push(setup); saveSetups(list); }
  }
  res.json({ ok: true, entry, setups: loadSetups() });
});
function pairTrades(n) {
  const all = readTail('trades.jsonl', n || 2000);
  const rows = [], byPos = {};
  for (const e of all) {
    if (e.kind === 'open') {
      const r = { pos: e.pos, sym: e.sym, dir: e.dir, lot: e.lot,
        openAt: e.at, openT: e.t || null, openPrice: e.price, ctx: e.ctx || null,
        closeAt: null, closePrice: null, pl: null, mfe: null, mae: null };
      rows.push(r); byPos[e.pos] = r;
    } else if (e.kind === 'close') {
      let r = byPos[e.pos];
      if (!r || r.closeAt != null) {
        r = { pos: e.pos, sym: e.sym, dir: e.dir, lot: e.lot,
          openAt: null, openT: null, openPrice: null, ctx: null,
          closeAt: null, closePrice: null, pl: null, mfe: null, mae: null };
        rows.push(r);
      }
      r.closeAt = e.at; r.closePrice = e.price; r.pl = e.pl; r.mfe = e.mfe; r.mae = e.mae;
      byPos[e.pos] = r;
    }
  }
  return rows;
}
app.get('/api/trades', (req, res) => {
  if (!pinOk(req)) return res.status(401).json({ ok: false, error: 'bad pin' });
  const rows = pairTrades(2000);
  const notes = {};
  for (const e of readTail('journal.jsonl', 1500)) if (e.pos) notes[e.pos] = { at: e.at, text: e.text, setup: e.setup || null, tag: e.tag };
  for (const r of rows) if (notes[r.pos]) r.note = notes[r.pos];
  const day = req.query.day;
  const filt = day ? rows.filter(r => etDayKey(r.closeAt || r.openAt || 0) === day) : rows.slice(-80);
  res.json({ ok: true, trades: filt });
});

app.get('/api/journal', (req, res) => {
  if (!pinOk(req)) return res.status(401).json({ ok: false, error: 'bad pin' });
  const all = readTail('journal.jsonl', 1000);
  const day = req.query.day;
  const rows = day ? all.filter(e => etDayKey(e.at) === day) : all.slice(-120);
  res.json({ ok: true, entries: rows, setups: loadSetups() });
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
