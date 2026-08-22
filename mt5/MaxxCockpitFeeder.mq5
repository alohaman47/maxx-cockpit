//+------------------------------------------------------------------+
//| MaxxCockpitFeeder.mq5                                            |
//| Data feeder for MAXX COCKPIT dashboard (Railway).                |
//| This EA NEVER trades. It only reads chart data and sends JSON.   |
//| Attach to an M15 chart of each symbol you want on the dashboard. |
//+------------------------------------------------------------------+
#property copyright "Maxx"
#property version   "0.71"
#property strict

input string InpURL         = "https://your-app.up.railway.app/api/snapshot"; // Server URL (/api/snapshot)
input string InpKey         = "change-me";   // Secret key (must match Railway env MAXX_KEY)
input int    InpIntervalSec = 2;             // Send interval (seconds)
input double InpZoneTol     = 2.0;           // Touch zone tolerance (price units, e.g. 2.0 for XAUUSD)
input int    InpScanBars    = 300;           // Closed M15 bars to scan for touch history
input int    InpMaxEvents   = 10;            // Max history events to send
input int    InpMaxTradesDay  = 6;           // Risk: max trades per day
input int    InpMaxLossStreak = 3;           // Risk: max consecutive losses
input double InpMaxDayLossPct = 3.0;         // Risk: max daily loss (% of balance)
input bool   InpManualOnly    = true;        // Count only manual trades (magic 0) in risk/attribution

// --- indicator handles ---
int hW50, hW89, hW100, hW144, hE200, hW800, hSAR, hH4W50, hH1W50, hH1W100, hM5W50, hM5W100;

datetime g_lastM15  = 0;
ulong    g_lastDeal = 0;
bool     g_dealInit = false;
string   g_events   = "[]";

struct TouchEvent
  {
   datetime          t;
   string            line;
   string            type;   // bounce / break_up / break_down / testing
   double            pts;
   bool              running;
  };

//+------------------------------------------------------------------+
int OnInit()
  {
   hW50   = iMA(_Symbol, PERIOD_M15, 50,  0, MODE_LWMA, PRICE_CLOSE);
   hW89   = iMA(_Symbol, PERIOD_M15, 89,  0, MODE_LWMA, PRICE_CLOSE);
   hW100  = iMA(_Symbol, PERIOD_M15, 100, 0, MODE_LWMA, PRICE_CLOSE);
   hW144  = iMA(_Symbol, PERIOD_M15, 144, 0, MODE_LWMA, PRICE_CLOSE);
   hE200  = iMA(_Symbol, PERIOD_M15, 200, 0, MODE_EMA,  PRICE_CLOSE);
   hW800  = iMA(_Symbol, PERIOD_M15, 800, 0, MODE_LWMA, PRICE_CLOSE);
   hSAR   = iSAR(_Symbol, PERIOD_M15, 0.02, 0.2);
   hH4W50 = iMA(_Symbol, PERIOD_H4, 50, 0, MODE_LWMA, PRICE_CLOSE);
   hH1W50  = iMA(_Symbol, PERIOD_H1, 50,  0, MODE_LWMA, PRICE_CLOSE);
   hH1W100 = iMA(_Symbol, PERIOD_H1, 100, 0, MODE_LWMA, PRICE_CLOSE);
   hM5W50  = iMA(_Symbol, PERIOD_M5, 50,  0, MODE_LWMA, PRICE_CLOSE);
   hM5W100 = iMA(_Symbol, PERIOD_M5, 100, 0, MODE_LWMA, PRICE_CLOSE);

   if(hW50==INVALID_HANDLE || hW89==INVALID_HANDLE || hW100==INVALID_HANDLE ||
      hW144==INVALID_HANDLE || hE200==INVALID_HANDLE || hW800==INVALID_HANDLE ||
      hSAR==INVALID_HANDLE || hH4W50==INVALID_HANDLE ||
      hH1W50==INVALID_HANDLE || hH1W100==INVALID_HANDLE ||
      hM5W50==INVALID_HANDLE || hM5W100==INVALID_HANDLE)
     {
      Print("MaxxCockpitFeeder: failed to create indicator handles");
      return(INIT_FAILED);
     }

   EventSetTimer(MathMax(1, InpIntervalSec));
   Print("MaxxCockpitFeeder started for ", _Symbol, " -> ", InpURL);
   return(INIT_SUCCEEDED);
  }

//+------------------------------------------------------------------+
void OnDeinit(const int reason) { EventKillTimer(); }

//+------------------------------------------------------------------+
double BufVal(const int handle, const int shift)
  {
   double b[];
   if(CopyBuffer(handle, 0, shift, 1, b) != 1) return(0.0);
   return(b[0]);
  }

string Jd(const double v) { return(DoubleToString(v, _Digits)); }
string Jb(const bool v)   { return(v ? "true" : "false"); }

//+------------------------------------------------------------------+
int SarBarsSinceFlip()
  {
   int n = 200;
   double s[]; MqlRates r[];
   ArraySetAsSeries(s, true); ArraySetAsSeries(r, true);
   if(CopyBuffer(hSAR, 0, 0, n, s) != n)               return(0);
   if(CopyRates(_Symbol, PERIOD_M15, 0, n, r) != n)    return(0);
   bool up0 = (s[1] < r[1].close);
   int cnt = 0;
   for(int i = 1; i < n; i++)
     {
      bool up = (s[i] < r[i].close);
      if(up == up0) cnt++; else break;
     }
   return(cnt);
  }

//+------------------------------------------------------------------+
void AddEvent(TouchEvent &evs[], int &cnt, const datetime t, const string line,
              const string type, const double pts, const bool running)
  {
   if(cnt >= ArraySize(evs)) ArrayResize(evs, cnt + 16);
   evs[cnt].t = t; evs[cnt].line = line; evs[cnt].type = type;
   evs[cnt].pts = pts; evs[cnt].running = running;
   cnt++;
  }

//+------------------------------------------------------------------+
// Walk closed bars oldest -> newest for one line and record
// touch / bounce / break events into evs[].
void ScanLine(const MqlRates &r[], const double &ln[], const int n,
              const string name, TouchEvent &evs[], int &cnt)
  {
   int    phase = 0;        // 0 idle, 1 touching zone, 2 measuring bounce
   bool   above = false;    // price side before the touch
   double peak = 0.0, touchLine = 0.0;
   datetime t0 = 0;

   for(int i = n - 2; i >= 1; i--)   // series arrays: larger i = older
     {
      double lv = ln[i];
      if(lv <= 0.0) continue;
      bool touch      = (r[i].low <= lv + InpZoneTol && r[i].high >= lv - InpZoneTol);
      bool closeAbove = (r[i].close > lv);

      if(phase == 0)
        {
         if(touch)
           {
            above = (r[i+1].close > ln[i+1]);
            touchLine = lv; t0 = r[i].time; phase = 1;
            if(closeAbove != above)
              {
               AddEvent(evs, cnt, r[i].time, name,
                        above ? "break_down" : "break_up",
                        MathAbs(r[i].close - lv), false);
               phase = 0;
              }
           }
        }
      else if(phase == 1)
        {
         if(closeAbove != above)
           {
            AddEvent(evs, cnt, r[i].time, name,
                     above ? "break_down" : "break_up",
                     MathAbs(r[i].close - lv), false);
            phase = 0;
            continue;
           }
         if(!touch)
           {
            phase = 2;
            peak = above ? (r[i].high - touchLine) : (touchLine - r[i].low);
           }
        }
      else // phase == 2 : bounce in progress, measure excursion
        {
         if(touch)
           {
            AddEvent(evs, cnt, t0, name, "bounce", peak, false);
            above = (r[i+1].close > ln[i+1]);
            touchLine = lv; t0 = r[i].time; phase = 1;
            if(closeAbove != above)
              {
               AddEvent(evs, cnt, r[i].time, name,
                        above ? "break_down" : "break_up",
                        MathAbs(r[i].close - lv), false);
               phase = 0;
              }
           }
         else if(closeAbove != above)
           {
            AddEvent(evs, cnt, t0, name, "bounce", peak, false);
            AddEvent(evs, cnt, r[i].time, name,
                     above ? "break_down" : "break_up",
                     MathAbs(r[i].close - lv), false);
            phase = 0;
           }
         else
            peak = MathMax(peak, above ? (r[i].high - touchLine)
                                       : (touchLine - r[i].low));
        }
     }
   if(phase == 2)      AddEvent(evs, cnt, t0, name, "bounce",  peak, true);
   else if(phase == 1) AddEvent(evs, cnt, t0, name, "testing", 0.0,  true);
  }

//+------------------------------------------------------------------+
void BuildEvents()
  {
   int n = InpScanBars + 2;
   MqlRates r[]; ArraySetAsSeries(r, true);
   if(CopyRates(_Symbol, PERIOD_M15, 0, n, r) < n) return;

   int    handles[6] = {0,0,0,0,0,0};
   handles[0]=hW50; handles[1]=hW89; handles[2]=hW100;
   handles[3]=hW144; handles[4]=hE200; handles[5]=hW800;
   string names[6] = {"WMA50","WMA89","WMA100","WMA144","EMA200","WMA800"};

   TouchEvent evs[]; ArrayResize(evs, 32);
   int cnt = 0;

   for(int k = 0; k < 6; k++)
     {
      double ln[]; ArraySetAsSeries(ln, true);
      if(CopyBuffer(handles[k], 0, 0, n, ln) < n) continue;
      ScanLine(r, ln, n, names[k], evs, cnt);
     }

   // sort newest first (small n, simple selection sort)
   for(int a = 0; a < cnt - 1; a++)
     {
      int best = a;
      for(int b = a + 1; b < cnt; b++)
         if(evs[b].t > evs[best].t) best = b;
      if(best != a)
        {
         TouchEvent tmp = evs[a]; evs[a] = evs[best]; evs[best] = tmp;
        }
     }

   int keep = MathMin(cnt, InpMaxEvents);
   long tzOff = (long)TimeGMT() - (long)TimeCurrent(); // server -> UTC offset
   string out = "[";
   for(int i = 0; i < keep; i++)
     {
      if(i > 0) out += ",";
      out += "{\"t\":\"" + TimeToString(evs[i].t, TIME_MINUTES) + "\"";
      out += ",\"ts\":" + IntegerToString((long)evs[i].t + tzOff);
      out += ",\"line\":\"" + evs[i].line + "\"";
      out += ",\"type\":\"" + evs[i].type + "\"";
      out += ",\"pts\":" + Jd(evs[i].pts);
      out += ",\"running\":" + Jb(evs[i].running) + "}";
     }
   out += "]";
   g_events = out;
  }

//+------------------------------------------------------------------+
// Last 96 M15 bars as [[ts,h,l,c],...] (ts in UTC epoch) for the
// dashboard session map (~24h of price path).
string BuildBars()
  {
   int n = 200;
   MqlRates r[]; ArraySetAsSeries(r, true);
   int got = CopyRates(_Symbol, PERIOD_M15, 0, n, r);
   if(got < 4) return("[]");
   long off = (long)TimeGMT() - (long)TimeCurrent();
   string out = "[";
   for(int i = got - 1; i >= 0; i--)
     {
      if(i < got - 1) out += ",";
      out += "[" + IntegerToString((long)r[i].time + off);
      out += "," + Jd(r[i].open) + "," + Jd(r[i].high);
      out += "," + Jd(r[i].low) + "," + Jd(r[i].close) + "]";
     }
   out += "]";
   return(out);
  }

//+------------------------------------------------------------------+
// WMA50/WMA100 values for the last 96 M15 bars (oldest -> newest),
// aligned with BuildBars output, for the dashboard candle chart.
string BuildMa()
  {
   int n = 200;
   double a50[], a100[];
   ArraySetAsSeries(a50, true); ArraySetAsSeries(a100, true);
   if(CopyBuffer(hW50, 0, 0, n, a50) < n)   return("null");
   if(CopyBuffer(hW100, 0, 0, n, a100) < n) return("null");
   string s50 = "[", s100 = "[";
   for(int i = n - 1; i >= 0; i--)
     {
      if(i < n - 1) { s50 += ","; s100 += ","; }
      s50 += Jd(a50[i]); s100 += Jd(a100[i]);
     }
   return("{\"w50\":" + s50 + "],\"w100\":" + s100 + "]}");
  }

//+------------------------------------------------------------------+
// Account risk stats for today (server day boundary = NY close)
string BuildAcct()
  {
   double bal = AccountInfoDouble(ACCOUNT_BALANCE);
   double eq  = AccountInfoDouble(ACCOUNT_EQUITY);
   datetime dayStart = iTime(_Symbol, PERIOD_D1, 0);
   double dayPL = 0.0; int trades = 0; int streak = 0;
   if(HistorySelect(dayStart, TimeCurrent() + 3600))
     {
      int total = HistoryDealsTotal();
      for(int i = 0; i < total; i++)
        {
         ulong tk = HistoryDealGetTicket(i);
         if(tk == 0) continue;
         if(InpManualOnly && HistoryDealGetInteger(tk, DEAL_MAGIC) != 0) continue;
         long en = HistoryDealGetInteger(tk, DEAL_ENTRY);
         double pl = HistoryDealGetDouble(tk, DEAL_PROFIT)
                   + HistoryDealGetDouble(tk, DEAL_SWAP)
                   + HistoryDealGetDouble(tk, DEAL_COMMISSION);
         if(en == DEAL_ENTRY_IN) trades++;
         else if(en == DEAL_ENTRY_OUT)
           {
            dayPL += pl;
            if(pl < 0.0) streak++; else streak = 0;
           }
        }
     }
   string out = "{";
   out += "\"bal\":" + DoubleToString(bal, 2);
   out += ",\"eq\":" + DoubleToString(eq, 2);
   out += ",\"dayPL\":" + DoubleToString(dayPL, 2);
   out += ",\"trades\":" + IntegerToString(trades);
   out += ",\"streak\":" + IntegerToString(streak);
   out += ",\"openPos\":" + IntegerToString(PositionsTotal());
   out += ",\"limTrades\":" + IntegerToString(InpMaxTradesDay);
   out += ",\"limStreak\":" + IntegerToString(InpMaxLossStreak);
   out += ",\"limLossPct\":" + DoubleToString(InpMaxDayLossPct, 2);
   out += "}";
   return(out);
  }

//+------------------------------------------------------------------+
// Report account deals newer than last seen (for trade attribution).
// First run only baselines the latest ticket so old history is not resent.
string BuildDeals()
  {
   if(!HistorySelect(TimeCurrent() - 172800, TimeCurrent() + 3600)) return("[]");
   int total = HistoryDealsTotal();
   if(!g_dealInit)
     {
      for(int i = 0; i < total; i++)
        {
         ulong tk = HistoryDealGetTicket(i);
         if(tk > g_lastDeal) g_lastDeal = tk;
        }
      g_dealInit = true;
      return("[]");
     }
   long tzOff = (long)TimeGMT() - (long)TimeCurrent();
   string out = "["; int cnt = 0; ulong maxTk = g_lastDeal;
   for(int i = 0; i < total && cnt < 20; i++)
     {
      ulong tk = HistoryDealGetTicket(i);
      if(tk <= g_lastDeal) continue;
      if(tk > maxTk) maxTk = tk;
      if(InpManualOnly && HistoryDealGetInteger(tk, DEAL_MAGIC) != 0) continue;
      long en = HistoryDealGetInteger(tk, DEAL_ENTRY);
      if(en != DEAL_ENTRY_IN && en != DEAL_ENTRY_OUT) continue;
      long dtype = HistoryDealGetInteger(tk, DEAL_TYPE);
      if(dtype != DEAL_TYPE_BUY && dtype != DEAL_TYPE_SELL) continue;
      double pl = HistoryDealGetDouble(tk, DEAL_PROFIT)
                + HistoryDealGetDouble(tk, DEAL_SWAP)
                + HistoryDealGetDouble(tk, DEAL_COMMISSION);
      if(cnt > 0) out += ",";
      out += "{\"id\":" + IntegerToString((long)tk);
      out += ",\"pos\":" + IntegerToString(HistoryDealGetInteger(tk, DEAL_POSITION_ID));
      out += ",\"e\":\"" + (en == DEAL_ENTRY_IN ? "in" : "out") + "\"";
      out += ",\"dir\":\"" + (dtype == DEAL_TYPE_BUY ? "buy" : "sell") + "\"";
      out += ",\"symd\":\"" + HistoryDealGetString(tk, DEAL_SYMBOL) + "\"";
      out += ",\"price\":" + DoubleToString(HistoryDealGetDouble(tk, DEAL_PRICE), 5);
      out += ",\"lot\":" + DoubleToString(HistoryDealGetDouble(tk, DEAL_VOLUME), 2);
      out += ",\"pl\":" + DoubleToString(pl, 2);
      out += ",\"t\":" + IntegerToString((long)HistoryDealGetInteger(tk, DEAL_TIME) + tzOff);
      out += "}";
      cnt++;
     }
   g_lastDeal = maxTk;
   out += "]";
   return(out);
  }

//+------------------------------------------------------------------+
// M5 trigger layer: 48 bars + M5 WMA50/100 + confirm-candle flags
// at the M15 WMA100 zone (the technique entry zone).
string BuildM5()
  {
   int n = 48;
   MqlRates r[];
   ArraySetAsSeries(r, true);
   if(CopyRates(_Symbol, PERIOD_M5, 0, n, r) < n) return("null");
   double a50[], a100[];
   ArraySetAsSeries(a50, true); ArraySetAsSeries(a100, true);
   bool haveMa = (CopyBuffer(hM5W50, 0, 0, n, a50) >= n && CopyBuffer(hM5W100, 0, 0, n, a100) >= n);
   long off = (long)TimeGMT() - (long)TimeCurrent();

   double w100 = BufVal(hW100, 0);
   double zHi = w100 + InpZoneTol, zLo = w100 - InpZoneTol;
   bool trigBuy = false, trigSell = false;
   if(w100 > 0)
     {
      trigBuy  = (r[1].low  <= zHi) && (r[1].close > r[1].open) && (r[1].close > w100);
      trigSell = (r[1].high >= zLo) && (r[1].close < r[1].open) && (r[1].close < w100);
     }

   string bars = "[";
   for(int i = n - 1; i >= 0; i--)
     {
      if(i < n - 1) bars += ",";
      bars += "[" + IntegerToString((long)r[i].time + off);
      bars += "," + Jd(r[i].open) + "," + Jd(r[i].high);
      bars += "," + Jd(r[i].low) + "," + Jd(r[i].close) + "]";
     }
   bars += "]";

   string ma = "null";
   if(haveMa)
     {
      string s50 = "[", s100 = "[";
      for(int i = n - 1; i >= 0; i--)
        {
         if(i < n - 1) { s50 += ","; s100 += ","; }
         s50 += Jd(a50[i]); s100 += Jd(a100[i]);
        }
      ma = "{\"w50\":" + s50 + "],\"w100\":" + s100 + "]}";
     }

   string out = "{";
   out += "\"trigBuy\":" + (trigBuy ? "true" : "false");
   out += ",\"trigSell\":" + (trigSell ? "true" : "false");
   out += ",\"bars\":" + bars;
   out += ",\"ma\":" + ma;
   out += "}";
   return(out);
  }

//+------------------------------------------------------------------+
// Current open positions (account-wide, up to 10) for AI red-teaming
string BuildPos()
  {
   int total = PositionsTotal();
   string out = "["; int cnt = 0;
   for(int i = 0; i < total && cnt < 10; i++)
     {
      ulong tk = PositionGetTicket(i);
      if(tk == 0) continue;
      if(InpManualOnly && PositionGetInteger(POSITION_MAGIC) != 0) continue;
      long ptype = PositionGetInteger(POSITION_TYPE);
      if(cnt > 0) out += ",";
      out += "{\"dir\":\"" + (ptype == POSITION_TYPE_BUY ? "buy" : "sell") + "\"";
      out += ",\"symp\":\"" + PositionGetString(POSITION_SYMBOL) + "\"";
      out += ",\"lot\":" + DoubleToString(PositionGetDouble(POSITION_VOLUME), 2);
      out += ",\"entry\":" + DoubleToString(PositionGetDouble(POSITION_PRICE_OPEN), 5);
      out += ",\"pl\":" + DoubleToString(PositionGetDouble(POSITION_PROFIT), 2);
      out += "}";
      cnt++;
     }
   out += "]";
   return(out);
  }

//+------------------------------------------------------------------+
void OnTimer()
  {
   // rebuild touch history on each new M15 bar (and on first run)
   datetime cur = iTime(_Symbol, PERIOD_M15, 0);
   if(cur != g_lastM15)
     {
      BuildEvents();
      g_lastM15 = cur;
     }

   double bid  = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   double w50  = BufVal(hW50, 0),  w89  = BufVal(hW89, 0);
   double w100 = BufVal(hW100, 0), w144 = BufVal(hW144, 0);
   double e200 = BufVal(hE200, 0), w800 = BufVal(hW800, 0);
   double sarv = BufVal(hSAR, 0);
   if(bid <= 0.0 || w100 <= 0.0) return;

   bool sarUp   = (sarv < bid);
   int  sarBars = SarBarsSinceFlip();

   // H1 confirm lines
   double h1w50  = BufVal(hH1W50, 0);
   double h1w100 = BufVal(hH1W100, 0);

   // previous day levels (D1 shift 1 - broker day = NY close boundary)
   double pdh = iHigh(_Symbol, PERIOD_D1, 1);
   double pdl = iLow(_Symbol, PERIOD_D1, 1);

   // H4 bias: closed-candle rule only (shift 1)
   double h4wClosed = BufVal(hH4W50, 1);
   double h4wNow    = BufVal(hH4W50, 0);
   double h4close   = iClose(_Symbol, PERIOD_H4, 1);
   bool   biasBuy   = (h4close > h4wClosed);

   // direction-aware checks
   bool stackOk = biasBuy ? (w50 > w89 && w89 > w100 && w100 > w144)
                          : (w50 < w89 && w89 < w100 && w100 < w144);
   bool emaOk   = biasBuy ? (bid > e200) : (bid < e200);
   bool sarOk   = biasBuy ? sarUp : !sarUp;
   double dist100 = bid - w100;
   bool inZone  = (MathAbs(dist100) <= InpZoneTol);

   // bounce confirm on last closed M15 bar
   double w100c = BufVal(hW100, 1);
   double lo1 = iLow(_Symbol, PERIOD_M15, 1);
   double hi1 = iHigh(_Symbol, PERIOD_M15, 1);
   double cl1 = iClose(_Symbol, PERIOD_M15, 1);
   bool bounceConfirm = biasBuy
      ? (lo1 <= w100c + InpZoneTol && cl1 > w100c)
      : (hi1 >= w100c - InpZoneTol && cl1 < w100c);

   string json = "{";
   json += "\"sym\":\"" + _Symbol + "\"";
   json += ",\"time\":\"" + TimeToString(TimeCurrent(), TIME_MINUTES) + "\"";
   json += ",\"ts\":" + IntegerToString((long)TimeGMT());
   json += ",\"digits\":" + IntegerToString(_Digits);
   json += ",\"bid\":" + Jd(bid);
   json += ",\"zoneTol\":" + Jd(InpZoneTol);
   json += ",\"spread\":" + Jd((double)SymbolInfoInteger(_Symbol, SYMBOL_SPREAD) * _Point);
   json += ",\"lines\":{";
   json += "\"WMA50\":" + Jd(w50) + ",\"WMA89\":" + Jd(w89);
   json += ",\"WMA100\":" + Jd(w100) + ",\"WMA144\":" + Jd(w144);
   json += ",\"EMA200\":" + Jd(e200) + ",\"WMA800\":" + Jd(w800) + "}";
   json += ",\"sar\":{\"val\":" + Jd(sarv) + ",\"up\":" + Jb(sarUp);
   json += ",\"bars\":" + IntegerToString(sarBars) + "}";
   json += ",\"pd\":{\"h\":" + Jd(pdh) + ",\"l\":" + Jd(pdl) + "}";
   json += ",\"h1\":{\"wma50\":" + Jd(h1w50) + ",\"wma100\":" + Jd(h1w100) + "}";
   json += ",\"h4\":{\"wma50\":" + Jd(h4wNow) + ",\"biasBuy\":" + Jb(biasBuy);
   json += ",\"dist\":" + Jd(bid - h4wNow) + "}";
   json += ",\"checks\":{\"stackOk\":" + Jb(stackOk) + ",\"emaOk\":" + Jb(emaOk);
   json += ",\"sarOk\":" + Jb(sarOk) + ",\"inZone100\":" + Jb(inZone);
   json += ",\"bounceConfirm\":" + Jb(bounceConfirm);
   json += ",\"dist100\":" + Jd(dist100) + "}";
   json += ",\"bars\":" + BuildBars();
   json += ",\"ma\":" + BuildMa();
   json += ",\"acct\":" + BuildAcct();
   json += ",\"deals\":" + BuildDeals();
   json += ",\"pos\":" + BuildPos();
   json += ",\"m5\":" + BuildM5();
   json += ",\"aioD1\":" + BuildAioD1();
   json += ",\"events\":" + g_events;
   json += "}";

   SendJson(json);
  }

//+------------------------------------------------------------------+
// Technique #2 D1 band bias (closed daily bars, no repaint):
// band = WMA87/WMA100 of D1 closes; slope = WMA100 now vs 20 D1 bars back.
// bias: +1 close above band & slope up / -1 below & slope down / 0 unclear.
string BuildAioD1()
  {
   double c[]; ArraySetAsSeries(c, true);
   int need = 130;
   int got = CopyClose(_Symbol, PERIOD_D1, 1, need, c);
   if(got < 121) return("null");
   double w87 = 0, w100 = 0, w100b = 0, den87 = 87.0*88.0/2.0, den100 = 100.0*101.0/2.0;
   for(int k = 0; k < 87; k++)  w87   += c[k] * (87 - k);
   for(int k = 0; k < 100; k++) w100  += c[k] * (100 - k);
   for(int k = 0; k < 100; k++) w100b += c[k + 20] * (100 - k);
   w87 /= den87; w100 /= den100; w100b /= den100;
   double up = MathMax(w87, w100), lo = MathMin(w87, w100);
   double slope = w100 - w100b;
   int bias = (c[0] > up && slope > 0) ? 1 : (c[0] < lo && slope < 0) ? -1 : 0;
   string s = "{\"bias\":" + IntegerToString(bias);
   s += ",\"upper\":" + Jd(up) + ",\"lower\":" + Jd(lo) + "}";
   return(s);
  }

//+------------------------------------------------------------------+
void SendJson(const string json)
  {
   char post[]; char result[]; string rheaders;
   int len = StringToCharArray(json, post, 0, StringLen(json), CP_UTF8);
   ArrayResize(post, StringLen(json)); // drop null terminator

   string headers = "Content-Type: application/json\r\n";
   headers += "X-MAXX-KEY: " + InpKey + "\r\n";

   ResetLastError();
   int code = WebRequest("POST", InpURL, headers, 5000, post, result, rheaders);
   if(code == -1)
     {
      int err = GetLastError();
      if(err == 4014)
         Print("MaxxCockpitFeeder: URL not allowed. Add ", InpURL,
               " in Tools > Options > Expert Advisors > Allow WebRequest");
      else
         Print("MaxxCockpitFeeder: WebRequest error ", err);
     }
   else if(code != 200)
      Print("MaxxCockpitFeeder: server responded ", code, " ",
            CharArrayToString(result));
  }
//+------------------------------------------------------------------+
