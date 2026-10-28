"""Local results page: the events recent runs delivered, one block per day, newest first.

local_run.py opens it after a run that was not a dry run, unless --no-html is
passed. Every event is a todo until marked registered or dismissed, which
moves it to the collapsed Handled section where the mark can be undone. An
event marked saved moves to a section pinned above the days, soonest first.
The process exits on its own once no page is open.

The ledger is the only store. Marks are written straight into local_events.db
through ReaderLedger, so the last-call sweep sees them exactly as it sees
`local_run.py --mark`.

Run with
    conda run -n ML python results_page.py
"""
from __future__ import annotations

import json
import os
import socket
import sqlite3
import sys
import threading
import time
import webbrowser
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from urllib.request import ProxyHandler, build_opener
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from eventscout.config import load_settings
from eventscout.models import State, display_zone, parse_iso
from eventscout.notifier import describe_when
from eventscout.store import MailedListing, ReaderLedger

DB = ROOT / "local_events.db"
# Older deliveries stay in the ledger, so widening this brings them back.
SHOW_DAYS = 5
# Email Check's task list holds 8765 and Job Scout's results page 8766, so all
# three can be open at once.
HOST, PORT = "127.0.0.1", 8767
URL = f"http://{HOST}:{PORT}/"
# Long enough for a cold browser start on a busy machine.
STARTUP_GRACE = 90
# Must clear a minute: Chrome throttles hidden-tab timers to about once a minute.
CLIENT_TIMEOUT = 150
# How long a closed page still counts as open, so a reload can arrive.
BYE_GRACE = 10
PING_EVERY_MS = 15000
WATCHDOG_TICK = 3
WEEKDAYS = "一二三四五六日"
# The page's buttons, "" being the undo.
DECISIONS = {"saved": State.SAVED, "registered": State.REGISTERED,
             "dismissed": State.DISMISSED, "": None}
_TODO = (State.NEW, State.SEEN)
_HANDLED = (State.REGISTERED, State.DISMISSED)


def exit_reason(now: float, started: float, clients: dict[str, float],
                seen_any: bool) -> str:
    """Why the process should stop, or "" to keep serving. Before the first page
    connects only the startup grace can end it, since no clients yet is not every
    page closed."""
    if not seen_any:
        return "" if now - started <= STARTUP_GRACE else "browser never connected"
    live = [t for t in clients.values() if now - t <= CLIENT_TIMEOUT]
    return "" if live else "page closed"


def _finished(listing: MailedListing, now: datetime) -> bool:
    """Over by its stated end, or by its start when it states no end. An all-day
    start is local midnight, so one with no end lasts that whole day."""
    event = listing.event
    closes = parse_iso(event.end or event.start)
    if closes is not None and event.all_day and not event.end:
        closes += timedelta(days=1)
    return closes is not None and closes < now


def _start_key(listing: MailedListing) -> tuple[bool, datetime]:
    """Soonest first, undated last."""
    start = parse_iso(listing.event.start)
    return (start is None, start or datetime.max.replace(tzinfo=timezone.utc))


def _card(listing: MailedListing, zone: ZoneInfo, now: datetime,
          long_running: timedelta) -> dict:
    event, score = listing.event, listing.score
    return {
        "url": event.url,
        "title": event.title,
        "when": describe_when(event, zone, now, long_running=long_running),
        "where": event.location or ("online" if event.is_virtual else "location unknown"),
        "urgency": str(listing.urgency or ""),
        "score": (f"fit={score.fit} access={score.access_value} cost={score.cost}"
                  if score else ""),
        "reason": score.reason if score else "",
        "source": event.source,
        "state": str(listing.state),
    }


def build_view(listings: list[MailedListing], now: datetime, zone: ZoneInfo, *,
               long_running: timedelta, show_days: int = SHOW_DAYS) -> dict:
    """Saved events, undecided ones grouped by the local day they were
    delivered (newest day first), and recently handled ones.

    Listings sharing a URL are one event to the reader, since a mark applies to
    them all, so only the latest delivered is kept. Finished events are left
    out of every section but Handled, where an undo still has to be reachable.
    Saved events ignore `show_days`, so one stays pinned until it is unmarked.
    """
    today = now.astimezone(zone).date()
    latest: dict[str, MailedListing] = {}
    for listing in listings:
        held = latest.get(listing.event.url)
        if held is None or listing.mailed_at > held.mailed_at:
            latest[listing.event.url] = listing
    recent = [l for l in latest.values()
              if (today - l.mailed_at.astimezone(zone).date()).days < show_days]
    newest_first = sorted(recent, key=lambda l: l.mailed_at, reverse=True)
    saved = sorted((l for l in latest.values()
                    if l.state == State.SAVED and not _finished(l, now)),
                   key=_start_key)
    days: dict[str, dict] = {}
    for listing in newest_first:
        if listing.state not in _TODO or _finished(listing, now):
            continue
        day = listing.mailed_at.astimezone(zone).date()
        days.setdefault(day.isoformat(), {
            "date": day.isoformat(),
            "label": f"{day:%m/%d} 週{WEEKDAYS[day.weekday()]}",
            "listings": []})["listings"].append(listing)
    card = lambda l: _card(l, zone, now, long_running)
    return {
        "saved": [card(l) for l in saved],
        "days": [{"date": d["date"], "label": d["label"],
                  "events": [card(l) for l in sorted(d["listings"], key=_start_key)]}
                 for d in days.values()],
        "handled": [card(l) for l in newest_first if l.state in _HANDLED],
        "show_days": show_days,
    }


def decide(ledger: ReaderLedger, url: str, decision: str) -> int:
    """Apply one button press. Returns how many listings changed."""
    state = DECISIONS[decision]
    return ledger.unmark(url) if state is None else ledger.mark(url, state)


class _Clients:
    """Open pages by id, for the watchdog's exit rule."""

    def __init__(self, clock=time.time):
        self._lock = threading.Lock()
        self._clock = clock
        self._started = clock()
        self._seen: dict[str, float] = {}
        self._seen_any = False

    def touch(self, cid: str) -> None:
        if not cid:
            return
        with self._lock:
            self._seen_any = True
            self._seen[cid] = self._clock()

    def drop(self, cid: str) -> None:
        """Ages only the closing page's id, so a second open tab keeps the server
        alive. Aged to BYE_GRACE before expiry rather than removed, because a
        reload sends this before the new page's first request, and removing it
        let a watchdog tick in that gap end the server."""
        with self._lock:
            if cid in self._seen:
                self._seen[cid] = min(self._seen[cid],
                                      self._clock() - CLIENT_TIMEOUT + BYE_GRACE)

    def exit_reason(self) -> str:
        """Forgets expired pages, then applies exit_reason."""
        now = self._clock()
        with self._lock:
            for cid in [c for c, t in self._seen.items() if now - t > CLIENT_TIMEOUT]:
                del self._seen[cid]
            return exit_reason(now, self._started, self._seen, self._seen_any)


class ResultsServer(ThreadingHTTPServer):
    daemon_threads = True
    # Windows lets SO_REUSEADDR bind a port another socket already listens on,
    # so two launches at once would both start. Off, the second bind fails.
    allow_reuse_address = False

    def __init__(self, address: tuple[str, int], ledger: ReaderLedger, clients: _Clients,
                 zone: ZoneInfo, long_running: timedelta):
        self.ledger = ledger
        self.clients = clients
        self.zone = zone
        self.long_running = long_running
        super().__init__(address, Handler)


class Handler(BaseHTTPRequestHandler):
    server: ResultsServer

    def log_message(self, format, *args):
        # Under pythonw sys.stderr is None, and the base class writes every
        # request line there, which would raise inside each request.
        pass

    def _send(self, status: int, body: bytes, kind: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, payload: dict) -> None:
        self._send(status, json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                   "application/json; charset=utf-8")

    def _route(self) -> tuple[str, str]:
        parts = urlsplit(self.path)
        return parts.path, parse_qs(parts.query).get("cid", [""])[0]

    def _foreign_host(self) -> bool:
        """A DNS-rebinding page reaches 127.0.0.1 under its own hostname, so the
        Host header is what tells it apart from this page."""
        port = self.server.server_address[1]
        return self.headers.get("Host") not in (f"{HOST}:{port}", f"localhost:{port}")

    def do_GET(self):
        if self._foreign_host():
            return self._json(403, {"error": "unexpected host"})
        path, cid = self._route()
        if path == "/":
            return self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
        if path != "/api/events":
            return self._json(404, {"error": "not found"})
        self.server.clients.touch(cid)
        now = datetime.now(timezone.utc)
        try:
            # One day past SHOW_DAYS, so the zone's offset cannot clip the
            # local-day window build_view applies.
            listings = self.server.ledger.page_rows(now - timedelta(days=SHOW_DAYS + 1))
        # ValueError from a row whose state or timestamps no longer parse, which
        # would otherwise drop the connection and read as a closed server.
        except (sqlite3.Error, ValueError) as exc:
            return self._json(503, {"error": f"{type(exc).__name__}: {exc}"})
        self._json(200, build_view(listings, now, self.server.zone,
                                   long_running=self.server.long_running))

    def do_POST(self):
        if self._foreign_host():
            return self._json(403, {"error": "unexpected host"})
        path, cid = self._route()
        if path == "/api/ping":
            self.server.clients.touch(cid)
            return self._json(200, {"ok": True})
        if path == "/api/bye":
            self.server.clients.drop(cid)
            return self._json(200, {"ok": True})
        if path != "/api/decide":
            return self._json(404, {"error": "not found"})
        # A cross-site form can post text/plain without a preflight, and only
        # this type forces one, which this server never answers.
        if not (self.headers.get("Content-Type") or "").startswith("application/json"):
            return self._json(415, {"error": "expects application/json"})
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
            url, decision = str(body["url"]), str(body["state"])
        except (ValueError, KeyError, TypeError):
            return self._json(400, {"error": "bad request"})
        if not url or decision not in DECISIONS:
            return self._json(400, {"error": "bad request"})
        try:
            changed = decide(self.server.ledger, url, decision)
        except sqlite3.Error as exc:
            # Usually a run holding its write lock. The page says unsaved and
            # the click can be retried.
            return self._json(503, {"error": f"{type(exc).__name__}: {exc}"})
        if not changed:
            return self._json(409, {"error": "gone"})
        self._json(200, {"ok": True})


def _watchdog(clients: _Clients) -> None:
    while True:
        time.sleep(WATCHDOG_TICK)
        why = clients.exit_reason()
        if why:
            # os._exit, not sys.exit, which would only end this thread.
            # Every write commits inside its own request, so no mark is cut off.
            print("exiting: " + why)
            os._exit(0)


# What listener() looks for to tell this page from another server on the port.
_TITLE = "<title>Event Scout 活動</title>"

PAGE = """<!doctype html>
<html lang="zh-Hant-TW">
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
__TITLE__
<style>
 :root{color-scheme:dark}
 body{font:15px/1.55 -apple-system,"Segoe UI",system-ui,sans-serif;
      background:#14161a;color:#e8eaed;margin:0;padding:28px 16px;}
 .wrap{max-width:900px;margin:0 auto}
 h1{font-size:19px;margin:0 0 4px;font-weight:600}
 .sub{color:#9aa0a6;font-size:13px;margin-bottom:22px}
 .sec{margin-bottom:26px}
 .hd{font-size:13px;font-weight:600;color:#7fb2ff;padding-bottom:9px;margin-bottom:11px;
     border-bottom:1px solid #262a31}
 .hd.old{color:#aab0b7}
 .n{font-weight:400;color:#6a6f76;font-size:12px}
 .day{border:1px solid #333a45;border-radius:12px;background:#171a1f;margin-bottom:22px;
      overflow:hidden}
 .dayhd{display:flex;align-items:baseline;gap:10px;padding:12px 16px;background:#20252e;
        cursor:pointer;user-select:none}
 .dayhd .date{font-size:16px;font-weight:600;color:#aab0b7}
 .day.newest .dayhd{background:#1c2a3d}
 .day.newest .dayhd .date{color:#9cc3ff}
 .dayhd .arrow{margin-left:auto;color:#8b9096;font-size:12px}
 .daybody{padding:10px 14px 6px;border-top:1px solid #333a45}
 .day.shut .daybody{display:none}
 @media (max-width:640px){.row{flex-wrap:wrap}.acts{width:100%;justify-content:flex-end}}
 .row{display:flex;gap:13px;padding:13px 15px;border:1px solid #2a2e35;
      border-radius:10px;margin-bottom:9px;background:#1b1e23;align-items:flex-start}
 .body{flex:1;min-width:0}
 .ttl{font-weight:600;margin-bottom:3px;overflow-wrap:anywhere}
 .ttl a{color:inherit;text-decoration:none;border-bottom:1px dotted #6a7480}
 .ttl a:hover{color:#9cc3ff;border-bottom-color:#9cc3ff}
 .meta{color:#8b9096;font-size:12px;margin-top:4px;overflow-wrap:anywhere}
 .when{color:#c9cdd3;font-size:13px;margin-top:2px}
 .why{color:#c9cdd3;font-size:13px;margin-top:4px;overflow-wrap:anywhere}
 .pri{font-size:11px;font-weight:600;border-radius:5px;padding:1px 7px;margin-right:8px}
 .pri.P0{background:#4a1f1f;color:#ff9a8a}
 .pri.P1{background:#43351a;color:#ffc46b}
 .pri.P2{background:#262a31;color:#aab0b7}
 .acts{display:flex;gap:6px;flex:none;align-items:center}
 .btn{background:#23272e;border:1px solid #383d45;color:#cfd4da;border-radius:7px;
      padding:5px 11px;font-size:12px;cursor:pointer}
 .btn:hover{background:#2b3038;color:#fff}
 .btn:disabled{opacity:.45;cursor:default}
 .btn.ok{border-color:#2f5a3a;color:#8fe0a6}
 .btn.fol{border-color:#4a3a6a;color:#d4b8ff}
 .st{font-size:11px;min-width:44px;text-align:right}
 .saving{color:#8b9096}.failed{color:#ff7a7a;font-weight:600}
 .row.done{opacity:.6}
 .tag{font-size:11px;font-weight:600;border-radius:5px;padding:1px 7px;margin-right:8px}
 .tag.registered{background:#1d3a26;color:#7ee09a}
 .tag.dismissed{background:#262a31;color:#aab0b7}
 .tag.saved{background:#2f2445;color:#c9a0ff}
 .follow{border:1px solid #4a3a6a;border-radius:12px;background:#1a1722;margin-bottom:22px;
         padding:0 14px 6px}
 .follow .hd{color:#c9a0ff;border-bottom-color:#3a2f52;padding-top:12px}
 .tog{float:right;font-weight:400;color:#7fb2ff;cursor:pointer;font-size:12px}
 .hidden{display:none}
 .empty{color:#9aa0a6;text-align:center;padding:44px 0}
 .none{color:#6a6f76;font-size:13px;padding:2px 0 8px}
 .err{color:#ff7a7a;font-weight:600;margin-bottom:16px}
 .foot{color:#8b9096;font-size:12px;margin-top:20px;text-align:center}
</style>
<div class=wrap>
  <h1>Event Scout 活動</h1>
  <div class=sub id=sub>載入中</div>
  <div id=err class="err hidden"></div>
  <div class=follow>
    <div class=hd>收藏 <span class=n id=cntsaved></span></div>
    <div id=listsaved></div>
  </div>
  <div id=days></div>
  <div class=sec>
    <div class="hd old">已處理 <span class=n id=cnthandled></span>
      <span class=tog id=toghandled>展開</span></div>
    <div id=listhandled class=hidden></div>
  </div>
  <div class=foot id=foot></div>
</div>
<script>
const el = (t,c,txt)=>{const e=document.createElement(t); if(c)e.className=c;
  if(txt!==undefined)e.textContent=txt; return e;};
// One id per page, so closing one of two tabs does not look like closing both.
const CID = (crypto.randomUUID ? crypto.randomUUID() : String(Math.random()));
// Collapsed days by date. load() rebuilds the list, so this outlives the DOM.
const SHUT = new Set();
const TAGS = {registered:'已報名', dismissed:'略過', saved:'收藏'};

function card(ev, acts){
  const row = el('div','row'), body = el('div','body');
  const t = el('div','ttl');
  if(acts.tag) t.append(el('span','tag '+acts.tag, TAGS[acts.tag]));
  else if(ev.urgency) t.append(el('span','pri '+ev.urgency, ev.urgency));
  const a = el('a',null,ev.title); a.href = ev.url; a.target = '_blank'; a.rel = 'noopener';
  t.append(a);
  body.append(t, el('div','when',ev.when), el('div','meta',ev.where));
  const meta = [ev.score, ev.source].filter(Boolean).join(' · ');
  if(meta) body.append(el('div','meta',meta));
  if(ev.reason) body.append(el('div','why','why: '+ev.reason));
  const side = el('div','acts'), st = el('span','st');
  for(const b of acts.buttons) side.append(b(st));
  side.append(st);
  row.append(body, side);
  return row;
}

function button(label, cls, url, state){
  return st => {
    const b = el('button','btn '+cls,label);
    b.onclick = async ()=>{
      b.disabled = true; st.className = 'st saving'; st.textContent = '儲存中';
      try{
        const r = await fetch('/api/decide',{method:'POST',
          headers:{'Content-Type':'application/json'},body:JSON.stringify({url,state})});
        // 409 means another tab or a run changed it first, so show what is there now.
        if(r.status === 409){ await load(); return; }
        if(!r.ok) throw new Error(String(r.status));
        await load();
      }catch(e){
        b.disabled = false; st.className = 'st failed'; st.textContent = '未儲存';
      }
    };
    return b;
  };
}

async function load(){
  let r, d;
  try{ r = await fetch('/api/events?cid='+CID); d = await r.json(); }
  catch(e){
    document.getElementById('sub').textContent = '讀不到資料，伺服器已經關閉。重新啟動 results_page.py';
    return;
  }
  const err = document.getElementById('err');
  // A locked or missing ledger keeps the last list on screen rather than blanking it.
  err.classList.toggle('hidden', r.ok);
  err.textContent = r.ok ? '' : '讀不到 local_events.db，畫面是上一次讀到的內容。'+(d.error||'');
  if(!r.ok) return;
  const saved = document.getElementById('listsaved');
  saved.replaceChildren();
  for(const ev of d.saved)
    saved.append(card(ev,{tag:'saved', buttons:[button('已報名','ok',ev.url,'registered'),
                                                button('略過','',ev.url,'dismissed'),
                                                button('移回待辦','',ev.url,'')]}));
  if(!d.saved.length)
    saved.append(el('div','none','沒有收藏的活動。在待辦按「收藏」就會移到這裡。'));
  document.getElementById('cntsaved').textContent = d.saved.length+' 筆';
  const box = document.getElementById('days');
  box.replaceChildren();
  let open = 0;
  d.days.forEach((day, i)=>{
    open += day.events.length;
    const sec = el('div', 'day'+(i ? '' : ' newest')+(SHUT.has(day.date) ? ' shut' : ''));
    const hd = el('div','dayhd'), body = el('div','daybody');
    const arrow = el('span','arrow', SHUT.has(day.date) ? '展開 ▸' : '收起 ▾');
    hd.append(el('span','date',day.label), el('span','n', day.events.length+' 筆待辦'), arrow);
    hd.onclick = ()=>{
      const shut = sec.classList.toggle('shut');
      shut ? SHUT.add(day.date) : SHUT.delete(day.date);
      arrow.textContent = shut ? '展開 ▸' : '收起 ▾';
    };
    for(const ev of day.events)
      body.append(card(ev,{buttons:[button('收藏','fol',ev.url,'saved'),
                                    button('已報名','ok',ev.url,'registered'),
                                    button('略過','',ev.url,'dismissed')]}));
    sec.append(hd, body);
    box.append(sec);
  });
  if(!d.days.length) box.append(el('div','empty','目前沒有待辦活動'));
  const list = document.getElementById('listhandled');
  list.replaceChildren();
  for(const ev of d.handled){
    const row = card(ev,{tag:ev.state, buttons:[button('取消','',ev.url,'')]});
    row.classList.add('done');
    list.append(row);
  }
  document.getElementById('cnthandled').textContent = d.handled.length+' 筆';
  document.getElementById('sub').textContent =
    d.days.length+' 天，共 '+open+' 筆待辦。按收藏會移到最上方，按已報名或略過會移到已處理，在那裡可以取消。';
  document.getElementById('foot').textContent =
    '待辦與已處理只顯示最近 '+d.show_days+' 天送達的活動，收藏會一直留到取消。已經結束的不顯示。';
}

document.getElementById('toghandled').onclick = e=>{
  const hidden = document.getElementById('listhandled').classList.toggle('hidden');
  e.target.textContent = hidden ? '展開' : '收起';
};
load();
setInterval(load, 60000);
setInterval(()=>fetch('/api/ping?cid='+CID,{method:'POST'}).catch(()=>{}), __PING_MS__);
// pagehide is the unload-side event browsers still allow a beacon from. A discarded
// tab sends none, which the ping timeout covers.
addEventListener('pagehide', ()=>{ navigator.sendBeacon('/api/bye?cid='+CID); });
</script>
""".replace("__TITLE__", _TITLE).replace("__PING_MS__", str(PING_EVERY_MS))


def listener(host: str, port: int) -> str:
    """"" when nothing listens on the port, "ours" when it serves this page,
    "other" for anything else."""
    with socket.socket() as sock:
        sock.settimeout(1)
        if sock.connect_ex((host, port)) != 0:
            return ""
    try:
        # No proxy, or a system proxy that does not exempt 127.0.0.1 answers
        # instead and the page reads as someone else's.
        with build_opener(ProxyHandler({})).open(f"http://{host}:{port}/",
                                                 timeout=3) as response:
            page = response.read().decode("utf-8", "replace")
    except (OSError, ValueError):
        return "other"
    return "ours" if _TITLE in page else "other"


def serve() -> int:
    if not DB.exists():
        print(f"{DB.name} not found, nothing to show")
        return 1
    held = listener(HOST, PORT)
    if held == "ours":
        # A page already serving picks up the new run itself. Opening a tab
        # anyway covers a reader who closed theirs and is inside BYE_GRACE.
        print(f"the results page is already running, opening {URL}")
        webbrowser.open(URL)
        return 0
    if held:
        print(f"port {PORT} is held by something other than the results page")
        return 1
    settings = load_settings()
    try:
        server = ResultsServer((HOST, PORT), ReaderLedger(DB), _Clients(),
                               display_zone(settings.display_timezone),
                               timedelta(days=settings.long_running_days))
    except OSError as exc:
        # Another launch bound the port between listener() and here.
        print(f"port {PORT} could not be bound: {exc}")
        return 1
    print("Event Scout results at " + URL)
    # After the bind, or a failed one would still pop a tab.
    threading.Timer(0.8, lambda: webbrowser.open(URL)).start()
    threading.Thread(target=_watchdog, args=(server.clients,), daemon=True).start()
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(serve())
