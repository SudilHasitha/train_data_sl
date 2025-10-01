#!/usr/bin/env python3
# slr_routes_to_csv_xlsx.py
# cached + resumable + PROGRESS + start/end/... + SHARDING + reuse old 4-shard caches

import os, re, time, random, logging, argparse, requests, pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional
from bs4 import BeautifulSoup
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from requests_cache import CachedSession  # pip install requests-cache

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)

BASE = "https://eservices.railway.gov.lk/schedule/searchTrain.action"
COMMON = {"lang": "en", "selectedLocale": "en", "searchCriteria.startTime": "00:00:01"}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; slr-scraper/1.3)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://eservices.railway.gov.lk/schedule/searchTrain.action?lang=en",
    "Connection": "close",
}

# === Regex helpers ===
TRAIN_NO_RX = re.compile(r"Train No:\s*\u00A0?\s*(\d+)", re.I)
CLASSES_RX  = re.compile(r"Available Classes:\s*([^\n\r]+?)\s+Train ends at", re.I)
ENDS_AT_RX  = re.compile(r"Train ends at\s+([A-Z \-\(\)\/&\.]+?)\s+at\s+(\d{2}:\d{2}:\d{2})", re.I)
ROW_RX = re.compile(
    r"(?P<yst>[A-Z][A-Z \-()'\/&\.]+?)\s+"
    r"(?P<s_arr>\d{2}:\d{2}:\d{2})\s+"
    r"(?P<s_dep>\d{2}:\d{2}:\d{2})\s+"
    r"(?P<dest>[A-Z][A-Z \-()'\/&\.]+?)\s+"
    r"(?P<dest_t>\d{2}:\d{2}:\d{2})\s+"
    r"(?P<endst>[A-Z][A-Z \-()'\/&\.]+?)\s+"
    r"(?P<end_t>\d{2}:\d{2}:\d{2})\s+"
    r"(?P<rest>.+?)$",
    re.I | re.S
)
TYPE_RX = re.compile(r"(EXPRESS TRAIN|LOCAL TRAINS|INTERCITY|MIXED|COMMUTER|SPECIAL)", re.I)

# Timeouts: (connect, read)
CONNECT_TIMEOUT = 10
READ_TIMEOUT    = 300
TIMEOUT_TUPLE   = (CONNECT_TIMEOUT, READ_TIMEOUT)

# These are rebound in main() after parsing args (per-shard)
DATA_DIR: Path = None
PARTIAL_CSV: Path = None
FINAL_CSV: Path = None
PAIR_LOG: Path = None

# ---------- cache + HTTP ----------

def make_session(cache_path: Path) -> requests.Session:
    s = CachedSession(
        cache_name=str(cache_path),                 # one SQLite DB per shard
        expire_after=timedelta(days=14),
        cache_control=True,
        stale_if_error=True,                        # serve stale on errors
        allowable_methods=["GET"],
    )
    s.headers.update(HEADERS)
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=2.0,                         # exponential backoff
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

def _sqlite_exists(cache_base: Path) -> bool:
    """requests-cache uses <base>.sqlite (+ optional -wal/-shm)."""
    p = cache_base.with_suffix(".sqlite")
    return p.exists()

def build_reuse_sessions(prev_shards: int, base_dir: Path) -> List[CachedSession]:
    """Open read-mostly sessions for previous shard caches under data/shard_i_of_prev/."""
    sessions: List[CachedSession] = []
    for i in range(prev_shards):
        shard_tag = f"shard_{i}_of_{prev_shards}"
        # your layout: data/shard_i_of_prev/http_cache.shard_i_of_prev.sqlite
        cache_base = base_dir / shard_tag / f"http_cache.{shard_tag}"
        if not _sqlite_exists(cache_base):
            continue
        s = CachedSession(
            cache_name=str(cache_base),
            expire_after=None,            # treat as non-expiring for reuse lookups
            cache_control=True,
            stale_if_error=True,
            allowable_methods=["GET"],
        )
        sessions.append(s)
    return sessions

def fetch_from_old_caches(reuse_sessions: List[CachedSession], params: dict) -> Optional[requests.Response]:
    """Try prior caches first; if found, return the cached Response (no network)."""
    for rs in reuse_sessions:
        try:
            r = rs.get(BASE, params=params, timeout=(5, 15), only_if_cached=True)  # 504 if miss
            # 'from_cache' is set on cache hits
            if getattr(r, "from_cache", False) and r.ok:
                return r
        except Exception:
            continue
    return None

# ---------- parsing ----------

def parse_ticket_prices_and_distance(soup: BeautifulSoup):
    txt = " ".join(soup.get_text("\n").split())
    prices = {}
    if "Ticket Prices" in txt:
        for cls in ("1st Class", "2nd Class", "3rd Class"):
            m = re.search(rf"{re.escape(cls)}\s+([0-9]+(?:\.[0-9]+)?)", txt, re.I)
            if m: prices[cls] = float(m.group(1))
    m = re.search(r"Total Distance:\s*([0-9]+(?:\.[0-9]+)?)\s*km", txt, re.I)
    dist = float(m.group(1)) if m else None
    return prices, dist

def split_frequency_name(rest: str):
    rest = " ".join((rest or "").split())
    freq_phrases = [
        "Monday to Friday","Saturday and Sunday","Monday to Saturday",
        "Monday to Sunday","Public Holidays","Weekends","Weekdays",
        "Saturday","Sunday","Daily","Mon-Fri"
    ]
    freq = None
    for ph in freq_phrases:
        if rest.lower().startswith(ph.lower()):
            freq = ph; rest = rest[len(ph):].strip(); break
    name = rest.rstrip(" -")
    return freq, name

def extract_row_fields(page_text_up_to_train_no: str):
    window = page_text_up_to_train_no[-1200:]
    line = " ".join(window.split())
    m = ROW_RX.search(line)
    if not m: return {}
    d = m.groupdict()
    type_m = TYPE_RX.search(d["rest"])
    train_type = type_m.group(1) if type_m else None
    rest_wo_type = TYPE_RX.sub("", d["rest"]).strip()
    freq, name = split_frequency_name(rest_wo_type)
    return {
        "your_station": d["yst"].strip(),
        "start_arrival_time": d["s_arr"],
        "start_departure_time": d["s_dep"],
        "dest_station": d["dest"].strip(),
        "dest_time": d["dest_t"],
        "end_station_name_row": d["endst"].strip(),
        "end_arrival_time": d["end_t"],
        "frequency": freq,
        "train_name": name or None,
        "train_type": train_type
    }

def parse_result(html: str, start_id: int, end_id: int, start_station_name: str, end_station_name: str):
    soup = BeautifulSoup(html, "html.parser")
    prices, distance_km = parse_ticket_prices_and_distance(soup)
    text = " ".join(soup.get_text("\n").split())

    trains = []
    for m in TRAIN_NO_RX.finditer(text):
        train_no = m.group(1).strip()
        row_fields = extract_row_fields(text[:m.start()])
        tail = text[m.end(): m.end() + 600]
        classes = None
        mc = CLASSES_RX.search(tail)
        if mc: classes = " ".join(mc.group(1).split()).replace(" ,", ",")
        mend = ENDS_AT_RX.search(tail)
        train_ends_at, train_ends_time = (None, None)
        if mend:
            train_ends_at = " ".join(mend.group(1).split())
            train_ends_time = mend.group(2)

        trains.append({
            "start_station_id": start_id, "end_station_id": end_id,
            "start_station_name": start_station_name, "end_station_name": end_station_name,
            "train_no": train_no, "available_classes": classes,
            "train_ends_at": train_ends_at, "train_ends_time": train_ends_time,
            "your_station": row_fields.get("your_station"),
            "start_arrival_time": row_fields.get("start_arrival_time"),
            "start_departure_time": row_fields.get("start_departure_time"),
            "dest_station": row_fields.get("dest_station"),
            "dest_time": row_fields.get("dest_time"),
            "end_arrival_time": row_fields.get("end_arrival_time"),
            "frequency": row_fields.get("frequency"),
            "train_name": row_fields.get("train_name"),
            "train_type": row_fields.get("train_type"),
            "price_1st_rs": prices.get("1st Class"),
            "price_2nd_rs": prices.get("2nd Class"),
            "price_3rd_rs": prices.get("3rd Class"),
            "total_distance_km": distance_km,
        })
    return trains

# ---------- progress + IO ----------

def pair_recently_done(sid:int, eid:int) -> bool:
    if not PAIR_LOG.exists(): return False
    try: df = pd.read_csv(PAIR_LOG)
    except Exception: return False
    df = df[(df.start_id == sid) & (df.end_id == eid)]
    if df.empty: return False
    last = pd.to_datetime(df.timestamp.max(), errors="coerce")
    return (pd.Timestamp.now(tz=None) - last) < timedelta(days=7)

def log_completed_pair(sid:int, eid:int, n_rows:int, source:str):
    row = pd.DataFrame([{
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "start_id": sid, "end_id": eid, "rows_written": n_rows, "source": source,
    }])
    row.to_csv(PAIR_LOG, mode="a", header=not PAIR_LOG.exists(), index=False)

def append_rows(rows:list):
    if not rows: return 0
    df = pd.DataFrame(rows)
    df.to_csv(PARTIAL_CSV, mode="a", header=not PARTIAL_CSV.exists(), index=False)
    try:
        with open(PARTIAL_CSV, "a") as f:
            f.flush(); os.fsync(f.fileno())
    except Exception:
        pass
    return len(df)

def fetch_route(
    sid, eid, session, start_station_name, end_station_name,
    reuse_sessions: Optional[List[CachedSession]] = None, delay_range=(3.5, 8.0)
):
    # polite jitter
    time.sleep(random.uniform(*delay_range))

    only_if_cached = bool(int(os.getenv("OFFLINE", "0")))
    params = dict(COMMON, **{"searchCriteria.startStationID": sid, "searchCriteria.endStationID": eid})
    t0 = time.time()

    # 1) try prior caches (4-shard run, etc.)
    r = fetch_from_old_caches(reuse_sessions or [], params)
    if r is not None:
        elapsed = time.time() - t0
        r.encoding = r.apparent_encoding or "utf-8"
        rows = parse_result(r.text, sid, eid, start_station_name, end_station_name)
        return rows, {"status": "ok", "code": 200, "from_cache": True, "elapsed": elapsed, "source": "cache-old"}

    # 2) else current shard cache / network
    try:
        r = session.get(BASE, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), only_if_cached=only_if_cached)
        elapsed = time.time() - t0
        from_cache = bool(getattr(r, "from_cache", False))
        if not r.ok and not from_cache:
            return [], {"status": "http", "code": r.status_code, "from_cache": from_cache, "elapsed": elapsed}
        r.encoding = r.apparent_encoding or "utf-8"
        rows = parse_result(r.text, sid, eid, start_station_name, end_station_name)
        return rows, {
            "status": "ok", "code": r.status_code, "from_cache": from_cache, "elapsed": elapsed,
            "source": "cache-new" if from_cache else "net"
        }
    except requests.exceptions.ReadTimeout:
        return [], {"status": "timeout", "from_cache": False, "elapsed": time.time() - t0}
    except requests.exceptions.RequestException as e:
        return [], {"status": "error", "error": str(e), "from_cache": False, "elapsed": time.time() - t0}

# ---------- main ----------

if __name__ == "__main__":
    start_wall = time.time()

    ap = argparse.ArgumentParser(description="Shardable SLR route crawler (with cache reuse)")
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--shard-index", type=int, default=0)
    ap.add_argument("--out-dir", default=None, help="Base output dir; default: data/shard_{i}_of_{N}")
    ap.add_argument("--reuse-shards-from", type=int, default=0,
                    help="Reuse caches from a previous run that used this many shards (e.g., 4).")
    ap.add_argument("--reuse-cache-dir", default="data",
                    help="Where previous shard dirs live (default: data).")
    args = ap.parse_args()
    assert 0 <= args.shard_index < args.shards

    stations = pd.read_csv("stations.csv")
    id2name = dict(zip(stations["station_id"], stations["station_name"]))
    start_ids = stations["station_id"].tolist()
    end_ids   = stations["station_id"].tolist()

    all_pairs = [(int(sid), int(eid)) for sid in start_ids for eid in end_ids if sid != eid]
    pairs = [pair for i, pair in enumerate(all_pairs) if i % args.shards == args.shard_index]

    shard_tag = f"shard_{args.shard_index}_of_{args.shards}"
    base_dir = Path(args.out_dir) if args.out_dir else Path("data") / shard_tag
    base_dir.mkdir(parents=True, exist_ok=True)

    # Bind per-shard paths
    DATA_DIR   = base_dir
    PARTIAL_CSV = DATA_DIR / f"slr_trains_by_route.{shard_tag}.partial.csv"
    FINAL_CSV   = DATA_DIR / f"slr_trains_by_route.{shard_tag}.csv"
    PAIR_LOG    = DATA_DIR / f"completed_pairs.{shard_tag}.csv"
    cache_path  = DATA_DIR / f"http_cache.{shard_tag}"

    # Build old-cache lookup sessions (data/shard_i_of_prev/http_cache.shard_i_of_prev.sqlite)
    reuse_sessions: List[CachedSession] = []
    if args.reuse_shards_from and args.reuse_shards_from > 0:
        reuse_sessions = build_reuse_sessions(args.reuse_shards_from, Path(args.reuse_cache_dir))

    total_pairs = len(pairs)
    print(f"[INIT] {shard_tag} pairs={total_pairs}  out={DATA_DIR}  reuse_from={args.reuse_shards_from or 0} in {args.reuse_cache_dir}")

    stats = {"done":0, "skipped_recent":0, "cache_hits":0, "net_hits":0,
             "timeouts":0, "errors":0, "http":0, "rows_written":0}

    s = make_session(cache_path)
    seen = set()
    last_summary_ts = time.time()

    for idx, (sid, eid) in enumerate(pairs, 1):
        if pair_recently_done(sid, eid):
            stats["done"] += 1; stats["skipped_recent"] += 1
            print(f"[{idx}/{total_pairs}] {sid}->{eid} SKIP (recent)")
            log_completed_pair(sid, eid, 0, "skip-recent")
            if idx % 25 == 0 or (time.time() - last_summary_ts) > 60:
                print(f"[SUMMARY] done={stats['done']} cache={stats['cache_hits']} net={stats['net_hits']} "
                      f"timeout={stats['timeouts']} http={stats['http']} err={stats['errors']} rows={stats['rows_written']}")
                last_summary_ts = time.time()
            continue

        rows, meta = fetch_route(sid, eid, s, id2name.get(sid), id2name.get(eid), reuse_sessions=reuse_sessions)
        source = meta.get("source") or ("cache" if meta.get("from_cache") else "net")

        if meta["status"] == "ok":
            write_rows = []
            for rec in rows:
                key = (rec["train_no"], rec["start_station_id"], rec["end_station_id"])
                if key in seen: 
                    continue
                seen.add(key); write_rows.append(rec)
            n = append_rows(write_rows)
            stats["rows_written"] += n; stats["done"] += 1
            if "cache" in source: stats["cache_hits"] += 1
            else:                 stats["net_hits"] += 1
            print(f"[{idx}/{total_pairs}] {sid}->{eid} OK ({source},{meta['elapsed']:.1f}s) +{n} rows  totalRows={stats['rows_written']}")
            log_completed_pair(sid, eid, n, source)

        elif meta["status"] == "timeout":
            stats["done"] += 1; stats["timeouts"] += 1
            print(f"[{idx}/{total_pairs}] {sid}->{eid} TIMEOUT ({meta['elapsed']:.1f}s)")
            log_completed_pair(sid, eid, 0, "timeout")

        elif meta["status"] == "http":
            stats["done"] += 1; stats["http"] += 1
            print(f"[{idx}/{total_pairs}] {sid}->{eid} HTTP {meta.get('code')} ({source},{meta['elapsed']:.1f}s)")
            log_completed_pair(sid, eid, 0, "http")

        else:
            stats["done"] += 1; stats["errors"] += 1
            print(f"[{idx}/{total_pairs}] {sid}->{eid} ERROR: {meta.get('error','')} ({meta['elapsed']:.1f}s)")
            log_completed_pair(sid, eid, 0, "error")

        if idx % 25 == 0 or (time.time() - last_summary_ts) > 60:
            print(f"[SUMMARY] done={stats['done']} cache={stats['cache_hits']} net={stats['net_hits']} "
                  f"timeout={stats['timeouts']} http={stats['http']} err={stats['errors']} rows={stats['rows_written']}")
            last_summary_ts = time.time()

    # finalize per-shard CSV + XLSX
    if PARTIAL_CSV.exists():
        df = pd.read_csv(PARTIAL_CSV).drop_duplicates(
            subset=["train_no","start_station_id","end_station_id"]
        )
        tmp = FINAL_CSV.with_suffix(".tmp.csv")
        df.to_csv(tmp, index=False); os.replace(tmp, FINAL_CSV)
        try:
            df.to_excel(DATA_DIR / f"slr_trains_by_route.{shard_tag}.xlsx", index=False)
        except Exception:
            pass
        print(f"[FINALIZE] {shard_tag} wrote {len(df)} unique rows → {FINAL_CSV} | elapsed {time.time()-start_wall:.1f}s")

    print(f"[DONE] {shard_tag} pairs={total_pairs} done={stats['done']} skipped_recent={stats['skipped_recent']} "
          f"cache={stats['cache_hits']} net={stats['net_hits']} timeout={stats['timeouts']} http={stats['http']} "
          f"err={stats['errors']} rows={stats['rows_written']} elapsed={time.time()-start_wall:.1f}s")
