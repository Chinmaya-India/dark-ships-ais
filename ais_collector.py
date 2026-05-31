"""
AIS Collector — continuous background service.

Connects to AisStream.io WebSocket and writes every position report into
a local SQLite database (ais_history.db).  The pipeline queries this DB
when a SAR scene arrives instead of making a live API call.

Architecture:
  AisStream WebSocket -> ais_history.db (rolling 30-day window)
                               |
  SAR scene acquired  -> query_ais_snapshot(bbox, acq_time, window_hours=3)
                               |
  GFW 4Wings API      -> backfill_from_gfw(bbox, start, end)   [when reachable]

Usage:
  # Start collector (run once, keep alive):
  python ais_collector.py --aois strait_of_malacca,gulf_of_aden

  # Query snapshot (used internally by run_pipeline.py):
  python ais_collector.py --query --bbox 99,0.5,104.5,6.5 --time 2026-03-22T23:11:00Z
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Optional

import requests
from dotenv import load_dotenv

load_dotenv('.env.local')

log = logging.getLogger("ais_collector")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [AIS_COL] %(message)s")

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "ais_history.db"))
RETENTION_DAYS = 90

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            mmsi      TEXT    NOT NULL,
            ts        REAL    NOT NULL,   -- Unix timestamp (UTC)
            lat       REAL    NOT NULL,
            lon       REAL    NOT NULL,
            speed     REAL,
            course    REAL,
            name      TEXT,
            ship_type INTEGER,
            flag      TEXT,
            source    TEXT DEFAULT 'aisstream'
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ts  ON positions(ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mmsi ON positions(mmsi)")
    conn.commit()
    return conn


def _prune_old(conn: sqlite3.Connection):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).timestamp()
    cur = conn.execute("DELETE FROM positions WHERE ts < ?", (cutoff,))
    if cur.rowcount:
        conn.commit()
        log.info(f"Pruned {cur.rowcount} records older than {RETENTION_DAYS} days")


def _insert_position(conn: sqlite3.Connection, row: dict):
    conn.execute("""
        INSERT INTO positions (mmsi, ts, lat, lon, speed, course, name, ship_type, flag, source)
        VALUES (:mmsi, :ts, :lat, :lon, :speed, :course, :name, :ship_type, :flag, :source)
    """, row)


# ---------------------------------------------------------------------------
# Query: called by run_pipeline.py
# ---------------------------------------------------------------------------

def query_ais_snapshot(
    bbox: List[float],
    acq_time: datetime,
    window_hours: float = 3.0,
) -> List[Dict]:
    """
    Return all vessel positions within bbox in ±window_hours around acq_time.
    bbox = [min_lon, min_lat, max_lon, max_lat]
    """
    if not os.path.exists(DB_PATH):
        log.warning("ais_history.db not found — no local AIS data yet")
        return []

    min_lon, min_lat, max_lon, max_lat = bbox
    t0 = (acq_time - timedelta(hours=window_hours)).timestamp()
    t1 = (acq_time + timedelta(hours=window_hours)).timestamp()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT mmsi, ts, lat, lon, speed, course, name, ship_type, flag, source
        FROM   positions
        WHERE  ts   BETWEEN ? AND ?
          AND  lon  BETWEEN ? AND ?
          AND  lat  BETWEEN ? AND ?
        ORDER  BY ts DESC
    """, (t0, t1, min_lon, max_lon, min_lat, max_lat)).fetchall()
    conn.close()

    # Deduplicate: keep most-recent position per MMSI
    seen = {}
    for r in rows:
        m = r["mmsi"]
        if m not in seen:
            seen[m] = {
                "mmsi":      m,
                "lat":       r["lat"],
                "lon":       r["lon"],
                "latitude":  r["lat"],
                "longitude": r["lon"],
                "timestamp": datetime.fromtimestamp(r["ts"], tz=timezone.utc).isoformat(),
                "speed":     r["speed"] or 0,
                "course":    r["course"] or 0,
                "vessel_name": r["name"] or "Unknown",
                "flag":      r["flag"] or "Unknown",
                "source":    r["source"] or "aisstream",
            }

    vessels = list(seen.values())
    log.info(f"[DB] {len(vessels)} unique vessels in bbox ±{window_hours}h of {acq_time.isoformat()}")
    return vessels


def check_ais_coverage(
    bbox: List[float],
    acq_time: datetime,
    window_hours: float = 24.0,
) -> float:
    """
    Returns a 'Coverage Confidence' score (0.0 to 1.0).
    Calculated as (unique_vessels_in_24h / expected_baseline).
    If 0 vessels are found in a 24h window, confidence is 0.0 (Data Gap).
    """
    if not os.path.exists(DB_PATH):
        return 0.0

    min_lon, min_lat, max_lon, max_lat = bbox
    t0 = (acq_time - timedelta(hours=window_hours)).timestamp()
    t1 = (acq_time + timedelta(hours=window_hours)).timestamp()

    conn = sqlite3.connect(DB_PATH)
    # Just count unique MMSIs in the wider window
    count = conn.execute("""
        SELECT COUNT(DISTINCT mmsi)
        FROM   positions
        WHERE  ts   BETWEEN ? AND ?
          AND  lon  BETWEEN ? AND ?
          AND  lat  BETWEEN ? AND ?
    """, (t0, t1, min_lon, max_lon, min_lat, max_lat)).fetchone()[0]
    conn.close()

    # For major shipping lanes (Aden, Malacca), we expect at least 10-20 vessels per 24h.
    # If we see 0, it's a 100% confirmed data gap.
    if count == 0:
        return 0.0
    
    return min(1.0, count / 5.0)  # Simple heuristic: 5+ vessels = "Healthy enough to attempt fusion"


# ---------------------------------------------------------------------------
# GFW 4Wings backfill (vessel presence — 2012 to 96h ago)
# ---------------------------------------------------------------------------

def backfill_from_gfw(bbox: List[float], start: datetime, end: datetime) -> int:
    """
    Query GFW AIS Vessel Presence dataset for a bbox+time window and write
    results to the local DB.  Uses gfwapiclient (bypasses Cloudflare TLS).

    Returns number of records inserted.
    """
    from ais_client import GFWClient
    try:
        client = GFWClient()
    except ValueError as e:
        log.warning(f"[GFW] {e} — skipping backfill")
        return 0

    start_str = start.strftime("%Y-%m-%d")
    end_str   = end.strftime("%Y-%m-%d")

    vessels = client.fetch_vessel_presence(bbox, start_str, end_str)
    if not vessels:
        log.info("[GFW] Backfill returned 0 vessels (data may be within 4-week lag)")
        return 0

    conn = _get_db()
    inserted = 0
    for v in vessels:
        try:
            ts_raw = v.get("timestamp", "")
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00")).timestamp() if ts_raw else start.timestamp()
        except Exception:
            ts = start.timestamp()
        _insert_position(conn, {
            "mmsi":      v["mmsi"],
            "ts":        ts,
            "lat":       v["lat"],
            "lon":       v["lon"],
            "speed":     v.get("speed"),
            "course":    v.get("course"),
            "name":      v.get("vessel_name", ""),
            "ship_type": None,
            "flag":      v.get("flag", ""),
            "source":    "gfw",
        })
        inserted += 1

    conn.commit()
    conn.close()
    log.info(f"[GFW] Inserted {inserted} positions into ais_history.db")
    return inserted


# ---------------------------------------------------------------------------
# AisStream WebSocket collector
# ---------------------------------------------------------------------------

async def _collect_loop(bbox: List[float], api_key: str, conn: sqlite3.Connection):
    import websockets

    subscribe_msg = {
        "APIKey": api_key,
        "BoundingBoxes": [[[bbox[1], bbox[0]], [bbox[3], bbox[2]]]],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    url = "wss://stream.aisstream.io/v0/stream"
    retry_delay = 5
    batch = []
    last_prune = time.time()

    while True:
        try:
            log.info(f"Connecting to AisStream ({bbox})...")
            async with websockets.connect(url, ping_interval=20) as ws:
                await ws.send(json.dumps(subscribe_msg))
                retry_delay = 5
                log.info("AisStream connected")

                async for raw in ws:
                    data = json.loads(raw)
                    mmsi = data.get("MetaData", {}).get("MMSI")
                    if not mmsi:
                        continue

                    msg_type = data.get("MessageType")
                    content  = data.get("Message", {})
                    ts_str   = data.get("MetaData", {}).get("time_utc")
                    try:
                        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
                    except Exception:
                        ts = datetime.now(timezone.utc).timestamp()

                    if msg_type == "PositionReport":
                        pos = content.get("PositionReport", {})
                        lat = pos.get("Latitude")
                        lon = pos.get("Longitude")
                        if lat is None or lon is None:
                            continue
                        batch.append({
                            "mmsi": str(mmsi), "ts": ts,
                            "lat": lat, "lon": lon,
                            "speed": pos.get("Sog"), "course": pos.get("Cog"),
                            "name": None, "ship_type": None, "flag": None,
                            "source": "aisstream",
                        })

                    if len(batch) >= 500:
                        conn.executemany("""
                            INSERT INTO positions
                            (mmsi,ts,lat,lon,speed,course,name,ship_type,flag,source)
                            VALUES (:mmsi,:ts,:lat,:lon,:speed,:course,:name,:ship_type,:flag,:source)
                        """, batch)
                        conn.commit()
                        log.info(f"Flushed {len(batch)} positions to DB")
                        batch.clear()

                    # Prune once per hour
                    if time.time() - last_prune > 3600:
                        _prune_old(conn)
                        last_prune = time.time()

        except Exception as e:
            log.warning(f"AisStream error: {e} — retry in {retry_delay}s")
            if batch:
                conn.executemany("""
                    INSERT INTO positions
                    (mmsi,ts,lat,lon,speed,course,name,ship_type,flag,source)
                    VALUES (:mmsi,:ts,:lat,:lon,:speed,:course,:name,:ship_type,:flag,:source)
                """, batch)
                conn.commit()
                batch.clear()
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 120)


def run_collector(aoi_names: List[str]):
    """Start continuous collection for named AOIs from aoi_config.json."""
    aoi_config_path = os.path.join(os.path.dirname(__file__), "aoi_config.json")
    with open(aoi_config_path) as f:
        aoi_config = json.load(f)

    api_key = os.getenv("AISSTREAM_API_KEY", "")
    if not api_key:
        log.error("AISSTREAM_API_KEY not set")
        sys.exit(1)

    conn = _get_db()

    aoi_map = {a["name"]: a for a in aoi_config.get("aois", [])}

    async def _all():
        tasks = []
        for name in aoi_names:
            if name not in aoi_map:
                log.warning(f"AOI '{name}' not in aoi_config.json — skipping")
                continue
            bbox = aoi_map[name]["bbox"]
            log.info(f"Collecting AIS for {name} bbox={bbox}")
            tasks.append(_collect_loop(bbox, api_key, conn))
        if not tasks:
            log.error("No valid AOIs to collect")
            return
        await asyncio.gather(*tasks)

    loop = asyncio.new_event_loop()

    def _stop(sig, frame):
        log.info("Shutting down collector...")
        loop.stop()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        loop.run_until_complete(_all())
    finally:
        conn.close()
        loop.close()


# ---------------------------------------------------------------------------
# HTTP query API  (used by SAR pipeline when running remotely)
# ---------------------------------------------------------------------------

def start_api_server(host: str = "0.0.0.0", port: int = 8080):
    """
    Minimal Flask HTTP server so the local SAR pipeline can query the cloud DB.

    Endpoints:
      GET /health                          → {"status":"ok","records":N}
      GET /query?bbox=W,S,E,N&time=ISO8601 → JSON array of vessels
      GET /stats                           → DB stats JSON
    """
    try:
        from flask import Flask, jsonify, request as freq
    except ImportError:
        log.error("flask not installed — run: pip install flask")
        return

    app = Flask("ais-api")

    @app.get("/health")
    def health():
        try:
            conn = sqlite3.connect(DB_PATH)
            n = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
            conn.close()
            return jsonify({"status": "ok", "records": n})
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    @app.get("/query")
    def query():
        try:
            bbox_str = freq.args.get("bbox", "")
            time_str = freq.args.get("time", "")
            window   = float(freq.args.get("window", "3.0"))
            if not bbox_str or not time_str:
                return jsonify({"error": "bbox and time required"}), 400
            bbox = [float(x) for x in bbox_str.split(",")]
            acq  = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            vessels = query_ais_snapshot(bbox, acq, window)
            return jsonify(vessels)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.get("/stats")
    def stats():
        try:
            conn = sqlite3.connect(DB_PATH)
            n     = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
            oldest = conn.execute("SELECT MIN(ts) FROM positions").fetchone()[0]
            newest = conn.execute("SELECT MAX(ts) FROM positions").fetchone()[0]
            conn.close()
            return jsonify({
                "records": n,
                "oldest": datetime.fromtimestamp(oldest, tz=timezone.utc).isoformat() if oldest else None,
                "newest": datetime.fromtimestamp(newest, tz=timezone.utc).isoformat() if newest else None,
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    log.info(f"[API] Starting HTTP server on {host}:{port}")
    app.run(host=host, port=port, threaded=True)


def run_all(aoi_names: list, api_port: int = 8080):
    """Run AIS collector + HTTP API server concurrently."""
    import threading
    api_thread = threading.Thread(
        target=start_api_server, kwargs={"port": api_port}, daemon=True
    )
    api_thread.start()
    log.info(f"[API] HTTP query server running on :{api_port}")
    run_collector(aoi_names)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AIS collector / query tool")
    sub = parser.add_subparsers(dest="cmd")

    # collect
    p_col = sub.add_parser("collect", help="Start continuous AisStream collector")
    p_col.add_argument("--aois", required=True, help="Comma-separated AOI names from aoi_config.json")

    # query
    p_qry = sub.add_parser("query", help="Query local DB for a SAR acquisition")
    p_qry.add_argument("--bbox", required=True, help="min_lon,min_lat,max_lon,max_lat")
    p_qry.add_argument("--time", required=True, help="ISO8601 acquisition time (UTC)")
    p_qry.add_argument("--window", type=float, default=3.0, help="Hours either side (default 3)")

    # backfill
    p_bf = sub.add_parser("backfill", help="Backfill from GFW (needs VPN if blocked)")
    p_bf.add_argument("--bbox", required=True)
    p_bf.add_argument("--start", required=True)
    p_bf.add_argument("--end",   required=True)

    # stats
    sub.add_parser("stats", help="Show DB statistics")

    # serve  — collect + HTTP API together (used in cloud deployment)
    p_srv = sub.add_parser("serve", help="Collect AIS + serve HTTP query API")
    p_srv.add_argument("--aois", required=True, help="Comma-separated AOI names")
    p_srv.add_argument("--port", type=int, default=8080, help="HTTP API port (default 8080)")

    args = parser.parse_args()

    if args.cmd == "serve":
        run_all([a.strip() for a in args.aois.split(",")], api_port=args.port)

    elif args.cmd == "collect":
        run_collector([a.strip() for a in args.aois.split(",")])

    elif args.cmd == "query":
        bbox = [float(x) for x in args.bbox.split(",")]
        acq  = datetime.fromisoformat(args.time.replace("Z", "+00:00"))
        vessels = query_ais_snapshot(bbox, acq, args.window)
        print(json.dumps(vessels, indent=2))
        print(f"\n{len(vessels)} vessels found")

    elif args.cmd == "backfill":
        bbox  = [float(x) for x in args.bbox.split(",")]
        start = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
        end   = datetime.fromisoformat(args.end.replace("Z", "+00:00"))
        n = backfill_from_gfw(bbox, start, end)
        print(f"Inserted {n} records")

    elif args.cmd == "stats":
        if not os.path.exists(DB_PATH):
            print("ais_history.db not found")
        else:
            conn = sqlite3.connect(DB_PATH)
            total = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
            oldest = conn.execute("SELECT MIN(ts) FROM positions").fetchone()[0]
            newest = conn.execute("SELECT MAX(ts) FROM positions").fetchone()[0]
            mmsis  = conn.execute("SELECT COUNT(DISTINCT mmsi) FROM positions").fetchone()[0]
            conn.close()
            print(f"Records  : {total:,}")
            print(f"Vessels  : {mmsis:,}")
            if oldest:
                print(f"Oldest   : {datetime.fromtimestamp(oldest, tz=timezone.utc).isoformat()}")
                print(f"Newest   : {datetime.fromtimestamp(newest, tz=timezone.utc).isoformat()}")

    else:
        parser.print_help()
