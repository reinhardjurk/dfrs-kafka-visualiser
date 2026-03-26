#!/usr/bin/env python3
"""
SENSORIS Live Map Server
Pulls SENSORIS messages from Kafka (or a text-format file for offline use),
parses them directly from protobuf, and pushes events to a browser map via SSE.

Usage
-----
  python server.py                             # connect to Kafka (latest)
  python server.py --from-file ../out.txt      # replay a text-format file
  python server.py --offset earliest           # Kafka from the beginning
  python server.py --port 8080 --max-events 2000
"""

import sys
import os
# make the sensoris package importable when running from the sensoris-map subdir
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import re
import json
import time
import queue
import threading
import argparse
from datetime import datetime, timezone
from collections import deque, defaultdict

from flask import Flask, Response, jsonify, send_from_directory
import requests as http_requests

from sensoris.protobuf.messages import data_pb2

# ── Flask app ─────────────────────────────────────────────────────────────────

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
app = Flask(__name__, static_folder=STATIC_DIR)

# ── global state ──────────────────────────────────────────────────────────────

MAX_EVENTS_DEFAULT = 5_000

events: deque = deque(maxlen=MAX_EVENTS_DEFAULT)
events_lock = threading.Lock()
_event_id = 0                           # monotonic counter for event IDs

sse_queues: list[queue.Queue] = []
sse_lock = threading.Lock()

label_counts: dict[str, int] = defaultdict(int)

_cfg: dict = {}                         # populated in main()

# ── helpers ───────────────────────────────────────────────────────────────────

_TS_MIN = 946_684_800_000   # 2000-01-01
_TS_MAX = 4_102_444_800_000 # 2100-01-01


def is_valid_ts(ms) -> bool:
    return bool(ms) and _TS_MIN <= ms <= _TS_MAX


def to_iso(ms) -> str | None:
    if not is_valid_ts(ms):
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def to_readable(ms) -> str:
    if not is_valid_ts(ms):
        return "unknown"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "Time: %d %b %Y %H:%M:%S UTC"
    )


def enum_name(pb_obj, field_name: str) -> str:
    """Return the string name of an enum field on a protobuf message object."""
    value = getattr(pb_obj, field_name, 0)
    try:
        fd = pb_obj.DESCRIPTOR.fields_by_name[field_name]
        return fd.enum_type.values_by_number[value].name
    except (KeyError, AttributeError):
        return f"VALUE_{value}"


def _next_id() -> int:
    global _event_id
    _event_id += 1
    return _event_id


def make_event(
    lat: float,
    lon: float,
    ts_ms,
    category: str,
    subtype: str,
    event_type: str,
    raw: str = "",
) -> dict:
    """Build the canonical event dict that is sent to the browser."""
    label = (
        event_type
        if event_type and not event_type.startswith("UNKNOWN")
        else subtype
    )
    return {
        "id":            _next_id(),
        "timestamp":     to_iso(ts_ms),
        "time_human":    to_readable(ts_ms),
        "timestamp_ms":  ts_ms,
        "latitude":      round(lat, 7),
        "longitude":     round(lon, 7),
        "category":      category,
        "event_subtype": subtype,
        "event_type":    event_type,
        "event_label":   label,
        "raw_message":   raw[:400] + ("\n…(truncated)" if len(raw) > 400 else ""),
    }


# ── protobuf parser ───────────────────────────────────────────────────────────

def _positions_from_event_group(eg) -> list[dict]:
    """Extract all vehicle positions from the localization_category of an event group."""
    positions = []
    for vpo in eg.localization_category.vehicle_position_and_orientation:
        lat_raw = vpo.position_and_accuracy.geographic_wgs84.latitude.value
        lon_raw = vpo.position_and_accuracy.geographic_wgs84.longitude.value
        ts_raw  = vpo.envelope.timestamp.posix_time.value
        if lat_raw or lon_raw:
            positions.append({
                "lat": lat_raw / 1e8,
                "lon": lon_raw / 1e8,
                "ts":  ts_raw if is_valid_ts(ts_raw) else None,
            })
    return positions


def parse_pb(data_msg, raw_text: str = "") -> list[dict]:
    """
    Parse a DataMessages protobuf object directly into a list of event dicts.
    One dict per event (weather condition, hazard, slow-down, etc.).
    """
    rows: list[dict] = []

    for dm in data_msg.data_message:
        msg_ts = dm.envelope.ids.message_id.value  # 0 if not present

        for eg in dm.event_group:
            positions = _positions_from_event_group(eg)
            if not positions:
                continue

            # prefer a position that carries its own timestamp (= event position)
            canon = next((p for p in positions if p["ts"]), positions[0])
            ts = canon["ts"] or (msg_ts if is_valid_ts(msg_ts) else None)
            lat, lon = canon["lat"], canon["lon"]

            # ── weather_category ─────────────────────────────────────────────
            wc = eg.weather_category
            for ev in wc.visibility_condition:
                rows.append(make_event(lat, lon, ts, "weather",
                    "visibility_condition",
                    enum_name(ev.type_and_confidence, "type"), raw_text))

            for ev in wc.precipitation:
                rows.append(make_event(lat, lon, ts, "weather",
                    "precipitation",
                    enum_name(ev.type_and_confidence, "type"), raw_text))

            for ev in wc.wind_condition:
                rows.append(make_event(lat, lon, ts, "weather",
                    "wind_condition",
                    enum_name(ev.type_and_confidence, "type"), raw_text))

            for ev in wc.atmosphere_condition:
                rows.append(make_event(lat, lon, ts, "weather",
                    "atmosphere_condition", "ATMOSPHERE_CONDITION", raw_text))

            # ── traffic_events_category ──────────────────────────────────────
            tc = eg.traffic_events_category
            for ev in tc.hazard:
                rows.append(make_event(lat, lon, ts, "traffic_event",
                    "hazard",
                    enum_name(ev.type_and_confidence, "type"), raw_text))

            for ev in tc.dangerous_slow_down:
                rows.append(make_event(lat, lon, ts, "traffic_event",
                    "dangerous_slow_down", "DANGEROUS_SLOW_DOWN", raw_text))

            for ev in tc.roadworks:
                rows.append(make_event(lat, lon, ts, "traffic_event",
                    "roadworks", "ROADWORKS", raw_text))

            for ev in tc.road_surface_condition:
                rows.append(make_event(lat, lon, ts, "traffic_event",
                    "road_surface_condition",
                    enum_name(ev.type_and_confidence, "type"), raw_text))

            for ev in tc.road_weather_condition:
                rows.append(make_event(lat, lon, ts, "traffic_event",
                    "road_weather_condition",
                    enum_name(ev.type_and_confidence, "type"), raw_text))

            for ev in tc.road_obstruction_condition:
                rows.append(make_event(lat, lon, ts, "traffic_event",
                    "road_obstruction_condition",
                    enum_name(ev.type_and_confidence, "type"), raw_text))

            for ev in tc.vulnerable_road_user_condition:
                rows.append(make_event(lat, lon, ts, "traffic_event",
                    "vulnerable_road_user_condition",
                    enum_name(ev.type_and_confidence, "type"), raw_text))

            for ev in tc.e_call_status:
                rows.append(make_event(lat, lon, ts, "traffic_event",
                    "ecall_status", "ECALL", raw_text))

    return rows


# ── text-format parser (used by --from-file) ──────────────────────────────────

_SKIP_TYPES = {"READING_FUSION", "TRIGGERED_MANUALLY", "TRIGGERED_AUTOMATED_RARE"}


def _extract_int(lines, key, start=0) -> int | None:
    for i in range(start, len(lines)):
        if re.search(rf"\b{re.escape(key)}\s*\{{", lines[i]):
            for j in range(i + 1, min(i + 5, len(lines))):
                m = re.search(r"value:\s*(-?\d+)", lines[j])
                if m:
                    return int(m.group(1))
    return None


def parse_text_block(lines: list[str], raw_text: str) -> list[dict]:
    """
    Parse one text-format block (the printable protobuf format written by
    consumer.py's print(data_msg)).  Mirrors the logic in parse_sensoris.py.
    """
    # collect vehicle positions
    positions: list[dict] = []
    vpo_pat = re.compile(r"\bvehicle_position_and_orientation\s*\{")
    i = 0
    while i < len(lines):
        if vpo_pat.search(lines[i]):
            start_i = i
            depth, ts_ms, lat, lon = 0, None, None, None
            for j in range(i, len(lines)):
                depth += lines[j].count("{") - lines[j].count("}")
                if "posix_time" in lines[j]:
                    m = re.search(r"value:\s*(\d+)", lines[j + 1]) if j + 1 < len(lines) else None
                    if m:
                        ts_ms = int(m.group(1))
                if "longitude" in lines[j]:
                    m = re.search(r"value:\s*(\d+)", lines[j + 1]) if j + 1 < len(lines) else None
                    if m:
                        lon = int(m.group(1))
                if "latitude" in lines[j]:
                    m = re.search(r"value:\s*(\d+)", lines[j + 1]) if j + 1 < len(lines) else None
                    if m:
                        lat = int(m.group(1))
                if depth <= 0 and j > start_i:
                    i = j
                    break
            if lat and lon:
                positions.append({
                    "lat": lat / 1e8,
                    "lon": lon / 1e8,
                    "ts": ts_ms if is_valid_ts(ts_ms) else None,
                })
        i += 1

    if not positions:
        return []

    canon = next((p for p in positions if p["ts"]), positions[0])
    msg_ts = _extract_int(lines, "message_id")
    ts = canon["ts"] or (msg_ts if is_valid_ts(msg_ts) else None)
    lat, lon = canon["lat"], canon["lon"]

    rows: list[dict] = []
    i = 0
    while i < len(lines):
        line = lines[i]

        if re.search(r"\bweather_category\s*\{", line):
            cs, depth = i, 0
            subtype, etype, cat_ts = None, None, None
            for j in range(cs, len(lines)):
                depth += lines[j].count("{") - lines[j].count("}")
                if "visibility_condition" in lines[j]:
                    subtype = "visibility_condition"
                elif "precipitation_condition" in lines[j]:
                    subtype = "precipitation_condition"
                elif "road_condition" in lines[j]:
                    subtype = "road_condition"
                if "posix_time" in lines[j]:
                    m = re.search(r"value:\s*(\d+)", lines[j + 1]) if j + 1 < len(lines) else None
                    if m:
                        cat_ts = int(m.group(1))
                m = re.search(r"^\s+type:\s+([A-Z_]+)", lines[j])
                if m and m.group(1) not in _SKIP_TYPES:
                    etype = m.group(1)
                if depth <= 0 and j > cs:
                    i = j
                    break
            ets = cat_ts if is_valid_ts(cat_ts) else ts
            rows.append(make_event(lat, lon, ets, "weather",
                                   subtype or "unknown", etype or "UNKNOWN", raw_text))

        elif re.search(r"\btraffic_events_category\s*\{", line):
            cs, depth = i, 0
            subtype, etype, cat_ts = None, None, None
            for j in range(cs, len(lines)):
                depth += lines[j].count("{") - lines[j].count("}")
                if "dangerous_slow_down" in lines[j]:
                    subtype = "dangerous_slow_down"
                elif re.search(r"\bhazard\s*\{", lines[j]):
                    subtype = "hazard"
                if "posix_time" in lines[j]:
                    m = re.search(r"value:\s*(\d+)", lines[j + 1]) if j + 1 < len(lines) else None
                    if m:
                        cat_ts = int(m.group(1))
                m = re.search(r"^\s+type:\s+([A-Z_]+)", lines[j])
                if m and m.group(1) not in _SKIP_TYPES:
                    etype = m.group(1)
                if depth <= 0 and j > cs:
                    i = j
                    break
            ets = cat_ts if is_valid_ts(cat_ts) else ts
            if not etype and subtype == "dangerous_slow_down":
                etype = "DANGEROUS_SLOW_DOWN"
            rows.append(make_event(lat, lon, ets, "traffic_event",
                                   subtype or "unknown", etype or "UNKNOWN", raw_text))

        i += 1

    return rows


# ── event storage + SSE broadcast ─────────────────────────────────────────────

def add_events(new_rows: list[dict]):
    """Append events to the ring buffer and fan-out to all SSE clients."""
    if not new_rows:
        return
    with events_lock:
        for r in new_rows:
            events.append(r)
            label_counts[r["event_label"]] += 1

    payload = json.dumps(new_rows, default=str)
    with sse_lock:
        dead = []
        for q in sse_queues:
            try:
                q.put_nowait(payload)
            except queue.Full:
                dead.append(q)
        for q in dead:
            sse_queues.remove(q)


# ── Kafka worker ──────────────────────────────────────────────────────────────

def _oauth_cb(oauth_config):
    url = "https://emea.prod.alpha.sso.bmwgroup.com/auth/oauth2/realms/root/realms/alpha/access_token"
    r = http_requests.post(url, data={
        "grant_type":    "client_credentials",
        "client_id":     "182a00cc-4e35-44cf-80df-73ac4a92657b",
        "client_secret": "ScFEEoGvy5KXliZIxH1ugMhBZ4ofUh8pjR4tgPWi",
        "scope":         "machine2machine",
    }, timeout=10)
    r.raise_for_status()
    token = r.json()
    expiry = int(time.time()) + int(token.get("expires_in", 3600))
    return (token["access_token"], expiry, "kafka-client", {})


def kafka_worker():
    from confluent_kafka import Consumer
    conf = {
        "bootstrap.servers": "gateway.catena-dmz-euc-stp.aws.bmw.cloud:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism":    "OAUTHBEARER",
        "oauth_cb":          _oauth_cb,
        "group.id":          "sensoris-map-live",
        "auto.offset.reset": _cfg.get("offset", "latest"),
    }
    print("Connecting to Kafka …")
    consumer = Consumer(conf)
    consumer.subscribe(["bmw.cdcerd.CDCHazardsForDfrsPROD.v1"])
    print("Kafka consumer ready — listening for messages")
    try:
        while True:
            msg = consumer.poll(5.0)
            if msg is None or msg.error():
                continue
            try:
                dm = data_pb2.DataMessages()
                dm.ParseFromString(msg.value())
                raw = str(dm)
                rows = parse_pb(dm, raw)
                if rows:
                    add_events(rows)
                    print(f"  +{len(rows)} event(s) — buffer: {len(events)}")
            except Exception as exc:
                print(f"  Parse error: {exc}")
    except Exception as exc:
        print(f"Kafka error: {exc}")
    finally:
        consumer.close()


# ── file worker ───────────────────────────────────────────────────────────────

def file_worker(path: str):
    """
    Read a text-format protobuf file (consumer.py output), parse all blocks,
    and push events in small batches so the browser sees them arrive in waves.
    """
    blocks: list[list[str]] = []
    block: list[str] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.rstrip("\n")
            if s == "" and block:
                blocks.append(block)
                block = []
            else:
                block.append(s)
    if block:
        blocks.append(block)

    print(f"File: {len(blocks)} blocks found in {path}")
    total = 0
    BATCH = 40
    for i in range(0, len(blocks), BATCH):
        rows: list[dict] = []
        for b in blocks[i : i + BATCH]:
            rows.extend(parse_text_block(b, "\n".join(b)))
        if rows:
            add_events(rows)
            total += len(rows)
        time.sleep(0.03)  # small delay → browser sees events arriving live
    print(f"File load complete — {total} events pushed")


# ── Flask routes ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(STATIC_DIR, "index.html")


@app.route("/api/events")
def api_events():
    with events_lock:
        ev = list(events)
    return jsonify(ev)


@app.route("/api/stats")
def api_stats():
    with events_lock:
        total = len(events)
        counts = dict(label_counts)
    return jsonify({"total": total, "by_label": counts})


@app.route("/api/health")
def api_health():
    return jsonify({
        "status": "ok",
        "mode": "file" if _cfg.get("from_file") else "kafka",
        "source": _cfg.get("from_file") or "kafka",
        "events_buffered": len(events),
        "max_events": events.maxlen,
    })


@app.route("/api/config")
def api_config():
    """Return server-side startup configuration for the browser to consume."""
    geo = _cfg.get("geo")
    return jsonify({"geo": geo})   # geo is None or {"lat":…,"lon":…,"radius_km":…}


@app.route("/api/stream")
def api_stream():
    """
    Server-Sent Events endpoint.
    On connect: immediately send all buffered events.
    Then: push any new events as they arrive.
    Heartbeat every ~25 s keeps the connection alive through proxies.
    """
    def generate():
        q: queue.Queue = queue.Queue(maxsize=200)
        with sse_lock:
            sse_queues.append(q)
        try:
            # send the full buffer to the newly connected client
            with events_lock:
                existing = list(events)
            if existing:
                yield f"data: {json.dumps(existing, default=str)}\n\n"

            # then stream live updates
            while True:
                try:
                    data = q.get(timeout=25)
                    yield f"data: {data}\n\n"
                except queue.Empty:
                    yield "data: []\n\n"   # heartbeat — browser ignores empty arrays
        except GeneratorExit:
            pass
        finally:
            with sse_lock:
                if q in sse_queues:
                    sse_queues.remove(q)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    global events

    parser = argparse.ArgumentParser(
        description="SENSORIS Live Map Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
  python server.py
  python server.py --from-file ../out.txt
  python server.py --from-file ../out.txt --center 48.137,11.575 --radius 50
  python server.py --offset earliest --max-events 10000
  python server.py --port 8080 --center 52.520,13.405 --radius 100
""",
    )
    parser.add_argument("--from-file", metavar="PATH",
                        help="Load from a text-format out.txt instead of Kafka")
    parser.add_argument("--port", type=int, default=5000,
                        help="HTTP port (default: 5000)")
    parser.add_argument("--max-events", type=int, default=MAX_EVENTS_DEFAULT,
                        help=f"Ring-buffer size (default: {MAX_EVENTS_DEFAULT})")
    parser.add_argument("--offset", choices=["latest", "earliest"], default="latest",
                        help="Kafka start offset (default: latest)")

    geo = parser.add_argument_group("geo filter (both --center and --radius required)")
    geo.add_argument("--center", metavar="LAT,LON",
                     help="Pre-apply a geo filter on startup, e.g. 48.137,11.575")
    geo.add_argument("--radius", metavar="KM", type=float,
                     help="Radius in km for the geo filter")

    args = parser.parse_args()

    if (args.center is None) != (args.radius is None):
        parser.error("--center and --radius must be supplied together")

    _cfg["offset"]    = args.offset
    _cfg["from_file"] = args.from_file

    if args.center:
        try:
            lat_s, lon_s = args.center.split(",")
            lat_c, lon_c = float(lat_s.strip()), float(lon_s.strip())
        except ValueError:
            parser.error("--center must be LAT,LON with decimal numbers, e.g. 48.137,11.575")
        if not (-90 <= lat_c <= 90):
            parser.error("--center latitude must be between -90 and 90")
        if not (-180 <= lon_c <= 180):
            parser.error("--center longitude must be between -180 and 180")
        if args.radius <= 0:
            parser.error("--radius must be greater than 0")
        _cfg["geo"] = {"lat": lat_c, "lon": lon_c, "radius_km": args.radius}
        print(f"  Geo filter: centre ({lat_c}, {lon_c}), radius {args.radius} km")
    else:
        _cfg["geo"] = None

    # re-create the ring buffer with the requested size
    events = deque(maxlen=args.max_events)

    # start background data worker
    if args.from_file:
        t = threading.Thread(target=file_worker, args=(args.from_file,), daemon=True)
    else:
        t = threading.Thread(target=kafka_worker, daemon=True)
    t.start()

    print(f"\n  SENSORIS Live Map  →  http://localhost:{args.port}\n")
    app.run(host="0.0.0.0", port=args.port, threaded=True, debug=False)


if __name__ == "__main__":
    main()
