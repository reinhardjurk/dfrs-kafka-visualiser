#!/usr/bin/env python3
"""
SENSORIS Kafka output parser.
Parses the text-format protobuf output from consumer.py,
prints message type statistics, and writes a kepler.gl-compatible CSV.

Optional filters
----------------
  --center LAT,LON   centre of the spatial filter (decimal degrees)
  --radius KM        keep only events within this many km of --center
  --time YYYYMMDDHHMM  centre of the time filter
  --window MINUTES   keep only events within ±MINUTES of --time
"""

import re
import csv
import sys
import math
import argparse
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ── helpers ──────────────────────────────────────────────────────────────────

def deg(raw_value: int) -> float:
    """SENSORIS stores lat/lon as integer × 1e-8 degrees (exponent = 8)."""
    return raw_value / 1e8


def posix_ms_to_human(ms: int) -> str:
    """Convert POSIX milliseconds → ISO-8601 UTC string (kepler.gl-compatible)."""
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    except (ValueError, OSError, OverflowError):
        return f"invalid_ts({ms})"


def posix_ms_to_readable(ms: int) -> str:
    """Convert POSIX milliseconds → human-readable string kepler.gl won't reparse."""
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
            "Time: %d %b %Y %H:%M:%S UTC"
        )
    except (ValueError, OSError, OverflowError):
        return f"invalid_ts({ms})"


def is_valid_timestamp_ms(v: int) -> bool:
    """Sanity check: accept only timestamps between year 2000 and 2100."""
    return 946_684_800_000 <= v <= 4_102_444_800_000


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance in kilometres between two points."""
    R = 6_371.0  # Earth radius in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def parse_center(value: str) -> tuple[float, float]:
    """Parse 'LAT,LON' string into (lat, lon) floats."""
    parts = value.split(",")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"--center must be LAT,LON (e.g. 48.137,11.576), got: {value!r}"
        )
    try:
        return float(parts[0].strip()), float(parts[1].strip())
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"--center values must be decimal numbers, got: {value!r}"
        )


def parse_timespec(value: str) -> datetime:
    """Parse 'YYYYMMDDHHMM' string into a UTC-aware datetime."""
    try:
        return datetime.strptime(value, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"--time must be YYYYMMDDHHMM (e.g. 202603261045), got: {value!r}"
        )


# ── block parser ─────────────────────────────────────────────────────────────

def read_blocks(path: str):
    """
    Yield individual message blocks (envelope + data_message pairs).
    Each block is a list of text lines.
    """
    with open(path, "r", encoding="utf-8") as f:
        block: list[str] = []
        for line in f:
            stripped = line.rstrip("\n")
            if stripped == "" and block:
                yield block
                block = []
            else:
                block.append(stripped)
        if block:
            yield block


def extract_value(lines: list[str], key: str, start: int = 0) -> int | None:
    """
    Find the first occurrence of  `<key> {` after *start*,
    then return the integer `value: N` inside it.
    """
    for i in range(start, len(lines)):
        if re.search(rf"\b{re.escape(key)}\s*\{{", lines[i]):
            for j in range(i + 1, min(i + 5, len(lines))):
                m = re.search(r"value:\s*(-?\d+)", lines[j])
                if m:
                    return int(m.group(1))
    return None


def find_line(lines: list[str], pattern: str, start: int = 0) -> int:
    """Return index of first line matching *pattern* at or after *start*."""
    rx = re.compile(pattern)
    for i in range(start, len(lines)):
        if rx.search(lines[i]):
            return i
    return -1


# ── per-block extraction ──────────────────────────────────────────────────────

def parse_block(lines: list[str], raw_text: str) -> list[dict]:
    """
    Parse one message block and return a list of event rows.
    A block may contain multiple vehicle_position_and_orientation entries
    and multiple category events; we match each event to its nearest position.
    """
    rows: list[dict] = []

    # ── collect all vehicle_position_and_orientation positions ──────────────
    positions: list[dict] = []
    vpo_pat = re.compile(r"\bvehicle_position_and_orientation\s*\{")
    i = 0
    while i < len(lines):
        if vpo_pat.search(lines[i]):
            vpo_start = i
            ts_ms = None
            lat = None
            lon = None
            depth = 0
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
                if depth <= 0 and j > vpo_start:
                    i = j
                    break
            if lat is not None and lon is not None:
                positions.append({"ts_ms": ts_ms, "lat": lat, "lon": lon})
        i += 1

    # ── find best canonical position ─────────────────────────────────────────
    canonical = next((p for p in positions if p["ts_ms"] is not None), None)
    if canonical is None and positions:
        canonical = positions[0]

    # ── extract event timestamp from data_message envelope ───────────────────
    msg_ts_ms = extract_value(lines, "message_id")

    def pick_ts(*candidates):
        for c in candidates:
            if c is not None and is_valid_timestamp_ms(c):
                return c
        return None

    event_ts_ms = pick_ts(
        canonical["ts_ms"] if canonical else None,
        msg_ts_ms,
    )

    if canonical is None:
        return []   # no position → skip block

    lat_deg = deg(canonical["lat"])
    lon_deg = deg(canonical["lon"])
    ts_human = posix_ms_to_human(event_ts_ms) if event_ts_ms else "unknown"

    # ── iterate over event categories ────────────────────────────────────────
    i = 0
    while i < len(lines):
        line = lines[i]

        # ── weather_category ─────────────────────────────────────────────────
        if re.search(r"\bweather_category\s*\{", line):
            cat_start = i
            depth = 0
            event_subtype = None
            event_type_val = None
            cat_ts_ms = None
            for j in range(cat_start, len(lines)):
                depth += lines[j].count("{") - lines[j].count("}")
                if "visibility_condition" in lines[j]:
                    event_subtype = "visibility_condition"
                elif "precipitation_condition" in lines[j]:
                    event_subtype = "precipitation_condition"
                elif "road_condition" in lines[j]:
                    event_subtype = "road_condition"
                if "posix_time" in lines[j]:
                    m2 = re.search(r"value:\s*(\d+)", lines[j + 1]) if j + 1 < len(lines) else None
                    if m2:
                        cat_ts_ms = int(m2.group(1))
                m = re.search(r"^\s+type:\s+([A-Z_]+)", lines[j])
                if m and m.group(1) not in ("READING_FUSION", "TRIGGERED_MANUALLY", "TRIGGERED_AUTOMATED_RARE"):
                    event_type_val = m.group(1)
                if depth <= 0 and j > cat_start:
                    i = j
                    break
            resolved_ts = pick_ts(event_ts_ms, cat_ts_ms)
            subtype = event_subtype or "unknown"
            etype = event_type_val or "UNKNOWN"
            rows.append({
                "timestamp": posix_ms_to_human(resolved_ts) if resolved_ts else "unknown",
                "time_human": posix_ms_to_readable(resolved_ts) if resolved_ts else "unknown",
                "timestamp_ms": resolved_ts,
                "latitude": round(lat_deg, 7),
                "longitude": round(lon_deg, 7),
                "category": "weather",
                "event_subtype": subtype,
                "event_type": etype,
                "event_label": etype if etype != "UNKNOWN" else subtype,
                "raw_message": raw_text,
            })

        # ── traffic_events_category ───────────────────────────────────────────
        elif re.search(r"\btraffic_events_category\s*\{", line):
            cat_start = i
            depth = 0
            event_subtype = None
            event_type_val = None
            cat_ts_ms = None
            for j in range(cat_start, len(lines)):
                depth += lines[j].count("{") - lines[j].count("}")
                if "dangerous_slow_down" in lines[j]:
                    event_subtype = "dangerous_slow_down"
                elif re.search(r"\bhazard\s*\{", lines[j]):
                    event_subtype = "hazard"
                if "posix_time" in lines[j]:
                    m2 = re.search(r"value:\s*(\d+)", lines[j + 1]) if j + 1 < len(lines) else None
                    if m2:
                        cat_ts_ms = int(m2.group(1))
                m = re.search(r"^\s+type:\s+([A-Z_]+)", lines[j])
                if m and m.group(1) not in ("READING_FUSION", "TRIGGERED_MANUALLY", "TRIGGERED_AUTOMATED_RARE"):
                    event_type_val = m.group(1)
                if depth <= 0 and j > cat_start:
                    i = j
                    break
            resolved_ts = pick_ts(event_ts_ms, cat_ts_ms)
            subtype = event_subtype or "unknown"
            etype = event_type_val or "UNKNOWN"
            rows.append({
                "timestamp": posix_ms_to_human(resolved_ts) if resolved_ts else "unknown",
                "time_human": posix_ms_to_readable(resolved_ts) if resolved_ts else "unknown",
                "timestamp_ms": resolved_ts,
                "latitude": round(lat_deg, 7),
                "longitude": round(lon_deg, 7),
                "category": "traffic_event",
                "event_subtype": subtype,
                "event_type": etype,
                "event_label": etype if etype != "UNKNOWN" else subtype,
                "raw_message": raw_text,
            })

        # localization_category is position context only — not a standalone event

        i += 1

    return rows


# ── filtering ─────────────────────────────────────────────────────────────────

def apply_filters(
    rows: list[dict],
    center: tuple[float, float] | None,
    radius_km: float | None,
    time_center: datetime | None,
    window_min: float | None,
) -> tuple[list[dict], dict]:
    """
    Apply spatial and/or temporal filters to *rows*.
    Returns (filtered_rows, summary_dict).
    """
    original_count = len(rows)
    dropped_spatial = 0
    dropped_temporal = 0
    dropped_no_ts = 0

    result = []
    for row in rows:
        # ── spatial filter ───────────────────────────────────────────────────
        if center is not None and radius_km is not None:
            dist = haversine_km(center[0], center[1], row["latitude"], row["longitude"])
            if dist > radius_km:
                dropped_spatial += 1
                continue

        # ── temporal filter ──────────────────────────────────────────────────
        if time_center is not None and window_min is not None:
            ts_ms = row.get("timestamp_ms")
            if ts_ms is None:
                dropped_no_ts += 1
                continue
            event_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            delta = abs((event_dt - time_center).total_seconds()) / 60.0
            if delta > window_min:
                dropped_temporal += 1
                continue

        result.append(row)

    summary = {
        "original": original_count,
        "kept": len(result),
        "dropped_spatial": dropped_spatial,
        "dropped_temporal": dropped_temporal,
        "dropped_no_ts": dropped_no_ts,
    }
    return result, summary


# ── main ─────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Parse SENSORIS Kafka output → statistics + kepler.gl CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
  # No filter — process everything
  python parse_sensoris.py out.txt

  # Only events within 25 km of Munich city centre
  python parse_sensoris.py out.txt --center 48.1374,11.5755 --radius 25

  # Only events in a ±30-minute window around noon on 26 Mar 2026
  python parse_sensoris.py out.txt --time 202603261200 --window 30

  # Both filters combined, custom output file
  python parse_sensoris.py out.txt events_filtered.csv \\
      --center 48.1374,11.5755 --radius 25 \\
      --time 202603261200 --window 30
""",
    )
    p.add_argument("input", nargs="?", default="out.txt",
                   help="Input file (default: out.txt)")
    p.add_argument("output", nargs="?", default="sensoris_events.csv",
                   help="Output CSV file (default: sensoris_events.csv)")

    geo = p.add_argument_group("spatial filter (both --center and --radius required)")
    geo.add_argument("--center", metavar="LAT,LON", type=parse_center,
                     help="Centre point, e.g. 48.1374,11.5755")
    geo.add_argument("--radius", metavar="KM", type=float,
                     help="Keep only events within this radius (km)")

    tm = p.add_argument_group("time filter (both --time and --window required)")
    tm.add_argument("--time", metavar="YYYYMMDDHHMM", type=parse_timespec,
                    help="Centre of the time window, e.g. 202603261200")
    tm.add_argument("--window", metavar="MINUTES", type=float,
                    help="Keep only events within ±MINUTES of --time")

    return p


def main():
    args = build_parser().parse_args()

    # Validate filter pairs
    if (args.center is None) != (args.radius is None):
        print("ERROR: --center and --radius must both be supplied together.", file=sys.stderr)
        sys.exit(1)
    if (args.time is None) != (args.window is None):
        print("ERROR: --time and --window must both be supplied together.", file=sys.stderr)
        sys.exit(1)

    # ── parse ─────────────────────────────────────────────────────────────────
    all_rows: list[dict] = []
    block_count = 0

    print(f"Parsing {args.input} …")
    for block in read_blocks(args.input):
        block_count += 1
        raw_text = "\n".join(block)
        rows = parse_block(block, raw_text)
        all_rows.extend(rows)

    # ── apply filters ─────────────────────────────────────────────────────────
    spatial_active = args.center is not None
    temporal_active = args.time is not None

    if spatial_active or temporal_active:
        print(f"\nFilters active:")
        if spatial_active:
            lat_c, lon_c = args.center
            print(f"  Spatial : centre ({lat_c}, {lon_c}), radius {args.radius} km")
        if temporal_active:
            print(f"  Temporal: centre {args.time.strftime('%Y-%m-%d %H:%M UTC')}, "
                  f"window ±{args.window} min  "
                  f"({(args.time - timedelta(minutes=args.window)).strftime('%H:%M')} – "
                  f"{(args.time + timedelta(minutes=args.window)).strftime('%H:%M')})")

        all_rows, fsummary = apply_filters(
            all_rows,
            center=args.center,
            radius_km=args.radius,
            time_center=args.time,
            window_min=args.window,
        )

        print(f"\n  {fsummary['original']:>6} events before filtering")
        if spatial_active:
            print(f"  {fsummary['dropped_spatial']:>6} dropped (outside radius)")
        if temporal_active:
            print(f"  {fsummary['dropped_temporal']:>6} dropped (outside time window)")
            if fsummary['dropped_no_ts']:
                print(f"  {fsummary['dropped_no_ts']:>6} dropped (no timestamp)")
        print(f"  {fsummary['kept']:>6} events remaining")

    # ── statistics ────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"  Blocks parsed : {block_count}")
    print(f"  Events total  : {len(all_rows)}")
    print(f"{'─'*60}\n")

    by_cat: dict[str, int] = defaultdict(int)
    for r in all_rows:
        by_cat[r["category"]] += 1

    print("Message type statistics (by category):")
    print(f"  {'Category':<30} {'Count':>7}")
    print(f"  {'─'*30} {'─'*7}")
    for cat, cnt in sorted(by_cat.items(), key=lambda x: -x[1]):
        print(f"  {cat:<30} {cnt:>7}")

    print()
    by_sub: dict[str, int] = defaultdict(int)
    for r in all_rows:
        key = f"{r['category']} / {r['event_subtype']}"
        by_sub[key] += 1

    print("Message type statistics (by category / subtype):")
    print(f"  {'Category / Subtype':<45} {'Count':>7}")
    print(f"  {'─'*45} {'─'*7}")
    for key, cnt in sorted(by_sub.items(), key=lambda x: -x[1]):
        print(f"  {key:<45} {cnt:>7}")

    print()
    by_type: dict[str, int] = defaultdict(int)
    for r in all_rows:
        if r["event_type"] not in ("POSITION", "UNKNOWN"):
            by_type[r["event_type"]] += 1

    if by_type:
        print("Event type value breakdown:")
        print(f"  {'Type':<35} {'Count':>7}")
        print(f"  {'─'*35} {'─'*7}")
        for typ, cnt in sorted(by_type.items(), key=lambda x: -x[1]):
            print(f"  {typ:<35} {cnt:>7}")

    # ── CSV output ────────────────────────────────────────────────────────────
    fields = [
        "timestamp", "time_human", "latitude", "longitude",
        "category", "event_subtype", "event_type", "event_label", "raw_message",
    ]
    # strip the internal-only timestamp_ms field before writing
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nCSV written → {args.output}  ({len(all_rows)} rows)")
    print("Import into kepler.gl: drag & drop the CSV file onto the map.")


if __name__ == "__main__":
    main()
