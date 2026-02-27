#!/usr/bin/env python3
"""
stat.py — Centipede RINEX metrics collector.

Walks PUB_ROOT for *.crx.gz files produced in the last SCAN_DAYS days,
aggregates per (iso_date, product) and writes:
  - STATS_DIR/metrics_daily.csv   (historical, one row per date×product)
  - STATS_DIR/metrics_latest.json (summary + windows today/7d/30d)
"""

import argparse
import csv
import json
import os
import re
from datetime import date, datetime, timedelta, timezone


# ─── product detection ───────────────────────────────────────────────────────

PRODUCT_HINTS = [
    ("1s",  re.compile(r"(?:^|/)centipede_1s(?:/|$)",  re.IGNORECASE)),
    ("30s", re.compile(r"(?:^|/)centipede_30s(?:/|$)", re.IGNORECASE)),
]
FILENAME_HINTS = [
    ("1s",  re.compile(r"_01S_", re.IGNORECASE)),
    ("30s", re.compile(r"_30S_", re.IGNORECASE)),
]


def guess_product(path: str) -> str:
    p = path.replace("\\", "/")
    for prod, rx in PRODUCT_HINTS:
        if rx.search(p):
            return prod
    base = os.path.basename(p)
    for prod, rx in FILENAME_HINTS:
        if rx.search(base):
            return prod
    return "unknown"


# ─── path parsing ─────────────────────────────────────────────────────────────

def extract_year_doy(path: str):
    """Return (year, doy) from a path containing /YYYY/DOY/."""
    parts = path.replace("\\", "/").split("/")
    for i in range(len(parts) - 2):
        y, d = parts[i], parts[i + 1]
        if len(y) == 4 and y.isdigit() and len(d) == 3 and d.isdigit():
            return int(y), int(d)
    return None


def doy_to_isodate(year: int, doy: int) -> str:
    """Convert year + day-of-year to ISO date string YYYY-MM-DD."""
    try:
        return date(year, 1, 1).replace() + timedelta(days=doy - 1)
    except Exception:
        return None


def doy_to_isodate_str(year: int, doy: int) -> str:
    try:
        d = date(year, 1, 1) + timedelta(days=doy - 1)
        return d.isoformat()          # "YYYY-MM-DD"
    except Exception:
        return f"{year}-{doy:03d}"    # fallback


def station_from_filename(filename: str):
    m = re.match(r"^([^_]+)_S_", filename)
    return m.group(1) if m else None


# ─── filesystem walk ──────────────────────────────────────────────────────────

def iter_recent_crxgz(pub_root: str, cutoff_utc: datetime):
    """Yield (fullpath, size) for every *.crx.gz modified after cutoff."""
    for root, _, files in os.walk(pub_root):
        for fn in files:
            if not fn.lower().endswith(".crx.gz"):
                continue
            full = os.path.join(root, fn)
            try:
                st = os.stat(full)
            except FileNotFoundError:
                continue
            mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)
            if mtime < cutoff_utc:
                continue
            yield full, st.st_size


# ─── CSV persistence ──────────────────────────────────────────────────────────

CSV_FIELDS = ["date", "doy_label", "product", "files", "stations", "bytes"]


def load_existing_daily(csv_path: str) -> dict:
    rows = {}
    if not os.path.exists(csv_path):
        return rows
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = (row["date"], row["product"])
            rows[key] = {
                "date":      row["date"],
                "doy_label": row.get("doy_label", row["date"]),
                "product":   row["product"],
                "files":     int(row["files"]),
                "stations":  int(row["stations"]),
                "bytes":     int(row["bytes"]),
            }
    return rows


def write_daily(csv_path: str, rows: dict):
    tmp = csv_path + ".tmp"
    items = sorted(rows.items(), key=lambda kv: (kv[0][0], kv[0][1]))
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for (_, _), row in items:
            w.writerow({k: row[k] for k in CSV_FIELDS})
    os.replace(tmp, csv_path)


# ─── windowed summaries ───────────────────────────────────────────────────────

def compute_window(rows: dict, since_iso: str, products=("1s", "30s", "unknown")):
    """Aggregate metrics for rows whose date >= since_iso."""
    result = {}
    for (date_key, prod), row in rows.items():
        if date_key < since_iso:
            continue
        if prod not in products:
            continue
        g = result.setdefault(prod, {"files": 0, "bytes": 0, "stations": 0, "days": 0})
        g["files"]    += row["files"]
        g["bytes"]    += row["bytes"]
        g["stations"] += row["stations"]   # sum of daily unique counts
        g["days"]     += 1
    return result


def last_n_days(rows: dict, n: int, products=("1s", "30s")):
    """Return sorted list of daily rows for the last n days, per product."""
    today = date.today()
    since = today - timedelta(days=n - 1)
    since_iso = since.isoformat()
    out = {}
    for (date_key, prod), row in rows.items():
        if date_key < since_iso or prod not in products:
            continue
        out.setdefault(prod, []).append({
            "date":     date_key,
            "files":    row["files"],
            "stations": row["stations"],
            "bytes":    row["bytes"],
        })
    for prod in out:
        out[prod].sort(key=lambda r: r["date"])
    return out


# ─── main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pub-root",   required=True)
    ap.add_argument("--stats-dir",  required=True)
    ap.add_argument("--scan-days",  type=int, default=60)
    args = ap.parse_args()

    pub_root  = os.path.abspath(args.pub_root)
    stats_dir = os.path.abspath(args.stats_dir)
    os.makedirs(stats_dir, exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    cutoff  = now_utc - timedelta(days=args.scan_days)

    print(f"[stat] pub_root={pub_root}  stats_dir={stats_dir}  scan_days={args.scan_days}")
    print(f"[stat] scanning files modified after {cutoff.isoformat()}")

    # ── scan filesystem ────────────────────────────────────────────────────────
    agg = {}    # (iso_date, product) -> {files, bytes, stations: set}
    n_scanned = 0

    for full, size in iter_recent_crxgz(pub_root, cutoff):
        n_scanned += 1
        yd = extract_year_doy(full)
        if yd is None:
            continue
        year, doy = yd
        iso_date  = doy_to_isodate_str(year, doy)
        doy_label = f"{year}-{doy:03d}"
        prod      = guess_product(full)
        key       = (iso_date, prod)

        if key not in agg:
            agg[key] = {"files": 0, "bytes": 0, "stations": set(), "doy_label": doy_label}
        agg[key]["files"]    += 1
        agg[key]["bytes"]    += size
        st = station_from_filename(os.path.basename(full))
        if st:
            agg[key]["stations"].add(st)

    print(f"[stat] scanned {n_scanned} .crx.gz files, {len(agg)} (date,product) buckets")

    # ── merge with existing CSV ────────────────────────────────────────────────
    daily_csv = os.path.join(stats_dir, "metrics_daily.csv")
    existing  = load_existing_daily(daily_csv)

    for (iso_date, prod), v in agg.items():
        existing[(iso_date, prod)] = {
            "date":      iso_date,
            "doy_label": v["doy_label"],
            "product":   prod,
            "files":     v["files"],
            "stations":  len(v["stations"]),
            "bytes":     v["bytes"],
        }

    write_daily(daily_csv, existing)

    # ── compute windows ────────────────────────────────────────────────────────
    today_iso = date.today().isoformat()
    d7_iso    = (date.today() - timedelta(days=6)).isoformat()   # today + 6 previous = 7 days
    d30_iso   = (date.today() - timedelta(days=29)).isoformat()  # 30 days

    windows = {
        "today": compute_window(existing, today_iso),
        "7d":    compute_window(existing, d7_iso),
        "30d":   compute_window(existing, d30_iso),
        "all":   compute_window(existing, "0000-00-00"),
    }

    # ── last 60 days timeseries (for chart) ───────────────────────────────────
    timeseries = last_n_days(existing, args.scan_days)

    # ── write JSON ────────────────────────────────────────────────────────────
    latest = {
        "generated_at_utc": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "scan_days":        args.scan_days,
        "pub_root":         pub_root,
        "files_scanned":    n_scanned,
        "windows":          windows,
        "timeseries":       timeseries,
    }

    out_json = os.path.join(stats_dir, "metrics_latest.json")
    tmp_json = out_json + ".tmp"
    with open(tmp_json, "w", encoding="utf-8") as f:
        json.dump(latest, f, indent=2, default=str)
    os.replace(tmp_json, out_json)

    print(f"[stat] wrote {daily_csv}")
    print(f"[stat] wrote {out_json}")
    for win, data in windows.items():
        for prod, v in data.items():
            print(f"[stat]   [{win}] {prod}: files={v['files']} bytes={v['bytes']} stations={v['stations']}")


if __name__ == "__main__":
    main()
