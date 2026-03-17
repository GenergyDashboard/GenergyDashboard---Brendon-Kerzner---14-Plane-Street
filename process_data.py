"""
14 Plane Street – Data Processor
Reads the VRM kWh XLSX exports and produces dashboard_data.json for the index.

Usage:
    python process_data.py --today downloads/14PlanestreetJBay_kwh_20260317.xlsx \
                           --lifetime data/Monthly_Data_Excluding_today.xlsx \
                           --output data/dashboard_data.json

The --lifetime file should contain ALL historical data EXCLUDING today,
so there is no double-counting.  Today's file comes from the scraper.

The XLSX layout (from VRM "Download kWh .xlsx"):
    Row 1: blank
    Row 2: headers  (timestamp, Solar Yield (delta), Grid to battery, Grid to consumers,
                     PV to battery, PV to grid, PV to consumers, Battery to consumers,
                     Battery to grid, Genset to consumers, Genset to battery, Gas)
    Row 3: units    (Africa/Johannesburg (+02:00), kWh, kWh, …)
    Row 4+: data    ("2026-03-17 00:00:00", value|None, …)
"""
import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

import openpyxl

# ── Column mapping ──────────────────────────────────────
# Columns are 0-indexed after timestamp
FIELDS = [
    "solar_yield",     # B – Solar Yield (delta)
    "grid_to_batt",    # C – Grid to battery
    "grid_to_cons",    # D – Grid to consumers
    "pv_to_batt",      # E – PV to battery
    "pv_to_grid",      # F – PV to grid
    "pv_to_cons",      # G – PV to consumers
    "batt_to_cons",    # H – Battery to consumers
    "batt_to_grid",    # I – Battery to grid
    "genset_to_cons",  # J – Genset to consumers
    "genset_to_batt",  # K – Genset to battery
]

# ── Environmental factors per kWh (PV generation) ───────
ENV = {"trees": 0.045, "homes": 0.102, "coal": 0.548, "water": 1.4}


def parse_xlsx(filepath):
    """Parse VRM kWh XLSX into list of row dicts."""
    wb = openpyxl.load_workbook(filepath, data_only=True)
    ws = wb.active
    rows = []
    for row in ws.iter_rows(min_row=4, max_row=ws.max_row, values_only=True):
        ts = str(row[0]) if row[0] else None
        if not ts or ts.strip() == "":
            continue
        entry = {"ts": ts}
        for i, field in enumerate(FIELDS):
            entry[field] = float(row[i + 1] or 0)
        rows.append(entry)
    return rows


def add_derived(d):
    """Add computed fields to a dict of summed FIELDS."""
    d["pv_total"] = d["pv_to_batt"] + d["pv_to_grid"] + d["pv_to_cons"]
    d["load"] = d["grid_to_cons"] + d["pv_to_cons"] + d["batt_to_cons"] + d["genset_to_cons"]
    d["grid_import"] = d["grid_to_cons"] + d["grid_to_batt"]
    d["export"] = d["pv_to_grid"] + d["batt_to_grid"]
    d["self_consumption"] = d["pv_to_cons"] + d["pv_to_batt"]
    return d


def aggregate(rows, key_fn):
    """Aggregate rows by a key function, summing all FIELDS."""
    agg = defaultdict(lambda: {k: 0.0 for k in FIELDS})
    counts = defaultdict(int)
    for r in rows:
        k = key_fn(r["ts"])
        for f in FIELDS:
            agg[k][f] += r[f]
        counts[k] += 1
    result = []
    for k in sorted(agg.keys()):
        entry = {"key": k}
        entry.update(agg[k])
        add_derived(entry)
        entry["count"] = counts[k]
        result.append(entry)
    return result


def hourly_key(ts):
    return ts[:13] + ":00:00"


def daily_key(ts):
    return ts[:10]


def monthly_key(ts):
    return ts[:7]


def round_dict(d, decimals=3):
    return {k: round(v, decimals) if isinstance(v, float) else v for k, v in d.items()}


def build_hourly_by_date(all_hourly):
    """Group hourly records by date for the daily-tab date picker."""
    by_date = defaultdict(list)
    output_fields = [
        "pv_total", "load", "grid_import", "export",
        "pv_to_cons", "pv_to_batt", "batt_to_cons", "grid_to_cons",
        "grid_to_batt", "batt_to_grid",
    ]
    for h in all_hourly:
        date = h["key"][:10]
        hour_num = int(h["key"][11:13])
        rec = {"h": hour_num}
        for f in output_fields:
            rec[f] = round(h.get(f, 0), 3)
        by_date[date].append(rec)
    return dict(by_date)


def calc_avg_hourly(hourly_by_date, month_str):
    """Calculate average hourly profile for a month (for chart overlays)."""
    sums = defaultdict(lambda: defaultdict(float))
    counts = defaultdict(int)
    for date, hours in hourly_by_date.items():
        if date[:7] != month_str:
            continue
        for h in hours:
            sums[h["h"]]["pv_total"] += h["pv_total"]
            sums[h["h"]]["load"] += h["load"]
            sums[h["h"]]["grid_import"] += h["grid_import"]
            counts[h["h"]] += 1
    avg = {}
    for hour in sorted(sums.keys()):
        c = counts[hour] or 1
        avg[hour] = {
            "pv_total": round(sums[hour]["pv_total"] / c, 3),
            "load": round(sums[hour]["load"] / c, 3),
            "grid_import": round(sums[hour]["grid_import"] / c, 3),
        }
    return avg


def main():
    parser = argparse.ArgumentParser(description="Process VRM kWh data for dashboard")
    parser.add_argument("--today", required=True, help="Path to today's XLSX")
    parser.add_argument("--lifetime", required=True, help="Path to lifetime XLSX (excluding today)")
    parser.add_argument("--output", default="data/dashboard_data.json", help="Output JSON path")
    args = parser.parse_args()

    print(f"Reading today:    {args.today}")
    today_raw = parse_xlsx(args.today)
    print(f"  → {len(today_raw)} rows")

    print(f"Reading lifetime: {args.lifetime}")
    lifetime_raw = parse_xlsx(args.lifetime)
    print(f"  → {len(lifetime_raw)} rows")

    all_raw = lifetime_raw + today_raw

    # ── Aggregate ──────────────────────────────────
    all_hourly = aggregate(all_raw, hourly_key)
    today_hourly = aggregate(today_raw, hourly_key)
    all_daily = aggregate(all_raw, daily_key)
    all_monthly = aggregate(all_raw, monthly_key)

    # ── Build hourly-by-date lookup ────────────────
    hourly_by_date = build_hourly_by_date(all_hourly)

    # ── Totals ─────────────────────────────────────
    totals = {f: 0.0 for f in FIELDS}
    for r in all_raw:
        for f in FIELDS:
            totals[f] += r[f]
    add_derived(totals)

    # ── Today date string ──────────────────────────
    today_date = today_raw[-1]["ts"][:10] if today_raw else datetime.now().strftime("%Y-%m-%d")

    # ── Compose output ─────────────────────────────
    output_fields = FIELDS + ["pv_total", "load", "grid_import", "export", "self_consumption"]

    data = {
        "site_name": "14 Plane Street",
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "today_date": today_date,
        "today_hourly": [
            {"h": int(h["key"][11:13]), **{f: round(h[f], 3) for f in output_fields}}
            for h in today_hourly
        ],
        "daily": [
            {"date": d["key"], **{f: round(d[f], 2) for f in output_fields}}
            for d in all_daily
        ],
        "monthly": [
            {"month": m["key"], "days": m["count"], **{f: round(m[f], 2) for f in output_fields}}
            for m in all_monthly
        ],
        "hourly_by_date": hourly_by_date,
        "totals": {f: round(totals[f], 2) for f in output_fields},
        "days_active": len(all_daily),
    }

    # ── Write JSON ─────────────────────────────────
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = os.path.getsize(args.output) / 1024
    print(f"\nWrote {args.output}  ({size_kb:.0f} KB)")
    print(f"  Days:   {len(all_daily)}")
    print(f"  Months: {len(all_monthly)}")
    print(f"  PV tot: {totals['pv_total']:.1f} kWh")
    print(f"  Load:   {totals['load']:.1f} kWh")


if __name__ == "__main__":
    main()
