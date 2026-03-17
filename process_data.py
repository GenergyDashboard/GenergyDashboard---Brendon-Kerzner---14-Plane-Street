"""
14 Plane Street – Data Processor
Reads the VRM kWh XLSX exports and produces dashboard_data.json for the index.

Also handles lifetime data accumulation: if --append-yesterday is provided,
yesterday's XLSX is appended into the lifetime file before processing,
so the lifetime file self-maintains over time.

Usage:
    python process_data.py --today downloads/14PlanestreetJBay_kwh_20260317.xlsx \
                           --lifetime data/Monthly_Data_Excluding_today.xlsx \
                           --output data/dashboard_data.json

    # With yesterday append (run by GitHub Actions):
    python process_data.py --today downloads/... --lifetime data/... --output data/... \
                           --append-yesterday downloads/14PlanestreetJBay_kwh_20260316.xlsx

The XLSX layout (from VRM "Download kWh .xlsx"):
    Row 1: blank
    Row 2: headers
    Row 3: units
    Row 4+: data  ("2026-03-17 00:00:00", value|None, …)
"""
import argparse
import json
import os
from collections import defaultdict
from datetime import datetime

import openpyxl

# ── Column mapping (0-indexed after timestamp) ──────────
FIELDS = [
    "solar_yield",     # B
    "grid_to_batt",    # C
    "grid_to_cons",    # D
    "pv_to_batt",      # E
    "pv_to_grid",      # F
    "pv_to_cons",      # G
    "batt_to_cons",    # H
    "batt_to_grid",    # I
    "genset_to_cons",  # J
    "genset_to_batt",  # K
]

VRM_HEADERS = [
    'timestamp', 'Solar Yield (delta)', 'Grid to battery',
    'Grid to consumers', 'PV to battery', 'PV to grid',
    'PV to consumers', 'Battery to consumers', 'Battery to grid',
    'Genset to consumers', 'Genset to battery', 'Gas'
]
VRM_UNITS = [
    'Africa/Johannesburg (+02:00)', 'kWh', 'kWh', 'kWh', 'kWh',
    'kWh', 'kWh', 'kWh', 'kWh', 'kWh', 'kWh', 'm3'
]


# ══════════════════════════════════════════════════
# LIFETIME XLSX ACCUMULATION
# ══════════════════════════════════════════════════

def append_to_lifetime(daily_xlsx, lifetime_xlsx):
    """
    Append all rows from daily_xlsx into lifetime_xlsx (idempotent).
    Creates the lifetime file with proper VRM headers if it doesn't exist.
    """
    if not os.path.exists(daily_xlsx):
        print(f"  [append] File not found, skipping: {daily_xlsx}")
        return

    if os.path.exists(lifetime_xlsx):
        wb = openpyxl.load_workbook(lifetime_xlsx)
        ws = wb.active
    else:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "VRM kWh report"
        ws.append([None] * 12)
        ws.append(VRM_HEADERS)
        ws.append(VRM_UNITS)

    existing = set()
    for row in ws.iter_rows(min_row=4, max_row=ws.max_row, values_only=True):
        ts = str(row[0]).strip() if row[0] else None
        if ts:
            existing.add(ts)

    wb_d = openpyxl.load_workbook(daily_xlsx, data_only=True)
    ws_d = wb_d.active
    added = 0
    for row in ws_d.iter_rows(min_row=4, max_row=ws_d.max_row, values_only=True):
        ts = str(row[0]).strip() if row[0] else None
        if not ts or ts in existing:
            continue
        ws.append(list(row))
        added += 1

    wb.save(lifetime_xlsx)
    print(f"  [append] +{added} rows (was {len(existing)}, now {len(existing)+added})")


# ══════════════════════════════════════════════════
# XLSX PARSING & AGGREGATION
# ══════════════════════════════════════════════════

def parse_xlsx(filepath):
    wb = openpyxl.load_workbook(filepath, data_only=True)
    ws = wb.active
    rows = []
    for row in ws.iter_rows(min_row=4, max_row=ws.max_row, values_only=True):
        ts = str(row[0]).strip() if row[0] else None
        if not ts:
            continue
        entry = {"ts": ts}
        for i, field in enumerate(FIELDS):
            entry[field] = float(row[i + 1] or 0)
        rows.append(entry)
    return rows


def add_derived(d):
    d["pv_total"] = d["pv_to_batt"] + d["pv_to_grid"] + d["pv_to_cons"]
    d["load"] = d["grid_to_cons"] + d["pv_to_cons"] + d["batt_to_cons"] + d["genset_to_cons"]
    d["grid_import"] = d["grid_to_cons"] + d["grid_to_batt"]
    d["export"] = d["pv_to_grid"] + d["batt_to_grid"]
    d["self_consumption"] = d["pv_to_cons"] + d["pv_to_batt"]
    return d


def aggregate(rows, key_fn):
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


def hourly_key(ts):  return ts[:13] + ":00:00"
def daily_key(ts):   return ts[:10]
def monthly_key(ts): return ts[:7]


def build_hourly_by_date(all_hourly):
    by_date = defaultdict(list)
    fields = ["pv_total","load","grid_import","export","pv_to_cons","pv_to_batt",
              "batt_to_cons","grid_to_cons","grid_to_batt","batt_to_grid"]
    for h in all_hourly:
        rec = {"h": int(h["key"][11:13])}
        for f in fields:
            rec[f] = round(h.get(f, 0), 3)
        by_date[h["key"][:10]].append(rec)
    return dict(by_date)


# ══════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description="Process VRM kWh data for dashboard")
    p.add_argument("--today", required=True, help="Path to today's XLSX")
    p.add_argument("--lifetime", required=True, help="Path to lifetime XLSX (excluding today)")
    p.add_argument("--output", default="data/dashboard_data.json", help="Output JSON path")
    p.add_argument("--append-yesterday", default=None,
                   help="Path to yesterday's XLSX — appended into lifetime before processing")
    args = p.parse_args()

    # Step 1: Append yesterday into lifetime (if provided)
    if args.append_yesterday:
        print(f"Appending yesterday: {args.append_yesterday}")
        append_to_lifetime(args.append_yesterday, args.lifetime)

    # Step 2: Parse
    print(f"Reading today:    {args.today}")
    today_raw = parse_xlsx(args.today)
    print(f"  → {len(today_raw)} rows")

    print(f"Reading lifetime: {args.lifetime}")
    lifetime_raw = parse_xlsx(args.lifetime)
    print(f"  → {len(lifetime_raw)} rows")

    all_raw = lifetime_raw + today_raw

    # Step 3: Aggregate
    all_hourly  = aggregate(all_raw, hourly_key)
    today_hourly = aggregate(today_raw, hourly_key)
    all_daily   = aggregate(all_raw, daily_key)
    all_monthly = aggregate(all_raw, monthly_key)
    hourly_by_date = build_hourly_by_date(all_hourly)

    # Step 4: Totals
    totals = {f: 0.0 for f in FIELDS}
    for r in all_raw:
        for f in FIELDS:
            totals[f] += r[f]
    add_derived(totals)

    today_date = today_raw[-1]["ts"][:10] if today_raw else datetime.now().strftime("%Y-%m-%d")
    out_fields = FIELDS + ["pv_total","load","grid_import","export","self_consumption"]

    # Step 5: Compose JSON
    data = {
        "site_name": "14 Plane Street",
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "today_date": today_date,
        "today_hourly": [
            {"h": int(h["key"][11:13]), **{f: round(h[f], 3) for f in out_fields}}
            for h in today_hourly
        ],
        "daily": [
            {"date": d["key"], **{f: round(d[f], 2) for f in out_fields}}
            for d in all_daily
        ],
        "monthly": [
            {"month": m["key"], "days": m["count"], **{f: round(m[f], 2) for f in out_fields}}
            for m in all_monthly
        ],
        "hourly_by_date": hourly_by_date,
        "totals": {f: round(totals[f], 2) for f in out_fields},
        "days_active": len(all_daily),
    }

    # Step 6: Write
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
