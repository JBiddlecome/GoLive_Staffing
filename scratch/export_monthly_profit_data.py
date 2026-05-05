"""
Export Profit Tracker data for every month from Jan 2024 to May 2026.
Calls the same get_profit_data() logic the app uses, month by month.

Outputs two CSVs:
  1. monthly_summary.csv       – One row per month (totals)
  2. monthly_client_breakdown.csv – One row per client per month

Usage (from project root, .venv active):
  python scratch/export_monthly_profit_data.py
"""

import os
import sys
import json
import asyncio
from datetime import date
from calendar import monthrange

from dotenv import load_dotenv

# ── Load env vars ──────────────────────────────────────────────────────────────
load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

# ── Make sure the project root is importable ───────────────────────────────────
PROJECT_ROOT = r"c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing"
sys.path.insert(0, PROJECT_ROOT)

import apps.profit_tracker.views as views

ProfitPayload = views.ProfitPayload

# ── Output paths ───────────────────────────────────────────────────────────────
OUT_DIR = os.path.dirname(__file__)
SUMMARY_CSV = os.path.join(OUT_DIR, "monthly_summary.csv")
CLIENT_CSV  = os.path.join(OUT_DIR, "monthly_client_breakdown.csv")


def generate_month_ranges(start_year, start_month, end_year, end_month):
    """Yield (start_date, end_date, label) for each month in the range."""
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        first_day = date(y, m, 1)
        last_day  = date(y, m, monthrange(y, m)[1])
        label     = first_day.strftime("%Y-%m")
        yield first_day.isoformat(), last_day.isoformat(), label
        # Advance to next month
        m += 1
        if m > 12:
            m = 1
            y += 1


async def main():
    months = list(generate_month_ranges(2024, 1, 2026, 5))
    print(f"Will pull data for {len(months)} months: {months[0][2]} to {months[-1][2]}\n")

    summary_rows = []
    client_rows  = []

    for i, (start_dt, end_dt, label) in enumerate(months, 1):
        print(f"  [{i:2d}/{len(months)}] {label}  ({start_dt} to {end_dt}) ... ", end="", flush=True)

        payload = ProfitPayload(start_date=start_dt, end_date=end_dt)
        try:
            response = await views.get_profit_data(payload)
            data = json.loads(response.body)
        except Exception as exc:
            print(f"ERROR: {exc}")
            continue

        # ── Monthly summary row ────────────────────────────────────────────
        summary_rows.append({
            "month":        label,
            "start_date":   start_dt,
            "end_date":     end_dt,
            "total_bill":   data["total_bill"],
            "gross_pay":    data["gross_pay"],
            "msp_fee":      data["msp_fee"],
            "wc_fee":       data["wc_fee"],
            "payroll_tax":  data["payroll_tax"],
            "other_work":   data["other_work"],
            "profit":       data["profit"],
            "profit_pct":   round(data["profit"] / data["total_bill"] * 100, 2) if data["total_bill"] else 0,
        })

        # ── Per-client breakdown rows ──────────────────────────────────────
        for c in data.get("client_breakdown", []):
            client_rows.append({
                "month":        label,
                "start_date":   start_dt,
                "end_date":     end_dt,
                "client_name":  c["client_name"],
                "msp_name":     c.get("msp_name", ""),
                "total_bill":   c["total_bill"],
                "gross_pay":    c["gross_pay"],
                "msp_fee":      c["msp_fee"],
                "wc_fee":       c["wc_fee"],
                "payroll_tax":  c["payroll_tax"],
                "profit":       c["profit"],
                "profit_pct":   round(c["profit"] / c["total_bill"] * 100, 2) if c["total_bill"] else 0,
            })

        n_clients = len(data.get("client_breakdown", []))
        print(f"OK  (bill={data['total_bill']:>12,.2f}  profit={data['profit']:>12,.2f}  clients={n_clients})")

    # ── Write CSVs ─────────────────────────────────────────────────────────────
    import csv

    # Summary CSV
    if summary_rows:
        keys = summary_rows[0].keys()
        with open(SUMMARY_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(summary_rows)
        print(f"\n[OK] Monthly summary     -> {SUMMARY_CSV}  ({len(summary_rows)} rows)")

    # Client breakdown CSV
    if client_rows:
        keys = client_rows[0].keys()
        with open(CLIENT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(client_rows)
        print(f"[OK] Client breakdown    -> {CLIENT_CSV}  ({len(client_rows)} rows)")

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
