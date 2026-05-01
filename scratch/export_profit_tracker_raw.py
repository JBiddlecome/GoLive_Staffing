"""
Export raw Profit Tracker query data to CSV.
Usage: run from the project root with the .venv active.
  python scratch/export_profit_tracker_raw.py
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

# ── Load env vars ──────────────────────────────────────────────────────────────
load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

START_DATE = "2026-04-20"
END_DATE   = "2026-04-26"
OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "profit_tracker_raw_2026-04-20_to_2026-04-26.csv")
OUTPUT_ASP = os.path.join(os.path.dirname(__file__), "profit_tracker_asp_rules_2026-04-20_to_2026-04-26.csv")
OUTPUT_OW  = os.path.join(os.path.dirname(__file__), "profit_tracker_other_work_2026-04-20_to_2026-04-26.csv")

# ── DB connection ──────────────────────────────────────────────────────────────
def build_engine():
    host     = os.getenv("DB_HOST")
    name     = os.getenv("DB_NAME", "cstaffing_live")
    user     = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port     = int(os.getenv("DB_PORT", "3306"))

    missing = [k for k, v in [("DB_HOST", host), ("DB_USER", user), ("DB_PASSWORD", password)] if not v]
    if missing:
        print(f"[ERROR] Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    url = URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name,
    )
    return create_engine(url, pool_pre_ping=True)


def main():
    engine = build_engine()
    params = {"start_date": START_DATE, "end_date": END_DATE}

    with engine.begin() as conn:
        # ── Dynamically check which timesheet columns exist ────────────────────
        inspector = inspect(engine)
        timesheet_columns = {col["name"] for col in inspector.get_columns("timesheet")}

        ts_cols = [
            "use_sheet",
            "client_seconds", "employee_seconds",
            "client_min_bill", "employee_min_pay",
            "client_no_bill", "employee_no_pay",
            "client_no_break_penalty", "employee_no_break_penalty",
            "client_tips", "client_parking", "client_travel", "client_service_charge",
            "employee_tips", "employee_parking", "employee_travel", "employee_service_charge",
        ]
        ts_select_str = ", ".join(
            f"t.{col}" if col in timesheet_columns else f"0 AS {col}"
            for col in ts_cols
        )

        # ── Main query (identical to profit_tracker/views.py) ─────────────────
        sql = text(
            f"""
            SELECT
                e.date,
                sp.bonus,
                c.name AS client_name,
                se.bill_rate,
                se.rate AS pay_rate,
                v.service_charge AS venue_service_charge,
                m.rate AS msp_rate,
                wc.rate AS wc_rate,
                e.state AS event_state,
                t.client_worked,
                t.employee_worked,
                s.start AS shift_start,
                s.end AS shift_end,
                t.client_start,
                t.employee_start,
                {ts_select_str}
            FROM shift_employee se
            JOIN event e ON se.event_id = e.event_id
            JOIN client c ON e.client_id = c.client_id
            LEFT JOIN msp m ON c.msp_id = m.id
            LEFT JOIN wc_code wc ON c.wc_id = wc.wc_id
            LEFT JOIN venue v ON e.venue_id = v.venue_id
            LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
            LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            LEFT JOIN shift s ON sp.shift_id = s.shift_id
            WHERE e.date >= :start_date AND e.date <= :end_date
              AND se.deleted_at IS NULL
              AND (
                  (se.confirmed = 1 AND se.cancel_reason = 0)
                  OR se.shift_employee_id IN (
                      SELECT shift_employee_id FROM timesheet
                      WHERE client_min_bill = 1 OR employee_min_pay = 1
                  )
              )
            ORDER BY c.name, e.date
            """
        )
        df = pd.read_sql(sql, conn, params=params)

        # ── Additional shift pay rules ─────────────────────────────────────────
        try:
            asp_df = pd.read_sql(
                text("SELECT rate, start_date, end_date FROM additional_shift_pay"),
                conn
            )
        except Exception as e:
            print(f"[WARN] Could not fetch additional_shift_pay: {e}")
            asp_df = pd.DataFrame()

        # ── Employee other work for date range ─────────────────────────────────
        ow_sql = text(
            """
            SELECT date, SUM(cost) AS total_other_work
            FROM employee_other_work
            WHERE date >= :start_date AND date <= :end_date
            GROUP BY date
            ORDER BY date
            """
        )
        ow_df = pd.read_sql(ow_sql, conn, params=params)

    engine.dispose()

    # ── Write outputs ──────────────────────────────────────────────────────────
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"[OK] Main query  -> {OUTPUT_CSV}  ({len(df)} rows)")

    if not asp_df.empty:
        asp_df.to_csv(OUTPUT_ASP, index=False)
        print(f"[OK] ASP rules   -> {OUTPUT_ASP}  ({len(asp_df)} rows)")
    else:
        print("[SKIP] No additional_shift_pay rows.")

    if not ow_df.empty:
        ow_df.to_csv(OUTPUT_OW, index=False)
        print(f"[OK] Other work  -> {OUTPUT_OW}  ({len(ow_df)} rows)")
    else:
        print("[SKIP] No employee_other_work rows for this range.")


if __name__ == "__main__":
    main()
