import os
import sys
import asyncio
import json
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

sys.path.insert(0, r"c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing")
import apps.profit_tracker.views as views

# Monkey-patch process_row to add debug logging
original_process_row = None

with engine.begin() as conn:
    ts_cols = [
        "use_sheet", "client_seconds", "employee_seconds",
        "client_min_bill", "employee_min_pay",
        "client_no_bill", "employee_no_pay",
        "client_no_break_penalty", "employee_no_break_penalty",
        "client_tips", "client_parking", "client_travel", "client_service_charge",
        "employee_tips", "employee_parking", "employee_travel", "employee_service_charge"
    ]
    ts_select_str = ", ".join(f"t.{col}" for col in ts_cols)
    
    sql = text(f"""
    SELECT
        e.date,
        sp.bonus,
        c.name AS client_name,
        c.client_id,
        se.shift_employee_id,
        se.bill_rate,
        se.rate AS pay_rate,
        se.overtime,
        v.service_charge AS venue_service_charge,
        wc.rate AS wc_rate,
        e.state AS event_state,
        t.client_worked,
        t.employee_worked,
        s.start AS shift_start,
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
      AND (
          (se.confirmed = 1 AND se.cancel_reason = 0)
          OR t.client_min_bill = 1
          OR t.employee_min_pay = 1
      )
    """)
    
    df = pd.read_sql(sql, conn, params={"start_date": "2026-04-13", "end_date": "2026-04-19"})
    asp_rules = [dict(r) for r in conn.execute(text("SELECT rate, start_date, end_date FROM additional_shift_pay")).mappings().fetchall()]

# Filter to Vibiana only
vibiana_df = df[df['client_name'] == 'Vibiana Events and Redbird Events'].copy()

# Fill numeric columns
numeric_cols = [
    "client_seconds", "client_tips", "client_parking", "client_travel", "client_service_charge", 
    "venue_service_charge", "client_no_break_penalty", "employee_no_break_penalty",
    "bill_rate", "pay_rate", "employee_tips", "employee_parking", "employee_travel", "employee_service_charge",
    "msp_rate", "wc_rate"
]
existing_numeric_cols = [col for col in numeric_cols if col in vibiana_df.columns]
if existing_numeric_cols:
    vibiana_df[existing_numeric_cols] = vibiana_df[existing_numeric_cols].fillna(0)

# Now run the actual views.py process_row for each Vibiana row
def debug_process_row(row):
    import json as _json
    use_sheet = str(row.get("use_sheet") or "").upper()
    c_sec = float(row["client_seconds"]) if pd.notna(row.get("client_seconds")) else 0.0
    e_sec = float(row["employee_seconds"]) if pd.notna(row.get("employee_seconds")) else 0.0
    
    uses_both_sheets = (use_sheet == "")
    if uses_both_sheets:
        bill_seconds = c_sec
        pay_seconds = e_sec
    elif use_sheet == "EMPLOYEE":
        bill_seconds = e_sec
        pay_seconds = e_sec
    else:  # CLIENT
        bill_seconds = c_sec
        pay_seconds = c_sec

    c_hours = bill_seconds / 3600.0
    e_hours = pay_seconds / 3600.0
    c_worked = str(row.get("client_worked") or "").upper()
    e_worked_raw = str(row.get("employee_worked") or "").upper()
    e_min = row.get("employee_min_pay")
    
    ot_json = row.get("overtime")
    custom_ot_pay_hours = None
    custom_dt_pay_hours = None
    if ot_json and pd.notna(ot_json) and str(ot_json).strip():
        try:
            ot_data = _json.loads(str(ot_json))
            for entry in ot_data.get("overtimes", {}).get("EMPLOYEE", []):
                if entry.get("type") == "ot" and custom_ot_pay_hours is None:
                    custom_ot_pay_hours = float(entry.get("hours", 0))
                elif entry.get("type") == "dt" and custom_dt_pay_hours is None:
                    custom_dt_pay_hours = float(entry.get("hours", 0))
        except Exception:
            pass
    
    e_worked = e_worked_raw
    
    if pd.notna(e_min) and float(e_min) > 0:
        e_pay_reg = 4.0
    else:
        e_pay_reg = e_hours
    
    e_non_worked = 0.0
    if e_worked in ("SENTHOME", "CANCELLED"):
        e_non_worked = max(e_pay_reg - e_hours, 0.0)
    e_worked_hours = e_pay_reg - e_non_worked
    
    e_ot = e_dt = 0.0
    if custom_ot_pay_hours is not None:
        e_ot = custom_ot_pay_hours or 0.0
        e_dt = custom_dt_pay_hours or 0.0
        e_reg = max(e_worked_hours - e_ot - e_dt, 0.0)
    else:
        state = str(row["event_state"]).upper() if row["event_state"] else ""
        if state in ("CA", "CALIFORNIA"):
            if e_worked_hours > 12: e_dt = e_worked_hours - 12; e_ot = 4.0; e_reg = 8.0
            elif e_worked_hours > 8: e_ot = e_worked_hours - 8; e_reg = 8.0
            else: e_reg = e_worked_hours
        else:
            e_reg = e_worked_hours
    
    pay_rate = float(row["pay_rate"])
    reg_pay = e_reg * pay_rate
    ot_pay = e_ot * pay_rate * 1.5
    dt_pay = e_dt * pay_rate * 2.0
    nw_pay = e_non_worked * pay_rate
    
    asp_pay = 0.0
    row_date = pd.to_datetime(row["date"]).date()
    if e_worked in ("WORKED", "SENTHOME"):
        for rule in asp_rules:
            r_start = pd.to_datetime(rule["start_date"]).date() if rule["start_date"] else None
            r_end = pd.to_datetime(rule["end_date"]).date() if rule["end_date"] else None
            if (r_start is None or r_start <= row_date) and (r_end is None or r_end >= row_date):
                asp_pay += float(rule["rate"])
    
    total_pay = reg_pay + ot_pay + dt_pay + nw_pay + asp_pay
    
    sid = row.get('shift_employee_id', 'N/A')
    print(f"SE {sid}: e_h={e_hours:.2f} e_reg={e_reg:.2f} e_ot={e_ot:.2f} e_nw={e_non_worked:.2f} rate={pay_rate} -> pay=${total_pay:.2f}")
    
    return pd.Series({"client_name": row["client_name"], "total_pay": total_pay, "total_bill": 0})

print(f"Processing {len(vibiana_df)} Vibiana rows...")
result = vibiana_df.apply(debug_process_row, axis=1)
print(f"\nTotal Vibiana Pay (debug): {result['total_pay'].sum():.2f}")
