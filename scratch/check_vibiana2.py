import os
import pandas as pd
import json
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

with engine.begin() as conn:
    # Get column info
    ts_cols = [
        "use_sheet",
        "client_seconds", "employee_seconds",
        "client_min_bill", "employee_min_pay",
        "client_no_bill", "employee_no_pay",
        "client_no_break_penalty", "employee_no_break_penalty",
        "client_tips", "client_parking", "client_travel", "client_service_charge",
        "employee_tips", "employee_parking", "employee_travel", "employee_service_charge"
    ]
    ts_select_str = ", ".join(f"t.{col}" for col in ts_cols)
    
    sql = f"""
    SELECT
        e.date,
        sp.bonus,
        c.name AS client_name,
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
        {ts_select_str},
        se.shift_employee_id
    FROM shift_employee se
    JOIN event e ON se.event_id = e.event_id
    JOIN client c ON e.client_id = c.client_id
    LEFT JOIN msp m ON c.msp_id = m.id
    LEFT JOIN wc_code wc ON c.wc_id = wc.wc_id
    LEFT JOIN venue v ON e.venue_id = v.venue_id
    LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
    LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
    LEFT JOIN shift s ON sp.shift_id = s.shift_id
    WHERE e.date >= '2026-04-13' AND e.date <= '2026-04-19'
      AND e.client_id = 1131
      AND (
          (se.confirmed = 1 AND se.cancel_reason = 0)
          OR t.client_min_bill = 1
          OR t.employee_min_pay = 1
      )
    """
    df = pd.read_sql(sql, conn)
    asp_rules = [dict(r) for r in conn.execute(text("SELECT rate, start_date, end_date FROM additional_shift_pay")).mappings().fetchall()]

print(f"Total Vibiana rows: {len(df)}")
print(df[['shift_employee_id', 'use_sheet', 'client_seconds', 'employee_seconds', 'client_worked', 'employee_worked', 'pay_rate']].to_string())

# Now compute pay for each row
total_pay = 0.0
for _, row in df.iterrows():
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
    
    e_hours = pay_seconds / 3600.0
    e_worked = str(row.get("employee_worked") or "").upper()
    e_min = row.get("employee_min_pay")
    pay_rate = float(row["pay_rate"])
    
    if pd.notna(e_min) and float(e_min) > 0:
        e_pay_reg = 4.0
    else:
        e_pay_reg = e_hours
    
    e_non_worked = 0.0
    if e_worked in ("SENTHOME", "CANCELLED"):
        e_non_worked = max(e_pay_reg - e_hours, 0.0)
    e_worked_hours = e_pay_reg - e_non_worked
    
    e_reg = e_worked_hours  # no OT for short shifts
    
    # ASP
    asp_pay = 0.0
    row_date = pd.to_datetime(row["date"]).date()
    if e_worked in ("WORKED", "SENTHOME"):
        for rule in asp_rules:
            r_start = pd.to_datetime(rule["start_date"]).date() if rule["start_date"] else None
            r_end = pd.to_datetime(rule["end_date"]).date() if rule["end_date"] else None
            if (r_start is None or r_start <= row_date) and (r_end is None or r_end >= row_date):
                asp_pay += float(rule["rate"])
    
    shift_pay = e_reg * pay_rate + e_non_worked * pay_rate + asp_pay
    total_pay += shift_pay
    
    sid = row['shift_employee_id']
    print(f"SE {sid}: e_h={e_hours:.2f} e_reg={e_reg:.2f} e_nw={e_non_worked:.2f} pay_rate={pay_rate} -> pay=${shift_pay:.2f}")

print(f"\nTotal Vibiana Pay: {total_pay:.2f}")
print("Expected from old: ~2039.20")
