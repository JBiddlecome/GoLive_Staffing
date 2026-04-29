import os
import pandas as pd
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

with engine.begin() as conn:
    sql = """
    SELECT se.shift_employee_id, e.date, se.rate AS pay_rate, se.bill_rate, se.overtime,
           e.state AS event_state,
           t.use_sheet, t.client_seconds, t.employee_seconds,
           t.client_worked, t.employee_worked,
           t.client_min_bill, t.employee_min_pay,
           t.client_start, t.employee_start,
           s.start as shift_start
    FROM shift_employee se
    JOIN event e ON se.event_id = e.event_id
    LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
    LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
    LEFT JOIN shift s ON sp.shift_id = s.shift_id
    WHERE e.date >= '2026-04-13' AND e.date <= '2026-04-19'
      AND e.client_id = 1785
      AND (
          (se.confirmed = 1 AND se.cancel_reason = 0)
          OR t.client_min_bill = 1
          OR t.employee_min_pay = 1
      )
    ORDER BY se.shift_employee_id
    """
    df = pd.read_sql(sql, conn)
    asp_rules = [dict(r) for r in conn.execute(text("SELECT rate, start_date, end_date FROM additional_shift_pay")).mappings().fetchall()]

# Check whether different rounding approaches affect the total
total_a = 0.0  # round at final step per shift
total_b = 0.0  # accumulate unrounded then round at end

for _, row in df.iterrows():
    sid = row['shift_employee_id']
    use_sheet = str(row.get("use_sheet") or "").upper()
    c_sec = float(row["client_seconds"]) if pd.notna(row.get("client_seconds")) else 0.0
    e_sec = float(row["employee_seconds"]) if pd.notna(row.get("employee_seconds")) else 0.0
    
    uses_both_sheets = (use_sheet == "")
    bill_seconds = e_sec if use_sheet == "EMPLOYEE" else c_sec if use_sheet == "CLIENT" else c_sec
    pay_seconds = e_sec if use_sheet == "EMPLOYEE" else c_sec if use_sheet == "CLIENT" else e_sec

    c_hours = bill_seconds / 3600.0
    e_hours = pay_seconds / 3600.0
    
    e_worked = str(row.get("employee_worked") or "").upper()
    c_min = row.get("client_min_bill")
    e_min = row.get("employee_min_pay")
    state = "CA"
    
    shift_start = pd.to_datetime(row.get("shift_start")) if pd.notna(row.get("shift_start")) else None
    c_late_hours = 0.0
    if shift_start and pd.notna(row.get("client_start")):
        c_actual = pd.to_datetime(row["client_start"])
        if c_actual > shift_start:
            c_late_hours = (c_actual - shift_start).total_seconds() / 3600.0
    e_late_hours = 0.0
    if shift_start and pd.notna(row.get("employee_start")):
        e_actual = pd.to_datetime(row["employee_start"])
        if e_actual > shift_start:
            e_late_hours = (e_actual - shift_start).total_seconds() / 3600.0
    late_hours = e_late_hours if use_sheet == "EMPLOYEE" else c_late_hours

    if pd.notna(e_min) and float(e_min) > 0:
        e_pay_reg = 4.0
        if late_hours > 0 and e_hours < e_pay_reg:
            e_pay_reg -= late_hours
        elif e_hours > e_pay_reg:
            e_pay_reg = e_hours
        e_pay_reg = max(e_pay_reg, 2.0)
        e_pay_reg = min(e_pay_reg, 4.0)
    else:
        e_pay_reg = e_hours

    e_non_worked = 0.0
    if e_worked in ("SENTHOME", "CANCELLED"):
        e_non_worked = max(e_pay_reg - e_hours, 0.0)
    e_worked_hours = e_pay_reg - e_non_worked

    e_ot = e_dt = 0.0
    if e_worked_hours > 12:
        e_dt = e_worked_hours - 12; e_ot = 4.0; e_reg = 8.0
    elif e_worked_hours > 8:
        e_ot = e_worked_hours - 8; e_reg = 8.0
    else:
        e_reg = e_worked_hours

    pay_rate = float(row["pay_rate"])
    
    # Method A: round each component first, then sum
    reg_pay_a = round(e_reg * pay_rate, 2)
    ot_pay_a = round(e_ot * pay_rate * 1.5, 2)
    dt_pay_a = round(e_dt * pay_rate * 2.0, 2)
    nw_pay_a = round(e_non_worked * pay_rate, 2)
    
    # ASP
    row_date = pd.to_datetime(row["date"]).date()
    asp_pay = 0.0
    if e_worked in ("WORKED", "SENTHOME"):
        for rule in asp_rules:
            r_start = pd.to_datetime(rule["start_date"]).date() if rule["start_date"] else None
            r_end = pd.to_datetime(rule["end_date"]).date() if rule["end_date"] else None
            if (r_start is None or r_start <= row_date) and (r_end is None or r_end >= row_date):
                asp_pay += float(rule["rate"])
    
    shift_pay_a = reg_pay_a + ot_pay_a + dt_pay_a + nw_pay_a + asp_pay
    total_a += shift_pay_a
    
    # Method B: compute unrounded then round total per shift
    reg_pay_b = e_reg * pay_rate
    ot_pay_b = e_ot * pay_rate * 1.5
    dt_pay_b = e_dt * pay_rate * 2.0
    nw_pay_b = e_non_worked * pay_rate
    shift_pay_b = round(reg_pay_b + ot_pay_b + dt_pay_b + nw_pay_b + asp_pay, 2)
    total_b += shift_pay_b
    
    print(f"SE {sid}: e_reg={e_reg:.4f} e_ot={e_ot:.4f} e_nw={e_non_worked:.4f} | "
          f"Method A=${shift_pay_a:.2f} Method B=${shift_pay_b:.2f}")

print(f"\nMethod A total: {total_a:.2f}")
print(f"Method B total: {total_b:.2f}")
print(f"Target: 2104.81")
