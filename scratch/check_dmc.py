import os
import sys
import asyncio
import json
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

# Expected values from legacy report for DMC:
# SE 631951: pay=69.95, bill=103.45 (2.1h * 31.5 + 1.1833*31.5=37.28)
# SE 632441: pay=83.60, bill=123.90

# Let's compute what views.py now calculates for each shift
with engine.begin() as conn:
    sql = """
    SELECT
        se.shift_employee_id,
        e.date,
        se.bill_rate,
        se.rate AS pay_rate,
        se.overtime,
        s.start AS shift_start,
        t.use_sheet,
        t.client_seconds,
        t.employee_seconds,
        t.client_worked,
        t.employee_worked,
        t.client_start,
        t.employee_start,
        t.client_min_bill,
        t.employee_min_pay,
        t.client_no_bill,
        t.employee_no_pay,
        t.client_no_break_penalty,
        t.employee_no_break_penalty,
        t.client_tips,
        t.client_parking,
        t.client_travel,
        t.client_service_charge,
        t.employee_tips,
        t.employee_parking,
        t.employee_travel,
        t.employee_service_charge,
        sp.bonus,
        e.state AS event_state
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
    """
    df = pd.read_sql(sql, conn)
    asp_rules_raw = conn.execute(text("SELECT rate, start_date, end_date FROM additional_shift_pay")).mappings().fetchall()
    asp_rules = [dict(r) for r in asp_rules_raw]

print(f"Total shifts: {len(df)}")

total_pay = 0.0
total_bill = 0.0

# Replicate exact logic from views.py process_row
for _, row in df.iterrows():
    sid = row['shift_employee_id']
    use_sheet = str(row.get("use_sheet") or "").upper()
    c_sec = float(row["client_seconds"]) if pd.notna(row.get("client_seconds")) else 0.0
    e_sec = float(row["employee_seconds"]) if pd.notna(row.get("employee_seconds")) else 0.0
    
    # getUsesBothSheets = (use_sheet IS NULL)
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
    e_worked = str(row.get("employee_worked") or "").upper()
    c_min = row.get("client_min_bill")
    e_min = row.get("employee_min_pay")
    state = str(row["event_state"]).upper() if row["event_state"] else ""

    # Parse overtime JSON
    custom_ot_bill_rate = None; custom_dt_bill_rate = None
    custom_ot_bill_hours = None; custom_dt_bill_hours = None
    custom_ot_pay_rate = None;  custom_dt_pay_rate = None
    custom_ot_pay_hours = None; custom_dt_pay_hours = None
    ot_paid_by_client = False
    ot_json = row.get("overtime")
    if ot_json and pd.notna(ot_json) and str(ot_json).strip():
        try:
            ot_data = json.loads(str(ot_json))
            ot_paid_by_client = (ot_data.get("paidBy", "") == "CLIENT")
            for entry in ot_data.get("overtimes", {}).get("CLIENT", []):
                if entry.get("type") == "ot" and custom_ot_bill_rate is None:
                    custom_ot_bill_rate = float(entry.get("rate", 0))
                    custom_ot_bill_hours = float(entry.get("hours", 0))
                elif entry.get("type") == "dt" and custom_dt_bill_rate is None:
                    custom_dt_bill_rate = float(entry.get("rate", 0))
                    custom_dt_bill_hours = float(entry.get("hours", 0))
            for entry in ot_data.get("overtimes", {}).get("EMPLOYEE", []):
                if entry.get("type") == "ot" and custom_ot_pay_rate is None:
                    custom_ot_pay_rate = float(entry.get("rate", 0))
                    custom_ot_pay_hours = float(entry.get("hours", 0))
                elif entry.get("type") == "dt" and custom_dt_pay_rate is None:
                    custom_dt_pay_rate = float(entry.get("rate", 0))
                    custom_dt_pay_hours = float(entry.get("hours", 0))
        except Exception as ex:
            pass

    # Late hours
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

    # CLIENT BILLING
    if pd.notna(c_min) and float(c_min) > 0:
        c_bill_reg = 4.0
        if late_hours > 0 and c_hours < c_bill_reg:
            c_bill_reg -= late_hours
        elif c_hours > c_bill_reg:
            c_bill_reg = c_hours
        c_bill_reg = max(c_bill_reg, 2.0)
        c_bill_reg = min(c_bill_reg, 4.0)
    else:
        c_bill_reg = c_hours

    c_non_worked = 0.0
    if e_worked in ("SENTHOME", "CANCELLED"):
        c_non_worked = max(c_bill_reg - c_hours, 0.0)
    c_worked_hours = c_bill_reg - c_non_worked

    c_ot = c_dt = 0.0
    if ot_paid_by_client and custom_ot_bill_hours is not None:
        c_ot = custom_ot_bill_hours or 0.0
        c_dt = custom_dt_bill_hours or 0.0
        c_reg = max(c_worked_hours - c_ot - c_dt, 0.0)
    else:
        if c_worked_hours > 12:
            c_dt = c_worked_hours - 12; c_ot = 4.0; c_reg = 8.0
        elif c_worked_hours > 8:
            c_ot = c_worked_hours - 8; c_reg = 8.0
        else:
            c_reg = c_worked_hours

    client_no = row.get("client_no_bill")
    if pd.notna(client_no) and float(client_no) > 0:
        c_reg = c_ot = c_dt = c_non_worked = 0.0

    # EMPLOYEE PAY
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
    if custom_ot_pay_hours is not None:
        e_ot = custom_ot_pay_hours or 0.0
        e_dt = custom_dt_pay_hours or 0.0
        e_reg = max(e_worked_hours - e_ot - e_dt, 0.0)
    else:
        if state in ("CA", "CALIFORNIA"):
            if e_worked_hours > 12: e_dt = e_worked_hours - 12; e_ot = 4.0; e_reg = 8.0
            elif e_worked_hours > 8: e_ot = e_worked_hours - 8; e_reg = 8.0
            else: e_reg = e_worked_hours
        elif state in ("NV", "NEVADA"):
            if e_worked_hours > 8: e_ot = e_worked_hours - 8; e_reg = 8.0
            else: e_reg = e_worked_hours
        else:
            e_reg = e_worked_hours

    emp_no = row.get("employee_no_pay")
    if pd.notna(emp_no) and float(emp_no) > 0:
        e_reg = e_ot = e_dt = e_non_worked = 0.0

    bill_rate = float(row["bill_rate"])
    pay_rate = float(row["pay_rate"])
    c_ot_rate = custom_ot_bill_rate if custom_ot_bill_rate is not None else (bill_rate * 1.5)
    c_dt_rate = custom_dt_bill_rate if custom_dt_bill_rate is not None else (bill_rate * 2.0)
    e_ot_rate = pay_rate * 1.5  # always standard (JSON rate is weekly avg)
    e_dt_rate = pay_rate * 2.0

    reg_bill = round(c_reg * bill_rate, 2)
    ot_bill = round(c_ot * c_ot_rate, 2)
    dt_bill = round(c_dt * c_dt_rate, 2)
    nw_bill = round(c_non_worked * bill_rate, 2)
    
    reg_pay = round(e_reg * pay_rate, 2)
    ot_pay = round(e_ot * e_ot_rate, 2)
    dt_pay = round(e_dt * e_dt_rate, 2)
    nw_pay = round(e_non_worked * pay_rate, 2)

    # ASP
    asp_pay = 0.0
    row_date = pd.to_datetime(row["date"]).date()
    if e_worked in ("WORKED", "SENTHOME") and c_worked in ("WORKED", "SENTHOME"):
        for rule in asp_rules:
            r_start = pd.to_datetime(rule["start_date"]).date() if rule["start_date"] else None
            r_end = pd.to_datetime(rule["end_date"]).date() if rule["end_date"] else None
            if (r_start is None or r_start <= row_date) and (r_end is None or r_end >= row_date):
                asp_pay += float(rule["rate"])
    
    bonus_pay = 0.0  # no bonus for DMC

    shift_pay = reg_pay + ot_pay + dt_pay + nw_pay + asp_pay + bonus_pay
    shift_bill = reg_bill + ot_bill + dt_bill + nw_bill
    
    total_pay += shift_pay
    total_bill += shift_bill

    # Show details
    print(f"SE {sid} [{use_sheet or 'NULL'}]: c_h={c_hours:.3f} e_h={e_hours:.3f} late={late_hours:.3f} | "
          f"c_reg={c_reg:.3f} c_ot={c_ot:.3f} c_nw={c_non_worked:.3f} | "
          f"e_reg={e_reg:.3f} e_ot={e_ot:.3f} e_nw={e_non_worked:.3f} | "
          f"pay=${shift_pay:.2f} bill=${shift_bill:.2f}")

print(f"\nTotal Pay: {total_pay:.2f}")
print(f"Total Bill: {total_bill:.2f}")
print(f"\nTarget: Pay=2104.81, Bill=3150.00")
print(f"Delta:  Pay={total_pay - 2104.81:.2f}, Bill={total_bill - 3150.00:.2f}")
