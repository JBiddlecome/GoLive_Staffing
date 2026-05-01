import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = (
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(url)

START_DATE = "2026-04-20"
END_DATE   = "2026-04-26"
CLIENT_ID  = 1083

# ── Pull raw data (same SQL as profit_tracker/views.py) ──────────────────────
sql = text("""
    SELECT
        se.shift_employee_id,
        e.date,
        e.client_id,
        CONCAT(emp.first_name, ' ', emp.last_name) AS employee_name,
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
        COALESCE(t.use_sheet, '')          AS use_sheet,
        COALESCE(t.client_seconds, 0)      AS client_seconds,
        COALESCE(t.employee_seconds, 0)    AS employee_seconds,
        COALESCE(t.client_min_bill, 0)     AS client_min_bill,
        COALESCE(t.employee_min_pay, 0)    AS employee_min_pay,
        COALESCE(t.client_no_bill, 0)      AS client_no_bill,
        COALESCE(t.employee_no_pay, 0)     AS employee_no_pay,
        COALESCE(t.client_no_break_penalty, 0)   AS client_no_break_penalty,
        COALESCE(t.employee_no_break_penalty, 0) AS employee_no_break_penalty,
        COALESCE(t.client_tips, 0)         AS client_tips,
        COALESCE(t.client_parking, 0)      AS client_parking,
        COALESCE(t.client_travel, 0)       AS client_travel,
        COALESCE(t.client_service_charge, 0) AS client_service_charge,
        COALESCE(t.employee_tips, 0)       AS employee_tips,
        COALESCE(t.employee_parking, 0)    AS employee_parking,
        COALESCE(t.employee_travel, 0)     AS employee_travel,
        COALESCE(t.employee_service_charge, 0) AS employee_service_charge
    FROM shift_employee se
    JOIN event e ON se.event_id = e.event_id
    JOIN client c ON e.client_id = c.client_id
    JOIN employee emp ON se.employee_id = emp.employee_id
    LEFT JOIN msp m ON c.msp_id = m.id
    LEFT JOIN wc_code wc ON c.wc_id = wc.wc_id
    LEFT JOIN venue v ON e.venue_id = v.venue_id
    LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
    LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
    LEFT JOIN shift s ON sp.shift_id = s.shift_id
    WHERE e.date >= :start_date AND e.date <= :end_date
      AND e.client_id = :client_id
      AND (
          (se.deleted_at IS NULL AND se.confirmed = 1 AND se.cancel_reason = 0)
          OR se.shift_employee_id IN (
              SELECT shift_employee_id FROM timesheet
              WHERE client_min_bill = 1 OR employee_min_pay = 1
          )
      )
    ORDER BY e.date, se.shift_employee_id
""")

with engine.begin() as conn:
    df = pd.read_sql(sql, conn, params={
        "start_date": START_DATE,
        "end_date": END_DATE,
        "client_id": CLIENT_ID,
    })
    asp_rules_raw = conn.execute(
        text("SELECT rate, start_date, end_date FROM additional_shift_pay")
    ).mappings().fetchall()
    asp_rules = [dict(r) for r in asp_rules_raw]

print(f"Rows returned: {len(df)}\n")

# ── Replicate process_row from profit_tracker/views.py ───────────────────────
def process_row(row):
    use_sheet = str(row.get("use_sheet") or "").upper()
    c_sec = float(row["client_seconds"])
    e_sec = float(row["employee_seconds"])

    uses_both_sheets = (use_sheet == "")
    if uses_both_sheets:
        bill_seconds = c_sec
        pay_seconds  = e_sec
    elif use_sheet == "EMPLOYEE":
        bill_seconds = e_sec
        pay_seconds  = e_sec
    else:  # CLIENT
        bill_seconds = c_sec
        pay_seconds  = c_sec

    c_hours = bill_seconds / 3600.0
    e_hours = pay_seconds  / 3600.0

    # Shift scheduling metrics
    shift_start_raw = row.get("shift_start")
    shift_end_raw   = row.get("shift_end")
    if pd.notna(shift_start_raw) and pd.notna(shift_end_raw):
        shift_dur_hours = (
            pd.to_datetime(shift_end_raw) - pd.to_datetime(shift_start_raw)
        ).total_seconds() / 3600.0
        meal_break_deduction  = 0.5 if shift_dur_hours > 5.0 else 0.0
        shift_work_hours      = shift_dur_hours - meal_break_deduction
        shift_senthome_min_hours = min(shift_work_hours / 2.0, 4.0)
        shift_min_bill_hours  = 4.0 if shift_dur_hours >= 4.0 else 2.0
    else:
        shift_dur_hours          = 4.0
        shift_work_hours         = 4.0
        shift_senthome_min_hours = 2.0
        shift_min_bill_hours     = 4.0

    e_worked_raw = str(row.get("employee_worked") or "").upper()
    c_min = row.get("client_min_bill")
    e_min = row.get("employee_min_pay")
    state = str(row["event_state"]).upper() if row["event_state"] else ""

    # Late hours
    shift_start = pd.to_datetime(shift_start_raw) if pd.notna(shift_start_raw) else None
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

    # ── CLIENT BILLING ────────────────────────────────────────────────────────
    if pd.notna(c_min) and float(c_min) > 0:
        c_bill_reg = shift_min_bill_hours
        if late_hours > 0 and c_hours < c_bill_reg:
            c_bill_reg -= late_hours
        elif c_hours > c_bill_reg:
            c_bill_reg = c_hours
        c_bill_reg = max(c_bill_reg, 2.0)
        c_bill_reg = min(c_bill_reg, shift_min_bill_hours)
    else:
        c_bill_reg = c_hours

    e_worked = e_worked_raw
    c_non_worked = 0.0
    if e_worked in ("SENTHOME", "CANCELLED"):
        c_non_worked = max(c_bill_reg - c_hours, 0.0)
    c_worked_hours = c_bill_reg - c_non_worked

    c_ot = c_dt = 0.0
    if c_worked_hours > 12:
        c_dt = c_worked_hours - 12; c_ot = 4.0; c_reg = 8.0
    elif c_worked_hours > 8:
        c_ot = c_worked_hours - 8;  c_reg = 8.0
    else:
        c_reg = c_worked_hours

    client_no = row.get("client_no_bill")
    if pd.notna(client_no) and float(client_no) > 0:
        c_reg = c_ot = c_dt = c_non_worked = 0.0

    # ── EMPLOYEE PAY ──────────────────────────────────────────────────────────
    is_senthome = e_worked == "SENTHOME"
    if is_senthome or (pd.notna(e_min) and float(e_min) > 0):
        # Match views.py: legacy PHP uses getMinBillingHours() (work_hours/2 capped at 4)
        # for ALL min-pay cases — both SENTHOME and employee_min_pay=1.
        e_pay_reg_floor = shift_senthome_min_hours
        e_pay_reg = e_pay_reg_floor
        if late_hours > 0 and e_hours < e_pay_reg:
            e_pay_reg -= late_hours
        elif e_hours > e_pay_reg:
            e_pay_reg = e_hours
        e_pay_reg = max(e_pay_reg, 2.0)
        e_pay_reg = min(e_pay_reg, e_pay_reg_floor)
    else:
        e_pay_reg = e_hours

    e_non_worked = 0.0
    if e_worked in ("SENTHOME", "CANCELLED"):
        e_non_worked = max(e_pay_reg - e_hours, 0.0)
    e_worked_hours = e_pay_reg - e_non_worked

    e_ot = e_dt = 0.0
    if state in ("CA", "CALIFORNIA"):
        if e_worked_hours > 12:
            e_dt = e_worked_hours - 12; e_ot = 4.0; e_reg = 8.0
        elif e_worked_hours > 8:
            e_ot = e_worked_hours - 8;  e_reg = 8.0
        else:
            e_reg = e_worked_hours
    elif state in ("NV", "NEVADA"):
        if e_worked_hours > 8:
            e_ot = e_worked_hours - 8;  e_reg = 8.0
        else:
            e_reg = e_worked_hours
    else:
        e_reg = e_worked_hours

    emp_no = row.get("employee_no_pay")
    if pd.notna(emp_no) and float(emp_no) > 0:
        e_reg = e_ot = e_dt = e_non_worked = 0.0

    bill_rate = float(row["bill_rate"])
    pay_rate  = float(row["pay_rate"])

    c_ot_rate = bill_rate * 1.5;  c_dt_rate = bill_rate * 2.0
    e_ot_rate = pay_rate  * 1.5;  e_dt_rate = pay_rate  * 2.0

    reg_pay        = e_reg        * pay_rate
    ot_pay         = e_ot         * e_ot_rate
    dt_pay         = e_dt         * e_dt_rate
    non_worked_pay = e_non_worked * pay_rate

    service_pct_e  = float(row.get("employee_service_charge") or 0)
    service_amt_e  = ((reg_pay + ot_pay + dt_pay) * service_pct_e / 100.0)
    meal_amt_e     = pay_rate if float(row.get("employee_no_break_penalty") or 0) > 0 else 0.0

    # Additional shift pay
    c_worked = str(row.get("client_worked") or "").upper()
    both_worked = (
        c_worked in ("WORKED", "SENTHOME") and e_worked in ("WORKED", "SENTHOME")
    )
    additional_pay = 0.0
    if both_worked and pd.notna(row.get("date")):
        row_date = pd.to_datetime(row["date"]).date()
        for rule in asp_rules:
            r_start = pd.to_datetime(rule["start_date"]).date() if rule["start_date"] else None
            r_end   = pd.to_datetime(rule["end_date"]).date()   if rule["end_date"]   else None
            if (r_start is None or r_start <= row_date) and (r_end is None or r_end >= row_date):
                additional_pay += float(rule["rate"])

    bonus_pay = float(row.get("bonus") or 0.0) if both_worked else 0.0

    if e_worked not in ("WORKED", "SENTHOME"):
        service_amt_e = 0.0; meal_amt_e = 0.0; additional_pay = 0.0; bonus_pay = 0.0
        e_tips = 0.0; e_parking = 0.0; e_travel = 0.0
    else:
        e_tips    = float(row.get("employee_tips",    0))
        e_parking = float(row.get("employee_parking", 0))
        e_travel  = float(row.get("employee_travel",  0))

    total_pay = (
        reg_pay + ot_pay + dt_pay + non_worked_pay
        + service_amt_e + meal_amt_e
        + e_tips + e_parking + e_travel
        + additional_pay + bonus_pay
    )

    return {
        "shift_employee_id": row["shift_employee_id"],
        "date":              row["date"],
        "employee_name":     row["employee_name"],
        "employee_worked":   e_worked_raw,
        "e_min_pay":         float(e_min) if pd.notna(e_min) else 0.0,
        "c_min_bill":        float(c_min) if pd.notna(c_min) else 0.0,
        "pay_rate":          pay_rate,
        "shift_dur_h":       round(shift_dur_hours, 4),
        "sh_work_h":         round(shift_work_hours, 4),
        "sh_sh_min_h":       round(shift_senthome_min_hours, 4),
        "e_hours_worked":    round(e_hours, 4),
        "e_reg_h":           round(e_reg, 4),
        "e_ot_h":            round(e_ot, 4),
        "e_non_worked_h":    round(e_non_worked, 4),
        "reg_pay":           round(reg_pay, 2),
        "ot_pay":            round(ot_pay, 2),
        "non_worked_pay":    round(non_worked_pay, 2),
        "additional_pay":    round(additional_pay, 2),
        "bonus_pay":         round(bonus_pay, 2),
        "total_pay":         round(total_pay, 2),
    }

results = [process_row(row) for _, row in df.iterrows()]

# ── Print results ─────────────────────────────────────────────────────────────
print(f"{'SE_ID':<8} {'Date':<12} {'Employee':<22} {'Worked':<12} "
      f"{'Rate':>6} {'Sched h':>7} {'Work h':>7} {'SH min h':>8} "
      f"{'e_hrs':>6} {'e_reg':>5} {'e_ot':>5} {'NW h':>6} "
      f"{'Reg $':>8} {'OT $':>6} {'NW $':>7} {'Addl':>5} {'TOTAL':>8}")
print("-" * 145)

total = 0.0
for r in results:
    print(
        f"{r['shift_employee_id']:<8} {str(r['date'])[:10]:<12} {r['employee_name']:<22} "
        f"{r['employee_worked']:<12} {r['pay_rate']:>6.2f} "
        f"{r['shift_dur_h']:>7.2f} {r['sh_work_h']:>7.2f} {r['sh_sh_min_h']:>8.2f} "
        f"{r['e_hours_worked']:>6.2f} {r['e_reg_h']:>5.2f} {r['e_ot_h']:>5.2f} "
        f"{r['e_non_worked_h']:>6.2f} "
        f"{r['reg_pay']:>8.2f} {r['ot_pay']:>6.2f} {r['non_worked_pay']:>7.2f} "
        f"{r['additional_pay']:>5.2f} {r['total_pay']:>8.2f}"
    )
    total += r["total_pay"]

print("-" * 145)
print(f"{'TOTAL GROSS PAY':>120} {total:>8.2f}")
print(f"\nNote: 'SH min h' = SENTHOME minimum hours (shift_work_hours / 2, capped at 4)")
print(f"      'NW h' = non-worked hours (pay floor minus actual hours worked)")
