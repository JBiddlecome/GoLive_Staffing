"""
Debug script: Chef Gunnar Planter (client_id 1627) on 2026-03-05
Run with DB env vars set:
  $env:DB_HOST="<host>"; $env:DB_NAME="cstaffing_live"; $env:DB_USER="<user>"; $env:DB_PASSWORD="<pass>"; python scratch/debug_chef_gunnar.py

Usage:
  .venv\Scripts\python.exe scratch\debug_chef_gunnar.py
"""
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

def db_engine():
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = int(os.getenv("DB_PORT", "3306"))
    if not host or not user or not password:
        raise RuntimeError("Missing DB_HOST, DB_USER, or DB_PASSWORD env vars")
    url = URL.create(
        drivername="mysql+pymysql",
        username=user, password=password,
        host=host, port=port, database=name,
    )
    return create_engine(url, pool_pre_ping=True)

engine = db_engine()

# ─── STEP 1: ALL shift_employee rows for client 1627 on 2026-03-05 ────────────
print("=" * 80)
print("STEP 1: ALL rows for client 1627 on 2026-03-05 (no filter)")
print("=" * 80)
with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT
            se.shift_employee_id,
            se.confirmed,
            se.cancel_reason,
            se.deleted_at,
            t.timesheet_id,
            t.client_worked,
            t.employee_worked,
            t.client_min_bill,
            t.employee_min_pay,
            t.client_no_bill,
            t.employee_no_pay,
            t.client_seconds,
            t.employee_seconds,
            t.use_sheet,
            se.bill_rate,
            se.rate AS pay_rate,
            e.state AS event_state,
            sp.bonus,
            t.client_tips, t.client_parking, t.client_travel, t.client_service_charge,
            t.employee_tips, t.employee_parking, t.employee_travel, t.employee_service_charge
        FROM shift_employee se
        JOIN event e ON se.event_id = e.event_id
        LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
        LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
        WHERE e.client_id = 1627 AND e.date = '2026-03-05'
        ORDER BY se.shift_employee_id
    """)).fetchall()
    print(f"Total rows (ALL, no filter): {len(rows)}")
    for r in rows:
        d = dict(r._mapping)
        print(f"\n  shift_employee_id={d['shift_employee_id']}")
        print(f"    confirmed={d['confirmed']}, cancel_reason={d['cancel_reason']}, deleted_at={d['deleted_at']}")
        print(f"    timesheet_id={d['timesheet_id']}")
        print(f"    client_worked={d['client_worked']}, employee_worked={d['employee_worked']}")
        print(f"    client_min_bill={d['client_min_bill']}, employee_min_pay={d['employee_min_pay']}")
        print(f"    client_no_bill={d['client_no_bill']}, employee_no_pay={d['employee_no_pay']}")
        print(f"    client_seconds={d['client_seconds']}, employee_seconds={d['employee_seconds']}")
        print(f"    use_sheet={d['use_sheet']}, bill_rate={d['bill_rate']}, pay_rate={d['pay_rate']}")
        print(f"    bonus={d['bonus']}")
        print(f"    state={d['event_state']}")

print()

# ─── STEP 2: Profit Tracker filter ────────────────────────────────────────────
print("=" * 80)
print("STEP 2: Profit Tracker filter (LEFT JOIN, no deleted_at check)")
print("  (se.confirmed=1 AND se.cancel_reason=0) OR t.client_min_bill=1 OR t.employee_min_pay=1")
print("=" * 80)
with engine.connect() as conn:
    rows2 = conn.execute(text("""
        SELECT se.shift_employee_id, se.confirmed, se.cancel_reason, se.deleted_at,
               t.timesheet_id, t.client_worked, t.employee_worked,
               t.client_min_bill, t.employee_min_pay, t.client_seconds, t.employee_seconds,
               t.use_sheet, se.bill_rate, se.rate AS pay_rate
        FROM shift_employee se
        JOIN event e ON se.event_id = e.event_id
        LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
        WHERE e.client_id = 1627 AND e.date = '2026-03-05'
          AND (
              (se.confirmed = 1 AND se.cancel_reason = 0)
              OR t.client_min_bill = 1
              OR t.employee_min_pay = 1
          )
        ORDER BY se.shift_employee_id
    """)).fetchall()
    print(f"Profit Tracker filter → {len(rows2)} rows")
    for r in rows2:
        d = dict(r._mapping)
        print(f"  se_id={d['shift_employee_id']}, confirmed={d['confirmed']}, cancel={d['cancel_reason']}, deleted={d['deleted_at']}, "
              f"ts_id={d['timesheet_id']}, e_worked={d['employee_worked']}, c_worked={d['client_worked']}")

print()

# ─── STEP 3: GoLive andValidForPayroll filter ─────────────────────────────────
print("=" * 80)
print("STEP 3: GoLive andValidForPayroll filter (subquery for min_bill/min_pay)")
print("  (se.confirmed AND NOT se.cancel_reason)")
print("  OR se.shift_employee_id IN (SELECT ... WHERE client_min_bill OR employee_min_pay)")
print("=" * 80)
with engine.connect() as conn:
    rows3 = conn.execute(text("""
        SELECT se.shift_employee_id, se.confirmed, se.cancel_reason, se.deleted_at,
               t.timesheet_id, t.client_worked, t.employee_worked,
               t.client_min_bill, t.employee_min_pay, t.client_seconds, t.employee_seconds,
               t.use_sheet, se.bill_rate, se.rate AS pay_rate
        FROM shift_employee se
        JOIN event e ON se.event_id = e.event_id
        LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
        WHERE e.client_id = 1627 AND e.date = '2026-03-05'
          AND (
              (se.confirmed AND NOT se.cancel_reason)
              OR se.shift_employee_id IN (
                  SELECT shift_employee_id FROM timesheet
                  WHERE client_min_bill OR employee_min_pay
              )
          )
        ORDER BY se.shift_employee_id
    """)).fetchall()
    print(f"andValidForPayroll → {len(rows3)} rows")
    for r in rows3:
        d = dict(r._mapping)
        print(f"  se_id={d['shift_employee_id']}, confirmed={d['confirmed']}, cancel={d['cancel_reason']}, deleted={d['deleted_at']}, "
              f"ts_id={d['timesheet_id']}, e_worked={d['employee_worked']}, c_worked={d['client_worked']}")

print()

# ─── STEP 4: Per-row gross pay calculation for Profit Tracker rows ────────────
print("=" * 80)
print("STEP 4: Gross pay calc for Profit Tracker rows (simulate process_row)")
print("=" * 80)
with engine.connect() as conn:
    rows4 = conn.execute(text("""
        SELECT
            se.shift_employee_id,
            t.client_worked,
            t.employee_worked,
            t.client_min_bill,
            t.employee_min_pay,
            t.client_no_bill,
            t.employee_no_pay,
            t.client_seconds,
            t.employee_seconds,
            t.use_sheet,
            se.bill_rate,
            se.rate AS pay_rate,
            e.state AS event_state,
            sp.bonus
        FROM shift_employee se
        JOIN event e ON se.event_id = e.event_id
        LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
        LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
        WHERE e.client_id = 1627 AND e.date = '2026-03-05'
          AND (
              (se.confirmed = 1 AND se.cancel_reason = 0)
              OR t.client_min_bill = 1
              OR t.employee_min_pay = 1
          )
        ORDER BY se.shift_employee_id
    """)).fetchall()

    total_pay = 0.0
    for r in rows4:
        d = dict(r._mapping)
        
        use_sheet = str(d.get('use_sheet') or '').upper()
        c_sec = float(d['client_seconds'] or 0)
        e_sec = float(d['employee_seconds'] or 0)

        if use_sheet == '':
            bill_seconds = c_sec
            pay_seconds = e_sec
        elif use_sheet == 'EMPLOYEE':
            bill_seconds = e_sec
            pay_seconds = e_sec
        else:
            bill_seconds = c_sec
            pay_seconds = c_sec

        c_hours = bill_seconds / 3600.0
        e_hours = pay_seconds / 3600.0
        
        e_min = d.get('employee_min_pay')
        c_min = d.get('client_min_bill')
        e_worked = str(d.get('employee_worked') or '').upper()
        c_worked = str(d.get('client_worked') or '').upper()
        pay_rate = float(d['pay_rate'] or 0)
        bill_rate = float(d['bill_rate'] or 0)
        
        # Employee pay hours
        if e_min and float(e_min) > 0:
            e_pay_reg = 4.0  # hardcoded min in Profit Tracker
        else:
            e_pay_reg = e_hours

        if e_worked in ('SENTHOME', 'CANCELLED'):
            e_non_worked = max(e_pay_reg - e_hours, 0.0)
        else:
            e_non_worked = 0.0

        e_worked_hours = e_pay_reg - e_non_worked
        
        # OT/DT
        state = str(d.get('event_state') or '').upper()
        e_ot = e_dt = 0.0
        if state in ('CA', 'CALIFORNIA'):
            if e_worked_hours > 12:
                e_dt = e_worked_hours - 12
                e_ot = 4.0
                e_reg = 8.0
            elif e_worked_hours > 8:
                e_ot = e_worked_hours - 8
                e_reg = 8.0
            else:
                e_reg = e_worked_hours
        elif state in ('NV', 'NEVADA'):
            if e_worked_hours > 8:
                e_ot = e_worked_hours - 8
                e_reg = 8.0
            else:
                e_reg = e_worked_hours
        else:
            e_reg = e_worked_hours

        emp_no = d.get('employee_no_pay')
        if emp_no and float(emp_no) > 0:
            e_reg = e_ot = e_dt = e_non_worked = 0.0
        
        reg_pay = e_reg * pay_rate
        ot_pay = e_ot * (pay_rate * 1.5)
        dt_pay = e_dt * (pay_rate * 2.0)
        non_worked_pay = e_non_worked * pay_rate

        bonus_pay = float(d.get('bonus') or 0)
        if e_worked not in ('WORKED', 'SENTHOME'):
            bonus_pay = 0.0

        row_total = reg_pay + ot_pay + dt_pay + non_worked_pay + bonus_pay
        total_pay += row_total

        print(f"\n  se_id={d['shift_employee_id']}, e_worked={e_worked}, c_worked={c_worked}")
        print(f"    use_sheet={use_sheet}, e_hours={e_hours:.4f}, e_min={e_min}")
        print(f"    e_pay_reg={e_pay_reg:.4f}, e_non_worked={e_non_worked:.4f}, e_worked_hours={e_worked_hours:.4f}")
        print(f"    e_reg={e_reg:.4f}, e_ot={e_ot:.4f}, e_dt={e_dt:.4f}")
        print(f"    pay_rate={pay_rate}")
        print(f"    reg_pay={reg_pay:.2f}, ot_pay={ot_pay:.2f}, non_worked_pay={non_worked_pay:.2f}, bonus={bonus_pay:.2f}")
        print(f"    >>> row_total = {row_total:.2f}")

    print(f"\n  TOTAL GROSS PAY (Profit Tracker) = {total_pay:.2f}")
    print(f"  EXPECTED (GoLive)               = 211.25")
    print(f"  DIFFERENCE                      = {total_pay - 211.25:.2f}")

print("\nDone.")
