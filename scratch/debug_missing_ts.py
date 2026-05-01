import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = (
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(url)

CLIENT_ID  = 1533
DATE       = "2026-04-23"
EMP_NAME   = "Adolphus Jenkins"

print(f"=== Diagnosing missing timesheet: {EMP_NAME}, client {CLIENT_ID}, {DATE} ===\n")

# 1. Find the shift_employee record — no filters, just find them
q1 = text("""
    SELECT
        se.shift_employee_id,
        se.confirmed,
        se.cancel_reason,
        se.deleted_at,
        CONCAT(emp.first_name, ' ', emp.last_name) AS employee_name,
        emp.employee_id,
        e.date,
        e.client_id,
        e.state
    FROM shift_employee se
    JOIN event e ON se.event_id = e.event_id
    JOIN employee emp ON se.employee_id = emp.employee_id
    WHERE e.date = :date
      AND e.client_id = :client_id
      AND (emp.first_name LIKE :fname OR emp.last_name LIKE :lname)
""")
fname = EMP_NAME.split()[0] + "%"
lname = EMP_NAME.split()[-1] + "%"

with engine.begin() as conn:
    df1 = pd.read_sql(q1, conn, params={
        "date": DATE, "client_id": CLIENT_ID,
        "fname": fname, "lname": lname
    })

print("── shift_employee record (no filters) ──────────────────────────────────")
if df1.empty:
    print("  !! No shift_employee record found for this employee/client/date at all.")
else:
    for _, r in df1.iterrows():
        print(f"  shift_employee_id : {r['shift_employee_id']}")
        print(f"  confirmed         : {r['confirmed']}")
        print(f"  cancel_reason     : {r['cancel_reason']}")
        print(f"  deleted_at        : {r['deleted_at']}")
        print(f"  event state       : {r['state']}")
        se_id = r['shift_employee_id']

# 2. Check the timesheet record
if not df1.empty:
    se_id = df1.iloc[0]['shift_employee_id']
    q2 = text("""
        SELECT
            t.shift_employee_id,
            t.employee_worked,
            t.client_worked,
            t.employee_min_pay,
            t.client_min_bill,
            t.employee_no_pay,
            t.client_no_bill,
            t.employee_seconds,
            t.client_seconds
        FROM timesheet t
        WHERE t.shift_employee_id = :se_id
    """)
    with engine.begin() as conn:
        df2 = pd.read_sql(q2, conn, params={"se_id": int(se_id)})

    print("\n── timesheet record ────────────────────────────────────────────────────")
    if df2.empty:
        print("  !! No timesheet row exists for this shift_employee_id.")
    else:
        for col in df2.columns:
            print(f"  {col:<25}: {df2.iloc[0][col]}")

    # 3. Explain exactly why the Profit Tracker filter excludes it
    print("\n── Why the Profit Tracker SQL excludes this row ────────────────────────")
    se = df1.iloc[0]
    confirmed     = int(se['confirmed'])
    cancel_reason = int(se['cancel_reason']) if pd.notna(se['cancel_reason']) else 0
    deleted_at    = se['deleted_at']

    if pd.notna(deleted_at):
        print(f"  EXCLUDED: se.deleted_at IS NOT NULL ({deleted_at})")
    else:
        print(f"  deleted_at: NULL ✓")

    passes_worked = (confirmed == 1 and cancel_reason == 0)
    print(f"  confirmed={confirmed}, cancel_reason={cancel_reason}  →  "
          f"'confirmed=1 AND cancel_reason=0' = {passes_worked}")

    if not df2.empty:
        emp_min = df2.iloc[0]['employee_min_pay']
        cli_min = df2.iloc[0]['client_min_bill']
        has_min = (pd.notna(emp_min) and int(emp_min) > 0) or \
                  (pd.notna(cli_min) and int(cli_min) > 0)
        print(f"  employee_min_pay={emp_min}, client_min_bill={cli_min}  →  "
              f"min-pay subquery match = {has_min}")

        if not passes_worked and not has_min:
            print("\n  RESULT: Row is EXCLUDED because cancel_reason != 0 AND neither")
            print("          employee_min_pay nor client_min_bill is set to 1.")
            print("  FIX NEEDED: The SQL filter must be extended to also include CANCELLED")
            print("          shifts that should receive min pay (like SENTHOME).")
    else:
        print("  No timesheet → min-pay subquery = False")
        if not passes_worked:
            print("\n  RESULT: Row is EXCLUDED — no timesheet at all and cancel_reason != 0.")
