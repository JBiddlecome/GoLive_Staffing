import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

# Check shift table columns
cols_q = "DESCRIBE `shift`"
df_cols = pd.read_sql(cols_q, engine)
print(df_cols[df_cols['Field'].str.contains('min|bill|hour', case=False)].to_string())

# Check late start data for 631951 and 632441
for sid in [631951, 632441]:
    q = f"""
    SELECT se.shift_employee_id, se.rate, se.bill_rate,
           t.use_sheet, t.client_worked, t.employee_worked,
           t.client_seconds, t.employee_seconds,
           t.client_min_bill, t.employee_min_pay,
           t.client_start, t.employee_start,
           s.start AS shift_start
    FROM shift_employee se
    LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
    LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
    LEFT JOIN shift s ON sp.shift_id = s.shift_id
    WHERE se.shift_employee_id = {sid}
    """
    df = pd.read_sql(q, engine)
    print(f"\n=== Shift {sid} ===")
    for col in df.columns:
        print(f"  {col}: {df.iloc[0][col]}")
    
    row = df.iloc[0]
    shift_start = pd.to_datetime(row["shift_start"])
    c_sec = float(row["client_seconds"]) if pd.notna(row.get("client_seconds")) else 0.0
    e_sec = float(row["employee_seconds"]) if pd.notna(row.get("employee_seconds")) else 0.0
    c_hours = c_sec / 3600.0
    e_hours = e_sec / 3600.0
    
    c_late = 0.0
    if pd.notna(row.get("client_start")):
        c_actual = pd.to_datetime(row["client_start"])
        if c_actual > shift_start:
            c_late = (c_actual - shift_start).total_seconds() / 3600.0
    
    e_late = 0.0
    if pd.notna(row.get("employee_start")):
        e_actual = pd.to_datetime(row["employee_start"])
        if e_actual > shift_start:
            e_late = (e_actual - shift_start).total_seconds() / 3600.0

    print(f"  client_hours={c_hours:.4f}, c_late={c_late:.4f}")
    print(f"  employee_hours={e_hours:.4f}, e_late={e_late:.4f}")
    
    # Legacy reCalculateRegularHours logic (uses getMinBillingHours = 4)
    min_hours = 4.0  # shift.getMinBillingHours() defaults to 4
    
    # CLIENT billing
    c_reg = min_hours
    if c_late > 0 and c_hours < c_reg:
        c_reg -= c_late
    elif c_hours > c_reg:
        c_reg = c_hours
    c_reg = max(c_reg, 2.0)
    c_reg = min(c_reg, 4.0)
    c_non_worked = max(c_reg - c_hours, 0.0)
    print(f"  => c_reg={c_reg:.4f}, c_non_worked={c_non_worked:.4f}")
    print(f"  => c billing: reg={c_reg - c_non_worked:.4f}h reg_pay={round((c_reg - c_non_worked)*31.5, 2)}, nw={c_non_worked:.4f}h nw_pay={round(c_non_worked*31.5, 2)}")
    
    # EMPLOYEE pay
    e_reg = min_hours
    if e_late > 0 and e_hours < e_reg:
        e_reg -= e_late
    elif e_hours > e_reg:
        e_reg = e_hours
    e_reg = max(e_reg, 2.0)
    e_reg = min(e_reg, 4.0)
    e_non_worked = max(e_reg - e_hours, 0.0)
    print(f"  => e_reg={e_reg:.4f}, e_non_worked={e_non_worked:.4f}")
    print(f"  => e pay: reg={e_reg - e_non_worked:.4f}h reg_pay={round((e_reg - e_non_worked)*21.0, 2)}, nw={e_non_worked:.4f}h nw_pay={round(e_non_worked*21.0, 2)}")
    print(f"  => TOTAL PAY for {sid}: {round((e_reg - e_non_worked)*21.0 + e_non_worked*21.0 + 1.0, 2)}")
