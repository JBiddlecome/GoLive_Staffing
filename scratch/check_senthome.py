import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

# All DMC shifts and their worked statuses 
q = """
SELECT se.shift_employee_id, e.state, se.rate, se.bill_rate,
       t.use_sheet, t.client_worked, t.employee_worked,
       t.client_seconds, t.employee_seconds,
       t.client_min_bill, t.employee_min_pay,
       t.client_no_bill, t.employee_no_pay
FROM shift_employee se
JOIN event e ON se.event_id = e.event_id
LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
WHERE e.date >= '2026-04-13' AND e.date <= '2026-04-19'
  AND e.client_id = 1785
  AND (
      (se.confirmed = 1 AND se.cancel_reason = 0)
      OR t.client_min_bill = 1
      OR t.employee_min_pay = 1
  )
ORDER BY se.shift_employee_id
"""
df = pd.read_sql(q, engine)
print(df[['shift_employee_id', 'state', 'employee_worked', 'employee_min_pay', 'client_min_bill']].to_string())
print(f"\nTotal: {len(df)} shifts")

# Check each shift's pay calculation with SENTHOME+CA condition for employee also
# and WITHOUT it (new code)
print("\n\nShifts where SENTHOME+CA applies but employee_min_pay is NOT set:")
for _, row in df.iterrows():
    e_worked = str(row.get('employee_worked') or '').upper()
    e_min = row.get('employee_min_pay')
    state = str(row.get('state') or '').upper()
    
    senthome_ca = (e_worked == 'SENTHOME' and state in ('CA', 'CALIFORNIA'))
    emp_min_pay = (pd.notna(e_min) and float(e_min) > 0)
    
    if senthome_ca and not emp_min_pay:
        print(f"  SE {row['shift_employee_id']}: SENTHOME+CA but no employee_min_pay")
