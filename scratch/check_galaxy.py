import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

# L.A. Galaxy shifts
q = """
SELECT se.shift_employee_id, se.rate, se.bill_rate,
       t.use_sheet, t.client_worked, t.employee_worked,
       t.client_seconds, t.employee_seconds,
       t.client_min_bill, t.employee_min_pay,
       t.client_no_bill, t.employee_no_pay
FROM shift_employee se
JOIN event e ON se.event_id = e.event_id
LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
WHERE e.date >= '2026-04-13' AND e.date <= '2026-04-19'
  AND e.client_id = (SELECT client_id FROM client WHERE name = 'L.A. Galaxy')
  AND se.confirmed = 1 AND se.cancel_reason = 0
"""
df = pd.read_sql(q, engine)
print(df.to_string())
print()

# Also check what Garibaldina Society has
q2 = """
SELECT se.shift_employee_id, se.rate, se.bill_rate,
       t.use_sheet, t.client_worked, t.employee_worked,
       t.client_seconds, t.employee_seconds
FROM shift_employee se
JOIN event e ON se.event_id = e.event_id
LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
WHERE e.date >= '2026-04-13' AND e.date <= '2026-04-19'
  AND e.client_id = (SELECT client_id FROM client WHERE name = 'Garibaldina Society ')
  AND se.confirmed = 1 AND se.cancel_reason = 0
"""
df2 = pd.read_sql(q2, engine)
print(df2.to_string())
