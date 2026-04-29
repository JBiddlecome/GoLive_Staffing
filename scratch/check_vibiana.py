import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

# Get Vibiana client_id
q = "SELECT client_id, name FROM client WHERE name LIKE '%%Vibiana%%'"
df = pd.read_sql(q, engine)
print(df.to_string())
print()

# Get Vibiana shifts for the date range
q2 = """
SELECT se.shift_employee_id, e.client_id, t.use_sheet, 
       t.client_seconds, t.employee_seconds, 
       t.client_worked, t.employee_worked,
       t.client_min_bill, t.employee_min_pay,
       t.client_start, t.employee_start,
       s.start as shift_start, se.rate, se.bill_rate
FROM shift_employee se
JOIN event e ON se.event_id = e.event_id
LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
LEFT JOIN shift s ON sp.shift_id = s.shift_id
WHERE e.date >= '2026-04-13' AND e.date <= '2026-04-19'
  AND e.client_id IN (SELECT client_id FROM client WHERE name LIKE '%%Vibiana%%')
  AND se.confirmed = 1 AND se.cancel_reason = 0
ORDER BY se.shift_employee_id
"""
df2 = pd.read_sql(q2, engine)
print(f"Vibiana shifts: {len(df2)}")
print(df2[['shift_employee_id','use_sheet','client_seconds','employee_seconds','client_worked','employee_worked','client_min_bill']].to_string())
