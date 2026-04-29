import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

# Check DMC service charges
q = """
SELECT se.shift_employee_id, se.bill_rate, t.client_service_charge, t.employee_service_charge, v.service_charge
FROM shift_employee se
JOIN event e ON se.event_id = e.event_id
LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
LEFT JOIN venue v ON e.venue_id = v.venue_id
WHERE e.date >= '2026-04-13' AND e.date <= '2026-04-19'
  AND e.client_id = 1785
  AND (se.confirmed = 1 AND se.cancel_reason = 0)
"""
df = pd.read_sql(q, engine)
print(df.to_string())
print()

# Also check the legacy ClientsAndEmployeesExportJob to understand service charge
# Check the Vibiana service charge
q2 = """
SELECT se.shift_employee_id, se.bill_rate, t.client_service_charge, t.employee_service_charge
FROM shift_employee se
JOIN event e ON se.event_id = e.event_id
LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
WHERE e.date >= '2026-04-13' AND e.date <= '2026-04-19'
  AND e.client_id = 1131
  AND (se.confirmed = 1 AND se.cancel_reason = 0)
"""
df2 = pd.read_sql(q2, engine)
print("Vibiana service charges:")
print(df2.to_string())
