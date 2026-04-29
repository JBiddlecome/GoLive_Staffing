import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

query = """
SELECT se.shift_employee_id, t.timesheet_id 
FROM shift_employee se 
JOIN event e ON se.event_id = e.event_id 
LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id 
WHERE e.date >= '2026-04-13' AND e.date <= '2026-04-19' AND e.client_id = 1785 
AND ((se.confirmed = 1 AND se.cancel_reason = 0) OR t.client_min_bill = 1 OR t.employee_min_pay = 1)
"""
df = pd.read_sql(query, engine)
print(df[df['timesheet_id'].isna()])
