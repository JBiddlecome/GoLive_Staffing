import os, pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv(r'C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env')
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT','3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)
q = """
SELECT se.shift_employee_id, t.employee_no_break_penalty, t.client_no_break_penalty 
FROM shift_employee se 
JOIN event e ON se.event_id = e.event_id 
LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id 
WHERE e.date >= '2026-04-13' AND e.date <= '2026-04-19' 
  AND e.client_id = 1785 
"""
df = pd.read_sql(q, engine)
# Show non-null break penalties
has_penalty = df[df['employee_no_break_penalty'].notna() & (df['employee_no_break_penalty'] != 0)]
print("Shifts with meal penalties:")
if len(has_penalty) > 0:
    print(has_penalty.to_string())
else:
    print("None - all are 0 or NULL")
    
# Check all values
print("\nAll penalty values:")
print(df[['shift_employee_id','employee_no_break_penalty','client_no_break_penalty']].to_string())
