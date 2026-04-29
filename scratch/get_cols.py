import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

query = """
SELECT 
    se.shift_employee_id, 
    s.start AS shift_start_time,
    t.employee_start,
    t.client_start
FROM shift_employee se 
JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
JOIN shift s ON sp.shift_id = s.shift_id
LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id 
WHERE se.shift_employee_id IN (631951, 632441)
"""
df = pd.read_sql(query, engine)
print(df.to_string())
