import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

query = """
SELECT se.shift_employee_id, se.overtime 
FROM shift_employee se 
JOIN event e ON se.event_id = e.event_id 
WHERE e.date >= '2026-04-13' AND e.date <= '2026-04-19' AND e.client_id = 1785 
AND se.confirmed = 1 AND se.cancel_reason = 0
"""
df = pd.read_sql(query, engine)
print(df.to_string())
