import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

query = """
SELECT se.shift_employee_id, se.deleted_at as se_del, e.deleted_at as e_del, 
sp.deleted_at as sp_del, s.deleted_at as s_del
FROM shift_employee se 
JOIN event e ON se.event_id = e.event_id 
JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
JOIN shift s ON sp.shift_id = s.shift_id
WHERE e.date >= '2026-04-13' AND e.date <= '2026-04-19' AND e.client_id = 1785 
AND se.confirmed = 1 AND se.cancel_reason = 0
"""
df = pd.read_sql(query, engine)
print(df)
