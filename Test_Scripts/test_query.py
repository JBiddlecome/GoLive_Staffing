import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def _db_url_from_env():
    host = os.getenv("DB_HOST", "127.0.0.1")
    name = os.getenv("DB_NAME", "cstaffing")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    port = int(os.getenv("DB_PORT", "3306"))
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"

engine = create_engine(_db_url_from_env())

sql = """
SELECT 
    c.name,
    e.date,
    sp.shift_position_id,
    sp.count,
    sp.created_at AS sp_created_at,
    se.shift_employee_id,
    se.created_at AS se_created_at,
    se.confirmed_at AS se_confirmed_at,
    t.timesheet_id,
    TIMESTAMPDIFF(SECOND, sp.created_at, COALESCE(se.confirmed_at, se.created_at)) AS diff1,
    TIMESTAMPDIFF(SECOND, se.created_at, se.confirmed_at) AS diff2
FROM client c
JOIN event e ON c.client_id = e.client_id
JOIN shift s ON e.event_id = s.event_id
JOIN shift_position sp ON s.shift_id = sp.shift_id
JOIN shift_employee se ON sp.shift_position_id = se.shift_position_id
LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
WHERE c.name LIKE '%Alonti%'
  AND e.date >= '2026-01-01'
ORDER BY e.date DESC
LIMIT 10;
"""

with engine.connect() as conn:
    df = pd.read_sql(text(sql), conn)
    print(df.to_string())
