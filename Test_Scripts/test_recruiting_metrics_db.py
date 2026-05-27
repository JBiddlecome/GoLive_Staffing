import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

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
    e.start_date AS `Start Date`,
    e.start_date2 AS `Rehire Date`,
    c.name AS `County of Residence`,
    e.concierge_date AS `Concierge Date`,
    pos_agg.positions AS `Positions`
FROM employee e
LEFT JOIN county c ON e.county_id = c.id
LEFT JOIN (
    SELECT ep.employee_id, GROUP_CONCAT(DISTINCT p.description ORDER BY p.description SEPARATOR ', ') as positions
    FROM employee_position ep
    JOIN position p ON ep.position_id = p.position_id
    WHERE ep.status = 'ACTIVE' AND ep.eligible = 1
    GROUP BY ep.employee_id
) pos_agg ON e.employee_id = pos_agg.employee_id
WHERE (e.payroll_id IS NULL OR e.payroll_id NOT LIKE '%DELETED%')
  AND e.status != 'DELETED'
LIMIT 20;
"""

try:
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
        print("Columns retrieved:", list(df.columns))
        print("Data retrieved:")
        print(df.to_string())
except Exception as e:
    print("Error querying database:", e)
finally:
    engine.dispose()
