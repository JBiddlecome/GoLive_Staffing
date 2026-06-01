import os
import pandas as pd
import shutil
import tempfile
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

# 1. Load the Excel file
excel_path = r"c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\compass_position_ids.xlsx"
temp_dir = tempfile.gettempdir()
temp_excel_path = os.path.join(temp_dir, "temp_compass_position_ids.xlsx")

shutil.copy2(excel_path, temp_excel_path)
df_excel = pd.read_excel(temp_excel_path)
excel_ids = df_excel['venue_position_id'].dropna().astype(int).unique().tolist()
os.remove(temp_excel_path)

# 2. Connect to the DB
env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
load_dotenv(env_path)

def get_engine():
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))
    
    return create_engine(URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name
    ))

engine = get_engine()

query_active = """
SELECT DISTINCT vp.venue_position_id
FROM timesheet t
JOIN shift_employee se ON se.shift_employee_id = t.shift_employee_id
JOIN shift_position sp ON sp.shift_position_id = se.shift_position_id
JOIN shift s ON s.shift_id = sp.shift_id
JOIN event e ON e.event_id = t.event_id
LEFT JOIN venue_position vp ON vp.venue_id = e.venue_id AND vp.position_id = sp.position_id
WHERE vp.venue_position_id IN :id_list
  AND se.deleted_at IS NULL
  AND sp.deleted_at IS NULL
  AND s.deleted_at IS NULL
  AND e.deleted_at IS NULL
"""

query_all = """
SELECT DISTINCT vp.venue_position_id
FROM timesheet t
JOIN shift_employee se ON se.shift_employee_id = t.shift_employee_id
JOIN shift_position sp ON sp.shift_position_id = se.shift_position_id
JOIN shift s ON s.shift_id = sp.shift_id
JOIN event e ON e.event_id = t.event_id
LEFT JOIN venue_position vp ON vp.venue_id = e.venue_id AND vp.position_id = sp.position_id
WHERE vp.venue_position_id IN :id_list
"""

with engine.connect() as conn:
    active_res = conn.execute(text(query_active), {"id_list": excel_ids}).fetchall()
    all_res = conn.execute(text(query_all), {"id_list": excel_ids}).fetchall()

active_ids = sorted([r[0] for r in active_res if r[0] is not None])
all_ids = sorted([r[0] for r in all_res if r[0] is not None])

print("Unique Excel IDs:", len(excel_ids))
print("Matched Active IDs:", len(active_ids))
print("Matched All IDs (incl. deleted):", len(all_ids))

difference = set(all_ids) - set(active_ids)
print("IDs that match ONLY soft-deleted/cancelled timesheets:", sorted(list(difference)))
