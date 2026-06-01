import os
import pandas as pd
import shutil
import tempfile
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

# 1. Load the Excel file (handling file lock)
excel_path = r"c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\compass_position_ids.xlsx"
temp_dir = tempfile.gettempdir()
temp_excel_path = os.path.join(temp_dir, "temp_compass_position_ids.xlsx")

shutil.copy2(excel_path, temp_excel_path)
df_excel = pd.read_excel(temp_excel_path)
excel_ids = df_excel['venue_position_id'].dropna().astype(int).unique().tolist()
os.remove(temp_excel_path)

print(f"Loaded {len(excel_ids)} unique venue_position_ids from the Excel file.")

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

# 3. Check for matching timesheets (ignoring soft deletes)
# Let's perform a query that gets all matches from the list.
# We also include client names, venue names, position titles, etc. for the matches.
query = """
SELECT 
    vp.venue_position_id AS venue_position_id,
    COUNT(t.timesheet_id) AS timesheet_count,
    MIN(e.date) AS min_date,
    MAX(e.date) AS max_date,
    c.name AS client_name,
    v.name AS venue_name,
    p.description AS position_name
FROM timesheet t
JOIN shift_employee se ON se.shift_employee_id = t.shift_employee_id
JOIN shift_position sp ON sp.shift_position_id = se.shift_position_id
JOIN shift s ON s.shift_id = sp.shift_id
JOIN event e ON e.event_id = t.event_id
JOIN client c ON c.client_id = e.client_id
JOIN venue v ON v.venue_id = e.venue_id
LEFT JOIN position p ON sp.position_id = p.position_id
LEFT JOIN venue_position vp ON vp.venue_id = e.venue_id AND vp.position_id = sp.position_id
WHERE vp.venue_position_id IN :id_list
  AND se.deleted_at IS NULL
  AND sp.deleted_at IS NULL
  AND s.deleted_at IS NULL
  AND e.deleted_at IS NULL
GROUP BY vp.venue_position_id, c.name, v.name, p.description
"""

# Let's also do a query without soft delete filters to see if any soft-deleted matches exist
query_all = """
SELECT 
    vp.venue_position_id AS venue_position_id,
    COUNT(t.timesheet_id) AS timesheet_count
FROM timesheet t
JOIN shift_employee se ON se.shift_employee_id = t.shift_employee_id
JOIN shift_position sp ON sp.shift_position_id = se.shift_position_id
JOIN shift s ON s.shift_id = sp.shift_id
JOIN event e ON e.event_id = t.event_id
LEFT JOIN venue_position vp ON vp.venue_id = e.venue_id AND vp.position_id = sp.position_id
WHERE vp.venue_position_id IN :id_list
GROUP BY vp.venue_position_id
"""

with engine.connect() as conn:
    # We execute using SQL parameter substitution to avoid SQL injection / query syntax limits
    results = conn.execute(text(query), {"id_list": excel_ids}).mappings().all()
    results_all = conn.execute(text(query_all), {"id_list": excel_ids}).mappings().all()

# Print summary
matched_active_ids = sorted(list(set([r['venue_position_id'] for r in results])))
matched_all_ids = sorted(list(set([r['venue_position_id'] for r in results_all])))

print(f"\nResults with active (non-deleted) records:")
print(f"Total venue_position_ids matching active timesheets: {len(matched_active_ids)}")
print(f"Total venue_position_ids not matching active timesheets: {len(excel_ids) - len(matched_active_ids)}")

print(f"\nResults including deleted/cancelled records:")
print(f"Total venue_position_ids matching any timesheets: {len(matched_all_ids)}")

# Details on active matches
print("\nActive Match Details:")
df_matches = pd.DataFrame(results)
if not df_matches.empty:
    print(df_matches.to_string(index=False))
else:
    print("No matches found.")

# Let's write the results back to a clean text file or a new Excel report for the user if needed, 
# but first we will print the exact lists as requested.
missing_active_ids = sorted(list(set(excel_ids) - set(matched_active_ids)))
print("\nMatched active venue_position_ids:")
print(matched_active_ids)
print("\nNon-matched active venue_position_ids:")
print(missing_active_ids)
