import sys
from sqlalchemy import create_engine, text
sys.path.append(r'c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing')
from scripts.find_event_supervisors import _get_db_url
engine = create_engine(_get_db_url())
with engine.connect() as conn:
    query = text("""
        SELECT e.employee_id, e.first_name, e.last_name, e.email, e.mobile, e.resume,
               GROUP_CONCAT(ep.position_id) as positions
        FROM employee e
        LEFT JOIN employee_position ep ON e.employee_id = ep.employee_id AND ep.status = 1
        WHERE e.status = 1 
          AND e.resume IS NOT NULL 
          AND e.resume != ''
          AND e.deleted_at IS NULL
        GROUP BY e.employee_id
        LIMIT 5
    """)
    res = conn.execute(query).fetchall()
    for r in res:
        print(r)
