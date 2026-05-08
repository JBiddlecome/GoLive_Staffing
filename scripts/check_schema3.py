import sys
from sqlalchemy import create_engine, text
sys.path.append(r'c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing')
from scripts.find_event_supervisors import _get_db_url
engine = create_engine(_get_db_url())
with engine.connect() as conn:
    print('Finding Server positions:')
    server_pos = conn.execute(text("SELECT position_id, description FROM position WHERE description LIKE '%Server 2%'")).fetchall()
    for row in server_pos:
        print(row)
