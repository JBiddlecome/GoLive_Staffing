import os
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

from sqlalchemy import text
from apps.position_requests.scheduler import _engine

def main():
    engine = _engine()
    
    with engine.connect() as conn:
        print("Checking for exact 'Dash Sync' notes:")
        cnt_dash_sync = conn.execute(text("SELECT COUNT(*) FROM history_entry WHERE related = 'Employee' AND notes = 'Dash Sync'")).scalar()
        print(f"Exact 'Dash Sync' count: {cnt_dash_sync}")
        
        print("\nChecking for case-insensitive/partial 'sync' in notes:")
        rows_sync = conn.execute(text("SELECT notes, COUNT(*) FROM history_entry WHERE related = 'Employee' AND notes LIKE '%sync%' GROUP BY notes")).fetchall()
        for r in rows_sync:
            print(f"Notes: {repr(r[0])} | Count: {r[1]}")
            
        print("\nChecking for any notes where related = 'Employee' and changes contain 'mobile':")
        cnt_mobile = conn.execute(text("SELECT COUNT(*) FROM history_entry WHERE related = 'Employee' AND changes LIKE '%mobile%'")).scalar()
        print(f"Count of Employee changes with 'mobile': {cnt_mobile}")
        
        if cnt_mobile > 0:
            print("\nRecent notes for 'Employee' changes with 'mobile':")
            rows_notes_mobile = conn.execute(text("SELECT notes, COUNT(*) FROM history_entry WHERE related = 'Employee' AND changes LIKE '%mobile%' GROUP BY notes")).fetchall()
            for r in rows_notes_mobile:
                print(f"Notes: {repr(r[0])} | Count: {r[1]}")

if __name__ == "__main__":
    main()
