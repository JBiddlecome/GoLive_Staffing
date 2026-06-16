import os
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

from sqlalchemy import text
from apps.position_requests.scheduler import _engine

def main():
    engine = _engine()
    # Query unique notes for 'Employee'
    sql_notes = text("""
        SELECT notes, COUNT(*) as count
        FROM history_entry 
        WHERE related = 'Employee'
        GROUP BY notes
    """)
    # Query sample history entry changes to see what they look like
    sql_samples = text("""
        SELECT id, related_id, changes, notes, created_at
        FROM history_entry
        WHERE related = 'Employee'
          AND (changes LIKE '%mobile%' OR changes LIKE '%phone%')
        ORDER BY created_at DESC
        LIMIT 10
    """)
    
    with engine.connect() as conn:
        print("--- DISTINCT NOTES FOR EMPLOYEE ---")
        rows_notes = conn.execute(sql_notes).fetchall()
        for r in rows_notes:
            print(f"Notes: {repr(r[0])} | Count: {r[1]}")
            
        print("\n--- SAMPLE HISTORY ENTRIES WITH MOBILE/PHONE CHANGES ---")
        rows_samples = conn.execute(sql_samples).fetchall()
        for r in rows_samples:
            print(f"ID: {r[0]} | Emp ID: {r[1]} | Notes: {repr(r[3])} | Created At: {r[4]}")
            print(f"Changes: {r[2]}")
            print("-" * 40)

if __name__ == "__main__":
    main()
