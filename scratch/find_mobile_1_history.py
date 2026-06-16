import os
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

from sqlalchemy import text
from apps.position_requests.scheduler import _engine
import json

def main():
    engine = _engine()
    
    with engine.connect() as conn:
        # Find 10 employees whose current mobile starts with 1
        sql_emp = text("""
            SELECT employee_id, first_name, last_name, mobile
            FROM employee
            WHERE deleted_at IS NULL
              AND status IN (1, 3)
              AND (mobile LIKE '1%' OR mobile LIKE '+1%')
            LIMIT 10
        """)
        
        employees = conn.execute(sql_emp).fetchall()
        print(f"Found {len(employees)} employees starting with 1:")
        for emp in employees:
            emp_id, first, last, mobile = emp
            print(f"\nEmployee: {first} {last} (ID: {emp_id}) | Current Mobile: {mobile}")
            
            # Get all history entries for this employee
            sql_hist = text("""
                SELECT id, changes, notes, created_at
                FROM history_entry
                WHERE related = 'Employee'
                  AND related_id = :emp_id
                ORDER BY created_at DESC
            """)
            
            hist_rows = conn.execute(sql_hist, {"emp_id": emp_id}).fetchall()
            print(f"  History count: {len(hist_rows)}")
            for h in hist_rows:
                h_id, changes_str, notes, created_at = h
                if not changes_str:
                    continue
                try:
                    changes_json = json.loads(changes_str)
                    # Check if mobile is in changes
                    has_mobile = False
                    if isinstance(changes_json, list):
                        for entry in changes_json:
                            if "mobile" in entry.get("attributes", {}):
                                has_mobile = True
                    elif isinstance(changes_json, dict):
                        if "mobile" in changes_json.get("attributes", {}):
                            has_mobile = True
                            
                    if has_mobile:
                        print(f"  [Mobile Change] ID: {h_id} | Notes: {repr(notes)} | Created At: {created_at}")
                        print(f"    Changes: {changes_str}")
                except Exception as ex:
                    pass

if __name__ == "__main__":
    main()
