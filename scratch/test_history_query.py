import os
import json
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

from sqlalchemy import text
from apps.position_requests.scheduler import _engine

def get_original_phone(employee_id):
    engine = _engine()
    sql = text("""
        SELECT changes 
        FROM history_entry 
        WHERE related = 'Employee' 
          AND related_id = :employee_id 
          AND notes = 'Dash Sync'
        ORDER BY created_at DESC
    """)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql, {"employee_id": employee_id}).fetchall()
            print(f"Found {len(rows)} entries for employee {employee_id}")
            for row in rows:
                changes_str = row[0]
                print("raw changes:", changes_str)
                if not changes_str:
                    continue
                try:
                    changes_json = json.loads(changes_str)
                    if isinstance(changes_json, list):
                        for entry in changes_json:
                            attributes = entry.get("attributes", {})
                            mobile_data = attributes.get("mobile", {})
                            if isinstance(mobile_data, dict):
                                old_phone = mobile_data.get("old")
                                if old_phone:
                                    return old_phone
                    elif isinstance(changes_json, dict):
                        attributes = changes_json.get("attributes", {})
                        mobile_data = attributes.get("mobile", {})
                        if isinstance(mobile_data, dict):
                            old_phone = mobile_data.get("old")
                            if old_phone:
                                return old_phone
                except Exception as ex:
                    print("JSON parsing error:", ex)
    except Exception as e:
        print("Database error:", e)
    return None

if __name__ == "__main__":
    res = get_original_phone(46648)
    print("Resolved old phone:", res)
