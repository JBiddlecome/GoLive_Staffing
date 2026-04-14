import sys
import os
sys.path.append('c:\\Users\\jakeb\\OneDrive\\Documents\\GitHub\\GoLive_Staffing')
from sqlalchemy import create_engine, text
from apps.meal_penalty_dashboard.views import _db_url_from_env

def main():
    engine = create_engine(_db_url_from_env())
    with engine.connect() as connection:
        sql = text("""
            SELECT 
                t.timesheet_id, e.date,
                t.employee_start,
                t.employee_end,
                t.employee_break_start,
                t.employee_break_end,
                t.employee_sec_break_start,
                t.employee_sec_break_end,
                t.employee_no_sec_break_reason,
                TIMESTAMPDIFF(MINUTE, t.employee_break_start, t.employee_break_end) AS break_minutes,
                TIMESTAMPDIFF(MINUTE, t.employee_sec_break_start, t.employee_sec_break_end) AS sec_break_minutes,
                t.employee_no_break_penalty,
                t.client_no_break_penalty
            FROM timesheet t
            JOIN event e ON t.event_id = e.event_id
            WHERE t.timesheet_id = 464873
        """)
        
        result = connection.execute(sql).mappings().first()
        if result:
            print(dict(result))
        else:
            print("Timesheet not found")

if __name__ == "__main__":
    main()
