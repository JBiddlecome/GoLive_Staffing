import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

# Load the environment variables
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

def main():
    engine = get_engine()
    
    query = """
    SELECT 
        e.date AS `Event Date`,
        c.name AS `Client Name`,
        v.name AS `Venue Name`,
        p.description AS `Position Name`,
        vp.venue_position_id AS `Venue Position ID`,
        t.timesheet_id AS `Timesheet ID`,
        se.shift_employee_id AS `Shift Employee ID`,
        emp.first_name AS `Employee First Name`,
        emp.last_name AS `Employee Last Name`,
        t.use_sheet AS `Use Sheet`,
        t.employee_seconds AS `Employee Seconds`,
        t.client_seconds AS `Client Seconds`
    FROM timesheet t
    JOIN shift_employee se ON se.shift_employee_id = t.shift_employee_id
    JOIN shift_position sp ON sp.shift_position_id = se.shift_position_id
    JOIN shift s ON s.shift_id = sp.shift_id
    JOIN event e ON e.event_id = t.event_id
    JOIN client c ON c.client_id = e.client_id
    JOIN venue v ON v.venue_id = e.venue_id
    JOIN employee emp ON emp.employee_id = se.employee_id
    LEFT JOIN position p ON sp.position_id = p.position_id
    LEFT JOIN venue_position vp ON vp.venue_id = e.venue_id AND vp.position_id = sp.position_id
    WHERE e.date BETWEEN '2026-04-25' AND '2026-05-24'
      AND c.deleted_at IS NULL
      AND v.deleted_at IS NULL
      AND e.deleted_at IS NULL
      AND s.deleted_at IS NULL
      AND sp.deleted_at IS NULL
      AND se.deleted_at IS NULL
      AND emp.deleted_at IS NULL
    ORDER BY e.date ASC, c.name ASC
    """
    
    print("Executing query...")
    df = pd.read_sql_query(query, engine)
    print(f"Retrieved {len(df)} rows.")
    
    if not df.empty:
        print(df.head(10))
    else:
        print("No records found in this range.")

if __name__ == "__main__":
    main()
