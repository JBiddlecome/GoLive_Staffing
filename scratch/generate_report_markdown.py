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
    
    # Query for exact shifts in March/April 2026 for the 11 target venues
    query_details = """
    SELECT 
        c.name AS client_name,
        v.name AS venue_name,
        e.date AS event_date,
        s.start AS shift_start,
        s.end AS shift_end,
        p.description AS position_title,
        sp.additional_title,
        COALESCE(CONCAT(emp.first_name, ' ', emp.last_name), 'Unassigned / Vacant') AS employee_name
    FROM shift_position sp
    JOIN shift s ON sp.shift_id = s.shift_id
    JOIN event e ON s.event_id = e.event_id
    JOIN venue v ON e.venue_id = v.venue_id
    JOIN client c ON e.client_id = c.client_id
    LEFT JOIN position p ON sp.position_id = p.position_id
    LEFT JOIN shift_employee se ON sp.shift_position_id = se.shift_position_id AND se.deleted_at IS NULL AND se.cancel_reason = 0
    LEFT JOIN employee emp ON se.employee_id = emp.employee_id AND emp.deleted_at IS NULL
    WHERE e.date BETWEEN '2026-03-01' AND '2026-04-30'
      AND (LOWER(sp.additional_title) LIKE '%%ongoing%%' OR LOWER(sp.additional_title) LIKE '%%on going%%')
      AND sp.deleted_at IS NULL
      AND s.deleted_at IS NULL
      AND e.deleted_at IS NULL
      AND v.deleted_at IS NULL
      AND c.deleted_at IS NULL
    ORDER BY v.name ASC, p.description ASC, e.date ASC, s.start ASC
    """
    
    df = pd.read_sql_query(query_details, engine)
    
    # Query comparison data
    comp_csv_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\scratch\ongoing_june_comparison.csv"
    df_comp = pd.read_csv(comp_csv_path)
    
    report_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\scratch\ongoing_assignments_report.md"
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("# GoLive Staffing — Ongoing Assignments Report\n\n")
        f.write("> **Purpose**: Analyzing ongoing assignments (shifts in March & April 2026 containing 'ongoing' or 'on going' in the title) at specified venues and verifying if they are missing in June 2026.\n\n")
        
        f.write("## Part 1: June 2026 Status Summary\n\n")
        f.write("| Client Name | Position | March/April Shift Count | March/April Employees | June 2026 Status | June Shift Count |\n")
        f.write("| :--- | :--- | :---: | :--- | :---: | :---: |\n")
        
        for idx, row in df_comp.iterrows():
            status_emoji = "❌ MISSING" if "MISSING" in row['June Status'] else "✅ ACTIVE"
            f.write(f"| {row['Client Name']} | {row['Position Title']} | {row['March/April Ongoing Shift Count']} | {row['March/April Assigned Employees']} | {status_emoji} | {row['June Shift Count']} |\n")
        
        f.write("\n---\n\n")
        f.write("## Part 2: Detailed Ongoing Shift Records (March & April 2026)\n\n")
        
        # Group by Venue name
        grouped = df.groupby('venue_name')
        for venue_name, group in grouped:
            f.write(f"### 📍 {venue_name}\n")
            f.write(f"**Client:** {group['client_name'].iloc[0]}\n\n")
            
            f.write("| Event Date | Position | Shift Start | Shift End | Additional Title | Employee Name |\n")
            f.write("| :--- | :--- | :---: | :---: | :--- | :--- |\n")
            
            for idx, row in group.iterrows():
                # Extract time string
                start_time = pd.to_datetime(row['shift_start']).strftime('%I:%M %p') if pd.notna(row['shift_start']) else 'N/A'
                end_time = pd.to_datetime(row['shift_end']).strftime('%I:%M %p') if pd.notna(row['shift_end']) else 'N/A'
                
                f.write(f"| {row['event_date']} | {row['position_title']} | {start_time} | {end_time} | {row['additional_title'] or ''} | {row['employee_name']} |\n")
            f.write("\n")
            
    print(f"Report successfully generated and saved to: {report_path}")

if __name__ == "__main__":
    main()
