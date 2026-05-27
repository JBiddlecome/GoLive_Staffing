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
    
    # 1. Let's first search for all venues that have "ongoing" or "on going" in additional_title in March/April 2026
    # to make sure we map them correctly.
    query_venues = """
    SELECT DISTINCT v.name AS venue_name, c.name AS client_name, c.client_id, v.venue_id
    FROM shift_position sp
    JOIN shift s ON sp.shift_id = s.shift_id
    JOIN event e ON s.event_id = e.event_id
    JOIN venue v ON e.venue_id = v.venue_id
    JOIN client c ON e.client_id = c.client_id
    WHERE e.date BETWEEN '2026-03-01' AND '2026-04-30'
      AND (LOWER(sp.additional_title) LIKE '%%ongoing%%' OR LOWER(sp.additional_title) LIKE '%%on going%%')
      AND sp.deleted_at IS NULL
      AND s.deleted_at IS NULL
      AND e.deleted_at IS NULL
      AND v.deleted_at IS NULL
      AND c.deleted_at IS NULL
    """
    
    print("Finding venues with ongoing shifts in March/April 2026...")
    with engine.connect() as conn:
        venues_found = conn.execute(text(query_venues)).mappings().all()
        print(f"Found {len(venues_found)} unique venues:")
        for vf in venues_found:
            print(f"- Venue: {vf['venue_name']} | Client: {vf['client_name']} (ID: {vf['client_id']})")
            
    # 2. Get detailed shift information for these venues in March & April
    # Required: position, shift times (start, end), employee names
    query_details = """
    SELECT 
        c.client_id,
        c.name AS client_name,
        v.venue_id,
        v.name AS venue_name,
        e.date AS event_date,
        s.start AS shift_start,
        s.end AS shift_end,
        sp.position_id,
        p.description AS position_title,
        sp.additional_title,
        emp.employee_id,
        CONCAT(emp.first_name, ' ', emp.last_name) AS employee_name,
        se.cancel_reason,
        se.confirmed
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
    ORDER BY c.name ASC, v.name ASC, p.description ASC, e.date ASC, s.start ASC
    """
    
    print("\nRetrieving ongoing shift details for March/April...")
    df_shifts = pd.read_sql_query(query_details, engine)
    print(f"Retrieved {len(df_shifts)} shift employee placements/slots.")
    
    # Save raw detailed shifts to Excel / CSV in scratch for debugging if needed
    df_shifts.to_csv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\scratch\march_april_ongoing_raw.csv", index=False)

    # 3. Check for June 2026 shifts for the SAME CLIENTS and SAME POSITIONS
    # Let's get the distinct client_ids and position_ids from the March/April ongoing list
    client_positions = df_shifts[['client_id', 'client_name', 'position_id', 'position_title']].drop_duplicates()
    print(f"\nWe have {len(client_positions)} unique Client-Position pairs to check in June 2026.")
    
    # We will query all active shifts in June 2026 for these clients
    june_query = """
    SELECT 
        c.client_id,
        c.name AS client_name,
        v.venue_id,
        v.name AS venue_name,
        e.date AS event_date,
        s.start AS shift_start,
        s.end AS shift_end,
        sp.position_id,
        p.description AS position_title,
        sp.additional_title,
        COUNT(DISTINCT se.shift_employee_id) AS active_placements
    FROM shift_position sp
    JOIN shift s ON sp.shift_id = s.shift_id
    JOIN event e ON s.event_id = e.event_id
    JOIN venue v ON e.venue_id = v.venue_id
    JOIN client c ON e.client_id = c.client_id
    LEFT JOIN position p ON sp.position_id = p.position_id
    LEFT JOIN shift_employee se ON sp.shift_position_id = se.shift_position_id AND se.deleted_at IS NULL AND se.cancel_reason = 0
    WHERE e.date BETWEEN '2026-06-01' AND '2026-06-30'
      AND c.deleted_at IS NULL
      AND v.deleted_at IS NULL
      AND e.deleted_at IS NULL
      AND s.deleted_at IS NULL
      AND sp.deleted_at IS NULL
    GROUP BY c.client_id, c.name, v.venue_id, v.name, e.date, s.start, s.end, sp.position_id, p.description, sp.additional_title
    """
    
    print("\nRetrieving June 2026 shifts...")
    df_june = pd.read_sql_query(june_query, engine)
    print(f"Retrieved {len(df_june)} shifts in June 2026 for all clients.")
    
    # Filter June shifts to only the relevant clients & positions
    # Check if there are any shifts in June for each Client + Position combination
    results = []
    for idx, cp in client_positions.iterrows():
        c_id = cp['client_id']
        p_id = cp['position_id']
        c_name = cp['client_name']
        p_title = cp['position_title']
        
        # Filter June shifts for this client and position
        june_matches = df_june[(df_june['client_id'] == c_id) & (df_june['position_id'] == p_id)]
        
        # Filter March/April shifts for this client and position to count how many ongoing shifts they had
        ma_matches = df_shifts[(df_shifts['client_id'] == c_id) & (df_shifts['position_id'] == p_id)]
        unique_ma_employees = ma_matches['employee_name'].dropna().unique()
        ma_employee_str = ", ".join(unique_ma_employees) if len(unique_ma_employees) > 0 else "None assigned"
        
        # Check if they have shifts in June
        if len(june_matches) == 0:
            status = "MISSING IN JUNE"
            june_shift_count = 0
            june_placements = 0
        else:
            status = "ACTIVE IN JUNE"
            june_shift_count = len(june_matches)
            june_placements = june_matches['active_placements'].sum()
            
        results.append({
            'Client ID': c_id,
            'Client Name': c_name,
            'Position ID': p_id,
            'Position Title': p_title,
            'March/April Ongoing Shift Count': len(ma_matches),
            'March/April Assigned Employees': ma_employee_str,
            'June Status': status,
            'June Shift Count': june_shift_count,
            'June Placements Count': june_placements
        })
        
    df_comparison = pd.DataFrame(results)
    
    print("\n--- Summary Comparison of Client-Positions in June 2026 ---")
    print(df_comparison.to_string(index=False))
    
    # Save the comparison to a CSV
    comp_csv_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\scratch\ongoing_june_comparison.csv"
    df_comparison.to_csv(comp_csv_path, index=False)
    print(f"\nSaved comparison result to: {comp_csv_path}")

if __name__ == "__main__":
    main()
