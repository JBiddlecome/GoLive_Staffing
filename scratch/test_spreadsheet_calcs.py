import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

def get_engine():
    env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
    if os.path.exists(env_path):
        load_dotenv(env_path)
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = int(os.getenv("DB_PORT", "3306"))
    return create_engine(
        URL.create(
            drivername="mysql+pymysql",
            username=user,
            password=password,
            host=host,
            port=port,
            database=name,
        ),
        pool_pre_ping=True
    )

def parse_csv_rates(file_path: str):
    print(f"Reading CSV from {file_path}")
    df = pd.read_csv(file_path)
    
    # Normalize headers
    normalized_cols = {c: str(c).strip().lower().replace(" ", "_") for c in df.columns}
    df = df.rename(columns=normalized_cols)
    
    rates = {}
    df = df.dropna(subset=['venue_position_id'])
    for _, row in df.iterrows():
        try:
            vpid = int(float(row['venue_position_id']))
            new_pay = float(row['new_pay'])
            new_bill = float(row['new_bill'])
            rates[vpid] = {
                'new_pay': new_pay,
                'new_bill': new_bill
            }
        except (ValueError, TypeError):
            continue
    print(f"Loaded {len(rates)} rates from CSV.")
    return rates

def test_query_and_match():
    engine = get_engine()
    csv_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\apps\pay_rate_reduction_calculator\Markup_Analysis_Option_A.csv"
    rates = parse_csv_rates(csv_path)
    
    # Query sample shifts with venue_position join
    sql = text(
        """
        SELECT
            e.date,
            c.name AS client_name,
            se.bill_rate AS orig_bill_rate,
            se.rate AS orig_pay_rate,
            vp.venue_position_id AS venue_position_id,
            v.name AS venue_name,
            sp.position_id
        FROM shift_employee se
        JOIN event e ON se.event_id = e.event_id
        JOIN client c ON e.client_id = c.client_id
        LEFT JOIN venue v ON e.venue_id = v.venue_id
        LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
        LEFT JOIN venue_position vp ON vp.venue_id = e.venue_id AND vp.position_id = sp.position_id
        WHERE e.date >= '2026-04-25' AND e.date <= '2026-05-24'
          AND c.msp_id = 2
          AND (se.deleted_at IS NULL AND se.confirmed = 1 AND se.cancel_reason = 0)
        LIMIT 50
        """
    )
    
    print("Executing database query...")
    with engine.connect() as connection:
        df_shifts = pd.read_sql(sql, connection)
    
    print(f"Retrieved {len(df_shifts)} shifts.")
    if df_shifts.empty:
        print("No shifts found in the specified range.")
        return
        
    print("\nSample Shift Matches:")
    matches_found = 0
    for idx, row in df_shifts.iterrows():
        vpid_val = row['venue_position_id']
        vpid = None
        if pd.notna(vpid_val):
            vpid = int(float(vpid_val))
            
        matched_rate = rates.get(vpid) if vpid else None
        if matched_rate:
            matches_found += 1
            if matches_found <= 10:
                print(f"Shift Date: {row['date']}, Client: {row['client_name']}, Venue: {row['venue_name']}")
                print(f"  Venue Position ID: {vpid}")
                print(f"  Original Rates: Pay={row['orig_pay_rate']}, Bill={row['orig_bill_rate']}")
                print(f"  Spreadsheet Rates: Pay={matched_rate['new_pay']}, Bill={matched_rate['new_bill']}")
                print("-" * 40)
                
    print(f"Total matching shifts in sample: {matches_found} out of {len(df_shifts)}")

if __name__ == "__main__":
    test_query_and_match()
