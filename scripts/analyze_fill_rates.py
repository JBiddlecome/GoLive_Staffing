import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def get_db_url():
    # Load environment variables
    # Update this path if the script is run from a different directory
    env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
    if os.path.exists(env_path):
        load_dotenv(env_path)

    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = os.getenv("DB_PORT", "3306")
    
    if not all([host, user, password]):
        print("Error: Missing database credentials in environment variables.")
        sys.exit(1)
        
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"

def main():
    engine = create_engine(get_db_url(), pool_pre_ping=True)
    
    # Query to get shift position data over the last year,
    # joining with shift_employee to find ACTUAL confirmed employees.
    # 
    # Logic:
    # - shift_position (sp): defines the number of spots needed (sp.count).
    # - shift_employee (se): defines the employees actually assigned to a position slot.
    # - We count se.shift_employee_id to get the actual number of filled spots, 
    #   filtering by confirmed = 1, cancel_reason = 0, and not deleted.
    query = text("""
        SELECT 
            sp.shift_position_id,
            sp.count AS spots_needed,
            sp.rate AS pay_rate,
            sp.base_rate AS base_pay_rate,
            COUNT(se.shift_employee_id) AS spots_filled
        FROM shift_position sp
        JOIN shift s ON sp.shift_id = s.shift_id
        JOIN event e ON s.event_id = e.event_id
        LEFT JOIN shift_employee se ON se.shift_position_id = sp.shift_position_id 
            AND se.deleted_at IS NULL 
            AND se.confirmed = 1 
            AND se.cancel_reason = 0
        WHERE e.date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
          AND sp.deleted_at IS NULL
          AND s.deleted_at IS NULL
          AND e.deleted_at IS NULL
          AND sp.count > 0
        GROUP BY sp.shift_position_id, sp.count, sp.rate, sp.base_rate
    """)
    
    print("Executing query to fetch data...")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
        
    if df.empty:
        print("No data found for the last year.")
        return
        
    # Calculate if the shift was completely filled
    df['fill_ratio'] = df['spots_filled'] / df['spots_needed']
    df['is_fully_filled'] = (df['fill_ratio'] >= 1.0).astype(int)
    
    # Analyze by Pay Rate Ranges
    print("\n--- Fill Rate by Pay Rate Ranges ---")
    bins = [0, 15, 20, 25, 30, 1000]
    labels = ['<$15', '$15-$20', '$20-$25', '$25-$30', '>$30']
    df['pay_rate_range'] = pd.cut(df['pay_rate'], bins=bins, labels=labels, right=False)
    
    rate_summary = df.groupby('pay_rate_range', observed=False).agg(
        total_positions=('shift_position_id', 'count'),
        avg_fill_ratio=('fill_ratio', 'mean'),
        fully_filled_rate=('is_fully_filled', 'mean')
    ).reset_index()
    
    print(rate_summary.to_string(index=False))
    
    # Analyze if increasing rate over base_rate helped
    print("\n--- Impact of Increasing Rate over Base Rate ---")
    # Some base_rates might be NULL or 0, filter valid ones
    valid_base = df[(df['base_pay_rate'].notna()) & (df['base_pay_rate'] > 0)].copy()
    
    valid_base['rate_increased'] = valid_base['pay_rate'] > valid_base['base_pay_rate']
    
    increase_summary = valid_base.groupby('rate_increased').agg(
        total_positions=('shift_position_id', 'count'),
        avg_fill_ratio=('fill_ratio', 'mean'),
        fully_filled_rate=('is_fully_filled', 'mean')
    ).reset_index()
    
    increase_summary['rate_increased'] = increase_summary['rate_increased'].map({True: 'Yes (Rate > Base)', False: 'No (Rate <= Base)'})
    print(increase_summary.to_string(index=False))
    
    # Further breakdown: By how much was it increased?
    valid_base['increase_amount'] = valid_base['pay_rate'] - valid_base['base_pay_rate']
    valid_base['increase_bucket'] = pd.cut(
        valid_base['increase_amount'], 
        bins=[-100, 0, 2, 5, 100], 
        labels=['No Increase', '$0.01 - $2.00', '$2.01 - $5.00', '>$5.00'],
        right=True
    )
    
    print("\n--- Impact by Increase Amount ---")
    amount_summary = valid_base.groupby('increase_bucket', observed=False).agg(
        total_positions=('shift_position_id', 'count'),
        avg_fill_ratio=('fill_ratio', 'mean'),
        fully_filled_rate=('is_fully_filled', 'mean')
    ).reset_index()
    
    print(amount_summary.to_string(index=False))

if __name__ == "__main__":
    main()
