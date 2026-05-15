import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def get_db_url():
    env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
    if os.path.exists(env_path):
        load_dotenv(env_path)

    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = os.getenv("DB_PORT", "3306")
    
    if not all([host, user, password]):
        print("Error: Missing database credentials.")
        sys.exit(1)
        
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"

def main():
    engine = create_engine(get_db_url(), pool_pre_ping=True)
    
    query = text("""
        SELECT 
            sp.shift_position_id,
            sp.count AS spots_needed,
            sp.rate AS pay_rate,
            sp.base_rate AS base_pay_rate,
            COUNT(se.shift_employee_id) AS spots_filled,
            COALESCE(c.name, 'Unknown County') AS county_name
        FROM shift_position sp
        JOIN shift s ON sp.shift_id = s.shift_id
        JOIN event e ON s.event_id = e.event_id
        JOIN venue v ON e.venue_id = v.venue_id
        LEFT JOIN county c ON v.county_id = c.id
        LEFT JOIN shift_employee se ON se.shift_position_id = sp.shift_position_id 
            AND se.deleted_at IS NULL 
            AND se.confirmed = 1 
            AND se.cancel_reason = 0
        WHERE e.date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
          AND sp.deleted_at IS NULL
          AND s.deleted_at IS NULL
          AND e.deleted_at IS NULL
          AND sp.count > 0
        GROUP BY sp.shift_position_id, sp.count, sp.rate, sp.base_rate, c.name
    """)
    
    print("Executing query to fetch data...")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
        
    if df.empty:
        print("No data found for the last year.")
        return
        
    df['fill_ratio'] = df['spots_filled'] / df['spots_needed']
    df['is_fully_filled'] = (df['fill_ratio'] >= 1.0).astype(int)
    
    print("\n--- Fill Rate by County ---")
    county_summary = df.groupby('county_name', observed=False).agg(
        total_positions=('shift_position_id', 'count'),
        avg_fill_ratio=('fill_ratio', 'mean'),
        fully_filled_rate=('is_fully_filled', 'mean')
    ).reset_index()
    
    county_summary = county_summary.sort_values('total_positions', ascending=False)
    print(county_summary.to_string(index=False))

    # Output detailed markdown to file
    md_lines = ["# Shift Fill Rate Analysis by County (Last Year)", ""]
    md_lines.append("## Overall County Fill Rates")
    md_lines.append("| County | Total Positions | Avg Fill Ratio | Fully Filled % |")
    md_lines.append("|--------|-----------------|----------------|----------------|")
    for _, row in county_summary.iterrows():
        md_lines.append(f"| {row['county_name']} | {row['total_positions']:,} | {row['avg_fill_ratio']:.1%} | {row['fully_filled_rate']:.1%} |")
        
    md_lines.append("")
    md_lines.append("## Impact of Increasing Rate Over Base Rate (By County)")
    md_lines.append("| County | Strategy | Total Positions | Avg Fill Ratio | Fully Filled % |")
    md_lines.append("|--------|----------|-----------------|----------------|----------------|")
    
    valid_base = df[(df['base_pay_rate'].notna()) & (df['base_pay_rate'] > 0)].copy()
    valid_base['rate_increased'] = valid_base['pay_rate'] > valid_base['base_pay_rate']
    
    county_increase_summary = valid_base.groupby(['county_name', 'rate_increased'], observed=False).agg(
        total_positions=('shift_position_id', 'count'),
        avg_fill_ratio=('fill_ratio', 'mean'),
        fully_filled_rate=('is_fully_filled', 'mean')
    ).reset_index()
    
    county_increase_summary['Strategy'] = county_increase_summary['rate_increased'].map({True: 'Rate > Base', False: 'Rate <= Base'})
    
    # Sort for display: County Name, then Strategy
    county_increase_summary = county_increase_summary.sort_values(['county_name', 'Strategy'])
    
    for _, row in county_increase_summary.iterrows():
        if row['total_positions'] > 10:  # Only show if there's a reasonable sample
            md_lines.append(f"| {row['county_name']} | {row['Strategy']} | {row['total_positions']:,} | {row['avg_fill_ratio']:.1%} | {row['fully_filled_rate']:.1%} |")
    
    with open("scratch/county_fill_analysis.md", "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))
        
    print("\nDetailed markdown report saved to scratch/county_fill_analysis.md")

if __name__ == "__main__":
    main()
