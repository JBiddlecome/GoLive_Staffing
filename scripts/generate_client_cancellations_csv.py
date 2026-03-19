import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

# Load .env if present
load_dotenv()

def _db_url_from_env() -> URL:
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST", "localhost")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))

    if host in {"127.0.0.1", "localhost"} and not reportable_host:
        tunnel_port = os.getenv("LOCAL_TUNNEL_PORT")
        rds_host = os.getenv("RDS_HOST")
        if rds_host and (not tunnel_port or str(port) != tunnel_port):
            host = rds_host

    return URL.create(drivername="mysql+pymysql", username=user, password=password, host=host, port=port, database=name)

def generate_report():
    print("Connecting to database...")
    engine = create_engine(_db_url_from_env(), pool_pre_ping=True)
    
    start_date = "2025-01-01"
    end_date = "2026-12-31" # A future date to capture all
    
    try:
        placed_sql = text("""
            SELECT 
                DATE(e.date) AS event_date,
                e.client_id,
                c.name AS client_name,
                SUM(sp.count) AS total_shifts_placed
            FROM event e
            JOIN client c ON e.client_id = c.client_id
            JOIN shift s ON e.event_id = s.event_id
            JOIN shift_position sp ON s.shift_id = sp.shift_id
            WHERE e.date >= :start_date AND e.date <= :end_date
              AND e.deleted_at IS NULL
              AND s.deleted_at IS NULL
              AND sp.deleted_at IS NULL
            GROUP BY DATE(e.date), e.client_id, c.name
        """)

        cancelled_sql = text("""
            SELECT 
                DATE(e.date) AS event_date,
                e.client_id,
                COUNT(se.shift_employee_id) AS total_client_cancellations
            FROM event e
            JOIN shift_employee se ON e.event_id = se.event_id
            WHERE e.date >= :start_date AND e.date <= :end_date
              AND e.deleted_at IS NULL
              AND se.cancel_reason IN (4, 5, 41, 51)
            GROUP BY DATE(e.date), e.client_id
        """)

        print("Querying shifts placed...")
        with engine.connect() as connection:
            placed_df = pd.read_sql(placed_sql, connection, params={"start_date": start_date, "end_date": end_date})
            print("Querying shifts cancelled...")
            cancelled_df = pd.read_sql(cancelled_sql, connection, params={"start_date": start_date, "end_date": end_date})

        if placed_df.empty:
            print("No data found.")
            return

        placed_df['event_date'] = pd.to_datetime(placed_df['event_date'])
        if not cancelled_df.empty:
            cancelled_df['event_date'] = pd.to_datetime(cancelled_df['event_date'])
            merged_df = pd.merge(placed_df, cancelled_df, on=["event_date", "client_id"], how="left")
            merged_df["total_client_cancellations"] = merged_df["total_client_cancellations"].fillna(0).astype(int)
        else:
            merged_df = placed_df.copy()
            merged_df["total_client_cancellations"] = 0

        merged_df["total_shifts_placed"] = merged_df["total_shifts_placed"].fillna(0).astype(int)
        
        # We want to group by week AND client
        # Resample requires a datetime index
        merged_df = merged_df.set_index('event_date')
        
        # Group by client_name, then resample by week for each group
        # It's better to use pd.Grouper for this multi-column grouping
        weekly_df = merged_df.groupby([
            'client_id',
            'client_name',
            pd.Grouper(freq='W-MON', closed='left', label='left')
        ]).agg({
            'total_shifts_placed': 'sum',
            'total_client_cancellations': 'sum'
        }).reset_index()

        weekly_df['week_of'] = weekly_df['event_date'].dt.strftime('%Y-%m-%d')
        
        def calc_percentage(row):
            if row["total_shifts_placed"] > 0:
                return round((row["total_client_cancellations"] / row["total_shifts_placed"]) * 100, 2)
            else:
                return 0.0

        weekly_df["cancellation_percentage"] = weekly_df.apply(calc_percentage, axis=1)

        # Output columns
        final_df = weekly_df[["client_name", "week_of", "total_shifts_placed", "total_client_cancellations", "cancellation_percentage"]]
        
        # Sort by client_name then week
        final_df = final_df.sort_values(by=["client_name", "week_of"])

        output_path = "cancellation_report_since_2025.csv"
        final_df.to_csv(output_path, index=False)
        print(f"Report generated successfully: {output_path}")

    except Exception as e:
        print(f"ERROR: {str(e)}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    generate_report()
