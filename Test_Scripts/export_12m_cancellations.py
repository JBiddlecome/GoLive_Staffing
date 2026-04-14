import os
import pandas as pd
from dotenv import load_dotenv
load_dotenv(r'C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env')
from sqlalchemy import create_engine, text

def main():
    db_user = os.getenv("DB_USER", "root")
    db_password = os.getenv("DB_PASSWORD", "")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "cstaffing_live")
    
    url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(url)

    sql = text("""
        WITH target_cancellations AS (
            SELECT 
                se.shift_position_id,
                se.shift_employee_id,
                se.cancelled_at,
                s.start as shift_start,
                e.client_id,
                TIMESTAMPDIFF(SECOND, se.cancelled_at, s.start) as seconds_before_start
            FROM shift_employee se
            JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            JOIN shift s ON sp.shift_id = s.shift_id
            JOIN event e ON s.event_id = e.event_id
            WHERE se.cancel_reason = '2'
              AND e.deleted_at IS NULL
              AND s.deleted_at IS NULL
              AND e.date >= DATE_SUB(LAST_DAY(NOW()), INTERVAL 12 MONTH)
        ),
        refills AS (
            SELECT 
                tc.shift_employee_id as cancelled_employee_id,
                MIN(se_new.confirmed_at) as refill_confirmed_at
            FROM target_cancellations tc
            JOIN shift_employee se_new ON tc.shift_position_id = se_new.shift_position_id
            WHERE se_new.confirmed = 1
              AND (se_new.cancel_reason = '0' OR se_new.cancel_reason IS NULL OR se_new.cancel_reason = '')
              AND se_new.confirmed_at > tc.cancelled_at
            GROUP BY tc.shift_employee_id
        ),
        cancellation_stats AS (
            SELECT 
                tc.client_id,
                tc.shift_employee_id,
                tc.seconds_before_start,
                r.refill_confirmed_at,
                TIMESTAMPDIFF(SECOND, tc.cancelled_at, r.refill_confirmed_at) as refill_seconds
            FROM target_cancellations tc
            LEFT JOIN refills r ON tc.shift_employee_id = r.cancelled_employee_id
        )
        SELECT 
            c.name AS `Client Name`,
            COUNT(cs.shift_employee_id) AS `Total <24 Hr Cancellations`,
            ROUND(AVG(cs.seconds_before_start) / 3600.0, 2) AS `Avg Time Cancelled Before Start (Hours)`,
            ROUND(AVG(cs.refill_seconds) / 3600.0, 2) AS `Avg Time to Refill (Hours)`,
            SUM(CASE WHEN cs.refill_confirmed_at IS NULL THEN 1 ELSE 0 END) AS `Total Shifts Never Refilled`
        FROM client c
        JOIN cancellation_stats cs ON c.client_id = cs.client_id
        WHERE c.deleted_at IS NULL
        GROUP BY c.client_id, c.name
        ORDER BY `Total <24 Hr Cancellations` DESC
    """)
    
    with engine.begin() as conn:
        df = pd.read_sql(sql, conn)
        
    output_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\All_Clients_12_Month_Cancellations.xlsx"
    df.to_excel(output_path, index=False, engine='openpyxl')
    print(f"Data exported successfully to {output_path}")

if __name__ == "__main__":
    main()
