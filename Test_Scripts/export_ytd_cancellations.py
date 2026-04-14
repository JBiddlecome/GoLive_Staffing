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
        SELECT 
            c.name AS `Client Name`,
            CONCAT(emp.first_name, ' ', emp.last_name) AS `Employee Name`,
            se.cancelled_at AS `Datetime of Cancellation`,
            DATE_FORMAT(se.cancelled_at, '%H:%i:%s') AS `Time of Cancellation`
        FROM shift_employee se
        JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
        JOIN shift s ON sp.shift_id = s.shift_id
        JOIN event e ON s.event_id = e.event_id
        JOIN client c ON e.client_id = c.client_id
        JOIN employee emp ON se.employee_id = emp.employee_id
        WHERE se.cancel_reason = '2'
          AND YEAR(se.cancelled_at) = YEAR(CURDATE())
          AND e.deleted_at IS NULL
          AND s.deleted_at IS NULL
        ORDER BY se.cancelled_at DESC
    """)
    
    with engine.begin() as conn:
        df = pd.read_sql(sql, conn)
        
    output_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\YTD_Cancellations_Report.xlsx"
    df.to_excel(output_path, index=False, engine='openpyxl')
    print(f"Data exported successfully to {output_path}")

if __name__ == "__main__":
    main()
