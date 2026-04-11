import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

def _db_url_from_env():
    host = os.getenv("DB_HOST", "127.0.0.1")
    name = os.getenv("DB_NAME", "cstaffing")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    port = int(os.getenv("DB_PORT", "3306"))
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"

engine = create_engine(_db_url_from_env())

with engine.connect() as conn:
    df_morgan = pd.read_sql(text("SELECT employee_id, first_name, last_name, status, email FROM employee WHERE first_name LIKE '%Morgan%' AND last_name LIKE '%Ziff%'"), conn)
    print("Morgan Ziff:")
    print(df_morgan.to_string())

    if not df_morgan.empty:
        morgan_id = df_morgan.iloc[0]['employee_id']
        print(f"\nLooking for publish_employee for {morgan_id}")
        df_pe = pd.read_sql(text(f"SELECT * FROM publish_employee WHERE employee_id = {morgan_id} LIMIT 5"), conn)
        print("publish_employee:")
        print(df_pe.to_string())
        
        # Test the query
        sql = text('''
            SELECT 
                ev.date AS event_date,
                s.start AS shift_start,
                s.end AS shift_end
            FROM 
                publish_employee pe
                INNER JOIN employee e ON e.employee_id = pe.employee_id
                INNER JOIN shift_position sp ON sp.shift_position_id = pe.shift_position_id
                INNER JOIN shift s ON s.shift_id = sp.shift_id
                INNER JOIN event ev ON ev.event_id = pe.event_id
                INNER JOIN client c ON c.client_id = ev.client_id
                INNER JOIN position p ON p.position_id = sp.position_id
            WHERE 
                pe.employee_id = :employee_id
            LIMIT 5;
        ''')
        df_result = pd.read_sql(sql, conn, params={"employee_id": morgan_id})
        print("Shift query:")
        print(df_result.to_string())

    # Get sample publish_employee
    print("\nSample publish_employee records:")
    df_sample = pd.read_sql(text("SELECT * FROM publish_employee ORDER BY id DESC LIMIT 5"), conn)
    print(df_sample.to_string())

