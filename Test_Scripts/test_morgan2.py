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
    sql = text('''
        SELECT 
            e.first_name,
            e.last_name,
            ev.date AS event_date,
            ev.title AS event_title,
            c.name AS client_name,
            s.start AS shift_start,
            s.end AS shift_end,
            p.description AS position,
            sp.rate AS pay_rate,
            pe.created_on AS published_at
        FROM 
            publish_employee pe
            INNER JOIN employee e 
                ON e.employee_id = pe.employee_id
            INNER JOIN shift_position sp 
                ON sp.shift_position_id = pe.shift_position_id
            INNER JOIN shift s 
                ON s.shift_id = sp.shift_id
            INNER JOIN event ev 
                ON ev.event_id = pe.event_id
            INNER JOIN client c 
                ON c.client_id = ev.client_id
            INNER JOIN position p 
                ON p.position_id = sp.position_id
        WHERE 
            pe.employee_id = :employee_id
            AND ev.date BETWEEN :start_date AND :end_date
        ORDER BY 
            ev.date, s.start;
    ''')
    df = pd.read_sql(sql, conn, params={"employee_id": 14700, "start_date": "2026-01-01", "end_date": "2026-12-31"})
    print(df.to_string())
