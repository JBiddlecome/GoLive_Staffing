import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = (
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(url)

START_DATE = "2026-05-25"

query = text("""
    SELECT 
        eow.id,
        eow.created_at,
        eow.non_work_hours,
        eow.employee_id,
        e.payroll_id,
        e.first_name,
        e.last_name
    FROM employee_other_work eow
    JOIN employee e ON eow.employee_id = e.employee_id
    WHERE eow.other_work_type_id = 8
      AND DATE(eow.created_at) >= :s
    ORDER BY eow.created_at DESC
""")

with engine.begin() as conn:
    df = pd.read_sql(query, conn, params={"s": START_DATE})
    print(f"Number of sick pay requests since {START_DATE}: {len(df)}")
    if len(df) > 0:
        print(df.head(10).to_string())
    else:
        print("No records found in this range.")
