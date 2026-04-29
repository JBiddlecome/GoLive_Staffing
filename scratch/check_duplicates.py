import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

with engine.begin() as conn:
    ts_cols = [
        "use_sheet", "client_seconds", "employee_seconds",
        "client_min_bill", "employee_min_pay",
        "client_no_bill", "employee_no_pay",
        "client_no_break_penalty", "employee_no_break_penalty",
        "client_tips", "client_parking", "client_travel", "client_service_charge",
        "employee_tips", "employee_parking", "employee_travel", "employee_service_charge"
    ]
    ts_select_str = ", ".join(f"t.{col}" for col in ts_cols)
    
    sql = text(f"""
    SELECT
        e.date,
        sp.bonus,
        c.name AS client_name,
        c.client_id,
        se.shift_employee_id,
        se.bill_rate,
        se.rate AS pay_rate,
        se.overtime,
        v.service_charge AS venue_service_charge,
        wc.rate AS wc_rate,
        e.state AS event_state,
        t.client_worked,
        t.employee_worked,
        s.start AS shift_start,
        t.client_start,
        t.employee_start,
        {ts_select_str}
    FROM shift_employee se
    JOIN event e ON se.event_id = e.event_id
    JOIN client c ON e.client_id = c.client_id
    LEFT JOIN msp m ON c.msp_id = m.id
    LEFT JOIN wc_code wc ON c.wc_id = wc.wc_id
    LEFT JOIN venue v ON e.venue_id = v.venue_id
    LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
    LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
    LEFT JOIN shift s ON sp.shift_id = s.shift_id
    WHERE e.date >= :start_date AND e.date <= :end_date
      AND (
          (se.confirmed = 1 AND se.cancel_reason = 0)
          OR t.client_min_bill = 1
          OR t.employee_min_pay = 1
      )
    """)
    
    df = pd.read_sql(sql, conn, params={"start_date": "2026-04-13", "end_date": "2026-04-19"})
    
print(f"Total rows in df: {len(df)}")

# Count Vibiana rows
vibiana = df[df['client_name'] == 'Vibiana Events and Redbird Events']
print(f"Vibiana rows: {len(vibiana)}")
print(vibiana[['shift_employee_id', 'use_sheet', 'client_seconds', 'employee_seconds', 'client_worked', 'employee_worked']].to_string())
print()

# Check if there are duplicate shift_employee_ids for Vibiana
dup = vibiana[vibiana.duplicated('shift_employee_id', keep=False)]
if len(dup) > 0:
    print("DUPLICATE shift_employee_ids found!")
    print(dup[['shift_employee_id','client_name']].to_string())
else:
    print("No duplicates found")
