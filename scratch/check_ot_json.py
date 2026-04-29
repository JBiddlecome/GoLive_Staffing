import os
import pandas as pd
import json
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

q = """
SELECT se.shift_employee_id, se.overtime, t.client_seconds, t.employee_seconds, t.use_sheet
FROM shift_employee se
JOIN event e ON se.event_id = e.event_id
LEFT JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
WHERE e.date >= '2026-04-13' AND e.date <= '2026-04-19'
  AND e.client_id = 1785
  AND se.overtime IS NOT NULL AND se.overtime != '' AND se.overtime != 'null'
ORDER BY se.shift_employee_id
"""
df = pd.read_sql(q, engine)
print(f"Shifts with OT JSON: {len(df)}")
for _, row in df.iterrows():
    try:
        ot_data = json.loads(str(row['overtime']))
        paid_by = ot_data.get('paidBy', 'N/A')
        overtimes = ot_data.get('overtimes', {})
        print(f"\nSE {row['shift_employee_id']} [use_sheet={row['use_sheet']}, c_sec={row['client_seconds']}, e_sec={row['employee_seconds']}]:")
        print(f"  paidBy={paid_by}")
        for role, entries in overtimes.items():
            for entry in entries:
                print(f"  {role}: type={entry.get('type')}, hours={entry.get('hours')}, rate={entry.get('rate')}")
    except Exception as ex:
        print(f"SE {row['shift_employee_id']}: Error {ex}")
