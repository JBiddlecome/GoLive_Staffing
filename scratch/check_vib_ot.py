import os, pandas as pd, json
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv(r'C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env')
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT','3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)
df = pd.read_sql("SELECT shift_employee_id, overtime FROM shift_employee WHERE shift_employee_id IN (631164, 631235, 631237, 631298, 631304)", engine)
for _, row in df.iterrows():
    print(f"SE {row['shift_employee_id']}: overtime={repr(row['overtime'][:200] if row['overtime'] else None)}")
    if row['overtime']:
        data = json.loads(row['overtime'])
        print(f"  paidBy={data.get('paidBy')}")
        for role, entries in data.get('overtimes', {}).items():
            for e in entries:
                print(f"  {role}: type={e.get('type')}, hours={e.get('hours')}, rate={e.get('rate')}")
