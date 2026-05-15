import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

def _db_url_from_env():
    host = os.getenv("REPORTABLE_DB_HOST") or os.getenv("DB_HOST", "127.0.0.1")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    port = int(os.getenv("REPORTABLE_DB_PORT") or os.getenv("DB_PORT", "3306"))
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"

engine = create_engine(_db_url_from_env())

file_path = "scratch/temp_list.xlsx"
df = pd.read_excel(file_path)

print(f"Loaded {len(df)} rows from {file_path}")

if 'client_id' not in df.columns:
    print("Error: client_id column not found.")
    exit(1)

client_ids = df['client_id'].dropna().astype(int).unique().tolist()
print(f"Found {len(client_ids)} unique client_ids to update.")

with engine.begin() as conn:
    if not client_ids:
        print("No client_ids to update.")
        exit(0)
    
    in_clause = ",".join(map(str, client_ids))
    
    res = conn.execute(text(f"SELECT COUNT(*) FROM client WHERE client_id IN ({in_clause}) AND sales_executive_id IS NOT NULL")).scalar()
    print(f"Clients with sales_executive_id before update: {res}")
    
    stmt = text(f"UPDATE client SET sales_executive_id = NULL WHERE client_id IN ({in_clause})")
    update_res = conn.execute(stmt)
    
    print(f"Updated {update_res.rowcount} rows in client table.")
    
    res_after = conn.execute(text(f"SELECT COUNT(*) FROM client WHERE client_id IN ({in_clause}) AND sales_executive_id IS NOT NULL")).scalar()
    print(f"Clients with sales_executive_id after update: {res_after}")
