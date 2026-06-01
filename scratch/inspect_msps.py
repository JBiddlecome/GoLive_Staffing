import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

host = os.getenv("DB_HOST")
name = os.getenv("DB_NAME", "cstaffing_live")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
port = int(os.getenv("DB_PORT", "3306"))

engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}")

try:
    with engine.begin() as conn:
        print("--- MSP TABLE ---")
        rows = conn.execute(text("SELECT id, name, rate FROM msp")).mappings().fetchall()
        for r in rows:
            print(dict(r))
            
        print("\n--- CLIENTS WITH COMPASS IN NAME ---")
        clients = conn.execute(text("SELECT client_id, name, msp_id FROM client WHERE name LIKE '%Compass%'")).mappings().fetchall()
        for c in clients:
            print(dict(c))
            
        print("\n--- CLIENTS WITH MSP_ID IS NOT NULL (LIMIT 15) ---")
        clients_msp = conn.execute(text("SELECT client_id, name, msp_id FROM client WHERE msp_id IS NOT NULL LIMIT 15")).mappings().fetchall()
        for cm in clients_msp:
            print(dict(cm))
            
except Exception as e:
    print("Error:", e)
finally:
    engine.dispose()
