import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

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
    inspector = inspect(engine)
    for table in ["client", "venue"]:
        columns = [col["name"] for col in inspector.get_columns(table)]
        print(f"Columns in {table}:", [c for c in columns if "lock" in c.lower() or "rate" in c.lower()])
        
except Exception as e:
    print("Error:", e)
finally:
    engine.dispose()
