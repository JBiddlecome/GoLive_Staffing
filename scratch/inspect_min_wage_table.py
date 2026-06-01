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
    if "min_wage_rate_amount" in inspector.get_table_names():
        columns = [col["name"] for col in inspector.get_columns("min_wage_rate_amount")]
        print("Columns in min_wage_rate_amount:", columns)
    else:
        print("min_wage_rate_amount table not found. Available tables:", [t for t in inspector.get_table_names() if "wage" in t.lower() or "min" in t.lower()])
        
except Exception as e:
    print("Error:", e)
finally:
    engine.dispose()
