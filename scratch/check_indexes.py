import os
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

from sqlalchemy import text
from apps.position_requests.scheduler import _engine

def main():
    engine = _engine()
    with engine.connect() as conn:
        print("Indexes on history_entry:")
        rows = conn.execute(text("SHOW INDEX FROM history_entry")).fetchall()
        for r in rows:
            print(f"Table: {r[0]} | Non_unique: {r[1]} | Key_name: {r[2]} | Seq_in_index: {r[3]} | Column_name: {r[4]}")

if __name__ == "__main__":
    main()
