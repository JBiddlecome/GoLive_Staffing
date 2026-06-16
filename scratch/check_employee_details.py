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
        print("--- EMPLOYEE DETAILS ---")
        sql = text("""
            SELECT employee_id, payroll_id, first_name, last_name
            FROM employee
            WHERE employee_id IN (15106, 34962)
        """)
        rows = conn.execute(sql).fetchall()
        for r in rows:
            print(f"Emp ID: {r[0]} | Payroll ID: {repr(r[1])} | Name: {r[2]} {r[3]}")

if __name__ == "__main__":
    main()
