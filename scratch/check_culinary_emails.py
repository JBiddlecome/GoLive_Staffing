import os
import sys
from sqlalchemy import text
from pathlib import Path
from dotenv import load_dotenv

# Load .env from outside the repo
env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    print(f"Warning: .env not found at {env_path}")

# Add project root to sys.path
sys.path.append(str(Path(__file__).parent.parent.parent))

from apps.position_requests.scheduler import _engine

def check_email():
    engine = _engine()
    with engine.connect() as conn:
        print("Checking for 'mail@culinarystaffing.com'...")
        sql1 = text("SELECT cc.email, c.name as client_name, c.status, c.deleted_at FROM client_contact cc JOIN client c ON cc.client_id = c.client_id WHERE cc.email = 'mail@culinarystaffing.com'")
        res1 = conn.execute(sql1).fetchall()
        if res1:
            for row in res1:
                print(f"Found match: {row.email} | Client: {row.client_name} | Status: {row.status} | Deleted: {row.deleted_at}")
        else:
            print("No exact match for 'mail@culinarystaffing.com'")

        print("\nChecking for any email ending in '@culinarystaffing.com'...")
        sql2 = text("SELECT cc.email, c.name as client_name, c.status, c.deleted_at FROM client_contact cc JOIN client c ON cc.client_id = c.client_id WHERE cc.email LIKE '%@culinarystaffing.com'")
        res2 = conn.execute(sql2).fetchall()
        if res2:
            for row in res2:
                print(f"Found domain match: {row.email} | Client: {row.client_name} | Status: {row.status} | Deleted: {row.deleted_at}")
        else:
            print("No domain matches for '@culinarystaffing.com'")

if __name__ == "__main__":
    check_email()
