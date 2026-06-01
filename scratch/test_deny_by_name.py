import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load env
load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

# Build URL
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

name = "John Doe" # Modify this to test a real name in the DB if needed
print("Testing name lookup fallback logic...")

with engine.connect() as conn:
    # Let's count matching employees with active status (1, 3, 6, 10, 14)
    name_sql = text("""
        SELECT employee_id, first_name, last_name, email, status 
        FROM employee 
        WHERE CONCAT(first_name, ' ', last_name) = :name
          AND status IN (1, 3, 6, 10, 14)
    """)
    results = conn.execute(name_sql, {"name": name}).fetchall()
    
    print(f"Results for '{name}': {len(results)} match(es)")
    for r in results:
        print(f"ID: {r[0]}, Name: {r[1]} {r[2]}, Email: {r[3]}, Status: {r[4]}")
        
    if len(results) > 1:
        print("ALERT: More than one user found. Deny request should fail with the multi-user warning.")
    elif len(results) == 1:
        print("Success: Exactly one user found. Request would be processed successfully.")
    else:
        print("Notice: No matching employee found in active statuses.")
