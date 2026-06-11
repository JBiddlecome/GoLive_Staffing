import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

def _db_url_from_env() -> URL:
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))
    return URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name
    )

engine = create_engine(_db_url_from_env())

with engine.connect() as conn:
    # Query employees with flag = 2 or flag = 4
    sql = text("""
        SELECT employee_id, first_name, last_name, flag
        FROM employee
        WHERE flag IN (2, 4)
    """)
    res = conn.execute(sql).mappings().all()
    print("Employees with flag 2 or 4:")
    for r in res:
        print(f"ID: {r['employee_id']}, Name: {r['first_name']} {r['last_name']}, Flag: {r['flag']}")
        
        # Let's check employee_note to see if there is any note about their flag color
        note_sql = text("""
            SELECT type, datetime, note
            FROM employee_note
            WHERE employee_id = :emp_id
            ORDER BY datetime DESC
            LIMIT 5
        """)
        notes = conn.execute(note_sql, {"emp_id": r['employee_id']}).mappings().all()
        if notes:
            print("  Notes:")
            for n in notes:
                print(f"    [{n['datetime']}] ({n['type']}) {n['note'][:100]}")

