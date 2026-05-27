import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.getenv("DB_HOST") is None and os.path.exists(env_path):
    load_dotenv(env_path)

def test_refined_fallback():
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))

    db_url = URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name,
    )
    engine = create_engine(db_url, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            # Query refined list
            sql = text("""
                SELECT DISTINCT email FROM user 
                WHERE email IS NOT NULL 
                  AND email LIKE '%@%'
                  AND email NOT LIKE '[DELETED]%'
                  AND email NOT LIKE '%[deleted]%'
                  AND (
                    `group` IN ('ADMIN', 'OWNER') 
                    OR email LIKE '%@culinarystaffing.com' 
                    OR email LIKE '%@culinarymanager.com'
                  )
            """)
            res = conn.execute(sql)
            emails = [row[0].strip().lower() for row in res.fetchall()]
            
            print(f"Total refined mailbox addresses: {len(emails)}")
            print("Sample mailboxes:")
            for email in sorted(emails)[:20]:
                print(f" - {email}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    test_refined_fallback()
