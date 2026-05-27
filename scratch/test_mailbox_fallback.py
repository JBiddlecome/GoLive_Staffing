import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

def test_fallback():
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))

    if host in {"127.0.0.1", "localhost"} and not reportable_host:
        tunnel_port = os.getenv("LOCAL_TUNNEL_PORT")
        rds_host = os.getenv("RDS_HOST")
        if rds_host and (not tunnel_port or str(port) != tunnel_port):
            host = rds_host

    print(f"Connecting to host={host}, port={port}, name={name}, user={user}")
    
    if not all([host, user, password]):
        print("Credentials missing!")
        return

    db_url = URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name,
    )
    engine = create_engine(db_url, pool_pre_ping=True)
    mailboxes = set()
    try:
        with engine.connect() as conn:
            # Query active employees
            emp_res = conn.execute(text("SELECT email FROM employee WHERE deleted_at IS NULL AND status = 1 AND email IS NOT NULL"))
            emp_count = 0
            for row in emp_res.fetchall():
                email = row[0]
                if email and "@" in email and not email.strip().startswith("#"):
                    mailboxes.add(email.strip().lower())
                    emp_count += 1
            print(f"Found {emp_count} active employees from employee table.")
            
            # Query users (admins/recruiters/staff)
            usr_res = conn.execute(text("SELECT email FROM user WHERE email IS NOT NULL"))
            usr_count = 0
            for row in usr_res.fetchall():
                email = row[0]
                if email and "@" in email and not email.strip().startswith("#"):
                    mailboxes.add(email.strip().lower())
                    usr_count += 1
            print(f"Found {usr_count} users from user table.")
            
            print(f"Total unique mailbox addresses: {len(mailboxes)}")
            print("Sample mailboxes:")
            for email in list(mailboxes)[:10]:
                print(f" - {email}")
    except Exception as e:
        print(f"Error querying fallback: {str(e)}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    test_fallback()
