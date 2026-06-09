import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

def _db_url_from_env():
    host = os.getenv("DB_HOST", "127.0.0.1")
    name = os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    port = int(os.getenv("DB_PORT", "3306"))
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"

engine = create_engine(_db_url_from_env())
with engine.connect() as conn:
    # Query client info
    c_sql = text("SELECT client_id, name, staff_id FROM client WHERE client_id = 1618")
    client = conn.execute(c_sql).fetchone()
    print("Client:", client)
    
    # Query user (staff_id)
    if client and client.staff_id:
        u_sql = text("SELECT id, username, email FROM user WHERE id = :id")
        user = conn.execute(u_sql, {"id": client.staff_id}).fetchone()
        print("Client Staff User:", user)
        
    # Query venues for client 1618
    v_sql = text("SELECT venue_id, name, staffing_manager_id FROM venue WHERE client_id = 1618")
    venues = conn.execute(v_sql).fetchall()
    print("Venues:")
    for v in venues:
        print("  Venue:", v)
        if v.staffing_manager_id:
            vu_sql = text("SELECT id, username, email FROM user WHERE id = :id")
            vuser = conn.execute(vu_sql, {"id": v.staffing_manager_id}).fetchone()
            print("  Venue Staff Manager:", vuser)
