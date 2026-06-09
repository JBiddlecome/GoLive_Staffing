from dotenv import load_dotenv
load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

from apps.position_requests.scheduler import _engine
from sqlalchemy import text

engine = _engine()
with engine.connect() as conn:
    print("--- Users matching 'jake' ---")
    users = conn.execute(text("SELECT id, username, email FROM user WHERE email LIKE '%jake%' OR username LIKE '%jake%' OR email LIKE '%biddlecome%'")).fetchall()
    for u in users:
        print(u)
