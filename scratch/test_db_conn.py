import os
import pymysql
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)
    print("Loaded env from", env_path)
else:
    print("Env path does not exist:", env_path)

db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

print("Loaded credentials:")
print(f"  DB_USER: {db_user}")
print(f"  DB_PASSWORD: {repr(db_pass)}")
print(f"  DB_HOST: {db_host}")
print(f"  DB_PORT: {db_port}")
print(f"  DB_NAME: {db_name}")

try:
    print("Attempting to connect...")
    conn = pymysql.connect(
        host=db_host or "127.0.0.1",
        port=int(db_port) if db_port else 3307,
        user=db_user,
        password=db_pass,
        database=db_name,
        connect_timeout=5
    )
    print("Success! Connection established.")
    conn.close()
except Exception as e:
    print("Failed to connect!")
    print(e)
