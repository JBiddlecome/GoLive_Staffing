import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

# Use standard pymysql URL
url = "mysql+pymysql://cstaffing:nWryUxSbGD@127.0.0.1:3307/cstaffing_live"
engine = create_engine(url)

tables = [
    'venue', 
    'dnr', 
    'exclusive', 
    'venue_document', 
    'venue_contact', 
    'venue_position', 
    'venue_attestation'
]

with engine.connect() as conn:
    for t in tables:
        try:
            res = conn.execute(text(f"SHOW CREATE TABLE {t}")).fetchone()
            print(f"--- Schema for {t} ---")
            print(res[1])
            print("\n")
        except Exception as e:
            print(f"Error getting schema for {t}: {e}")
