import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

# Check DMC client MSP and billing type details
q = """
SELECT c.client_id, c.name, c.msp_id, m.name AS msp_name, m.rate AS msp_rate, 
       c.billing_type_id, bt.name AS billing_type_name
FROM client c
LEFT JOIN msp m ON c.msp_id = m.id
LEFT JOIN billing_type bt ON c.billing_type_id = bt.id
WHERE c.client_id = 1785
"""
df = pd.read_sql(q, engine)
print("DMC client details:")
print(df.to_string())

# Check the msp table structure
q2 = "DESCRIBE msp"
df2 = pd.read_sql(q2, engine)
print("\nMSP table structure:")
print(df2.to_string())
