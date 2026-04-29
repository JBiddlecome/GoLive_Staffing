import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

# Check if client_multiplier table exists and DMC's multipliers
q = "SHOW TABLES LIKE 'client_multiplier'"
df = pd.read_sql(q, engine)
print("client_multiplier table:", df.to_string())

if len(df) > 0:
    q2 = "SELECT * FROM client_multiplier WHERE client_id = 1785"
    df2 = pd.read_sql(q2, engine)
    print("\nDMC multipliers:")
    print(df2.to_string())
    
    # Also check all clients with multipliers
    q3 = "SELECT * FROM client_multiplier LIMIT 10"
    df3 = pd.read_sql(q3, engine)
    print("\nSample multipliers:")
    print(df3.to_string())
