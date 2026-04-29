import os, pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv(r'C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env')
url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:3306/{os.getenv('DB_NAME')}"
engine = create_engine(url)
df = pd.read_sql("SELECT client_id, name FROM client WHERE name LIKE '%%Vibiana%%'", engine)
print(df.to_string())
