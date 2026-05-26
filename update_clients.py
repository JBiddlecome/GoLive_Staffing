import os
import pandas as pd
import pymysql
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
load_dotenv(env_path)

# Connect to database
connection = pymysql.connect(
    host=os.getenv("DB_HOST", "127.0.0.1"),
    port=int(os.getenv("LOCAL_TUNNEL_PORT", 3307)),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    cursorclass=pymysql.cursors.DictCursor
)

excel_path = "client_auto_confirm_update_copy.xlsx"
df = pd.read_excel(excel_path)

print(f"Loaded {len(df)} rows from Excel.")
print(df.head())

updated_count = 0
with connection.cursor() as cursor:
    for index, row in df.iterrows():
        client_id = int(row['client_id'])
        new_auto_confirm = int(row['new_auto_confirm'])
        
        # Update query
        update_query = "UPDATE client SET auto_confirm = %s WHERE client_id = %s"
        cursor.execute(update_query, (new_auto_confirm, client_id))
        updated_count += cursor.rowcount

connection.commit()
connection.close()

print(f"Successfully updated {updated_count} client rows.")
