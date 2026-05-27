import os
import pymysql
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
load_dotenv(env_path)

try:
    connection = pymysql.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "cstaffing_live"),
        cursorclass=pymysql.cursors.DictCursor
    )
    
    with connection.cursor() as cursor:
        # Check if users exist in the user table
        cursor.execute("SELECT id, first_name, last_name, email FROM user WHERE id IN (1803, 36956)")
        users = cursor.fetchall()
        print("Users in DB:")
        for u in users:
            print(f"  - User ID: {u['id']}, Name: {u['first_name']} {u['last_name']}, Email: {u['email']}")
        
        # Check current count of venues with staffing_manager_id = 1803
        cursor.execute("SELECT COUNT(*) as count FROM venue WHERE staffing_manager_id = 1803")
        count_1803 = cursor.fetchone()['count']
        print(f"\nNumber of venues currently with staffing_manager_id = 1803: {count_1803}")
        
        # Check current count of venues with staffing_manager_id = 36956
        cursor.execute("SELECT COUNT(*) as count FROM venue WHERE staffing_manager_id = 36956")
        count_36956 = cursor.fetchone()['count']
        print(f"Number of venues currently with staffing_manager_id = 36956: {count_36956}")
                
except Exception as e:
    import traceback
    print("An error occurred:")
    print(traceback.format_exc())
finally:
    if 'connection' in locals() and connection.open:
        connection.close()
