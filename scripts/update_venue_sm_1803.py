import os
import pymysql
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
load_dotenv(env_path)

def main():
    connection = pymysql.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "cstaffing_live"),
        cursorclass=pymysql.cursors.DictCursor
    )
    
    try:
        with connection.cursor() as cursor:
            # 1. Pre-Check: count matching venues and verify managers
            cursor.execute("SELECT id, first_name, last_name, email FROM user WHERE id IN (1803, 36956)")
            users = cursor.fetchall()
            print("Staffing Managers verified:")
            for u in users:
                print(f"  - User ID: {u['id']}, Name: {u['first_name']} {u['last_name']}, Email: {u['email']}")
                
            cursor.execute("SELECT COUNT(*) as count FROM venue WHERE staffing_manager_id = 1803")
            count_1803 = cursor.fetchone()['count']
            print(f"\nPre-check: {count_1803} venues currently have staffing_manager_id = 1803")
            
            cursor.execute("SELECT COUNT(*) as count FROM venue WHERE staffing_manager_id = 36956")
            count_36956 = cursor.fetchone()['count']
            print(f"Pre-check: {count_36956} venues currently have staffing_manager_id = 36956")
            
            if count_1803 == 0:
                print("\nNo venues found with staffing_manager_id = 1803. Nothing to update.")
                return
            
            # 2. Perform Update
            print("\nUpdating venues...")
            update_query = "UPDATE venue SET staffing_manager_id = 36956 WHERE staffing_manager_id = 1803"
            cursor.execute(update_query)
            rows_updated = cursor.rowcount
            print(f"Database reported {rows_updated} rows updated.")
            
            # 3. Post-Check: verify update
            cursor.execute("SELECT COUNT(*) as count FROM venue WHERE staffing_manager_id = 1803")
            post_count_1803 = cursor.fetchone()['count']
            print(f"\nPost-check: {post_count_1803} venues currently have staffing_manager_id = 1803")
            
            cursor.execute("SELECT COUNT(*) as count FROM venue WHERE staffing_manager_id = 36956")
            post_count_36956 = cursor.fetchone()['count']
            print(f"Post-check: {post_count_36956} venues currently have staffing_manager_id = 36956")
            
            # 4. Commit transaction
            connection.commit()
            print("\nTransaction committed successfully.")
            
    except Exception as e:
        print("\nAn error occurred during update. Rolling back changes.")
        connection.rollback()
        raise e
    finally:
        connection.close()

if __name__ == "__main__":
    main()
