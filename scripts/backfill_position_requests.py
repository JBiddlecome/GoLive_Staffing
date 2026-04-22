import asyncio
import sys
import os
from dotenv import load_dotenv

# Load env vars
env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

# Add root to python path
sys.path.insert(0, os.path.abspath('.'))

from apps.position_requests.scheduler import load_records, save_records, evaluate_candidate

async def main():
    records = load_records()
    updated = False
    
    print(f"Total records found: {len(records)}")
    
    for idx, record in enumerate(records):
        # Target records with the DB connection error
        ai_analysis = record.get("ai_analysis", "")
        if "Database error during checking" in ai_analysis:
            print(f"Fixing record {idx+1}/{len(records)}: {record.get('employee_name', 'Unknown')}")
            
            phone = record.get('phone', '')
            resume_link = record.get('resume_link', '')
            experience = record.get('experience', '')
            positions = record.get('positions', '')
            
            try:
                status, new_analysis = await evaluate_candidate(phone, resume_link, experience, positions)
                record["status"] = status
                record["ai_analysis"] = new_analysis
                updated = True
                print(f"  -> Fixed Result: {status}")
                
                # Save intermittently to avoid losing progress
                save_records(records)
                
            except Exception as e:
                print(f"  -> Error: {str(e)}")
                
    if updated:
        save_records(records)
        print("Finished fixing all erroneous records.")
    else:
        print("No erroneous records found to fix.")

if __name__ == "__main__":
    asyncio.run(main())
