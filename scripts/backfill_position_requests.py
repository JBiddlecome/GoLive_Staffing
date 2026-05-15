import asyncio
import sys
import os
from dotenv import load_dotenv
from sqlalchemy import text

# Load env vars
env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

# Add root to python path
sys.path.insert(0, os.path.abspath('.'))

from apps.position_requests.scheduler import evaluate_candidate, _engine

async def main():
    engine = _engine()
    
    try:
        with engine.connect() as conn:
            # status = 10 corresponds to "Inactive 60 days"
            emp_sql = text("""
                SELECT 
                    employee_id, 
                    COALESCE(mobile, home, work) as phone, 
                    resume, 
                    CONCAT(first_name, ' ', last_name) as name 
                FROM employee 
                WHERE status = 10 
                  AND resume IS NOT NULL 
                  AND resume != ''
            """)
            employees = conn.execute(emp_sql).fetchall()
            
            print(f"Found {len(employees)} inactive (60) employees with resumes.")
            
            for emp in employees:
                emp_id = emp.employee_id
                phone = emp.phone or ''
                resume_link = emp.resume
                name = emp.name
                
                # Check their existing eligible positions for Server 2 (115), Prep Cook 2 (110), Cook 2 (102)
                pos_sql = text("""
                    SELECT position_id
                    FROM employee_position
                    WHERE employee_id = :emp_id
                      AND position_id IN (115, 110, 102)
                      AND status = 1
                      AND eligible = 1
                """)
                existing_positions = conn.execute(pos_sql, {"emp_id": emp_id}).fetchall()
                
                already_eligible_ids = {row.position_id for row in existing_positions}
                
                id_to_name = {
                    115: "Server 2",
                    110: "Prep Cook 2",
                    102: "Cook 2"
                }
                
                needed = [name for pid, name in id_to_name.items() if pid not in already_eligible_ids]
                
                if not needed:
                    print(f"Skipping {name} (ID: {emp_id}): Already eligible for all target positions.")
                    continue
                    
                requested_positions = ", ".join(needed)
                
                print(f"Evaluating {name} (ID: {emp_id}) for: {requested_positions}")
                
                # Experience text is empty as we only have the resume in the DB
                try:
                    status, analysis = await evaluate_candidate(phone, resume_link, "", requested_positions)
                    print(f"  -> Result: {status}")
                    # TODO: add logic here to update the database based on the result if needed
                    
                except Exception as e:
                    print(f"  -> Error: {str(e)}")
                
    finally:
        engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
