import os
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

import pandas as pd
from sqlalchemy import text
import sys

# Add project root to sys.path so we can import from apps
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from apps.position_requests.scheduler import _engine

def process_updates():
    try:
        df = pd.read_excel("Update_level_2.xlsx")
    except PermissionError:
        print("Error: The file 'Update_level_2.xlsx' is currently open. Please close it and try again.")
        return
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # Look for employee ID column and 'new qualified for' column
    emp_id_col = None
    qual_col = None
    
    for col in df.columns:
        col_str = str(col).lower().strip()
        if 'employee id' in col_str or 'employee_id' in col_str or col_str == 'id':
            emp_id_col = col
        if 'new qualified for' in col_str or 'qualified for' in col_str:
            qual_col = col
            
    if not emp_id_col or not qual_col:
        print(f"Could not find required columns. Found columns: {df.columns.tolist()}")
        return
        
    engine = _engine()
    
    try:
        with engine.connect() as conn:
            for index, row in df.iterrows():
                emp_id = row[emp_id_col]
                if pd.isna(emp_id):
                    continue
                    
                try:
                    emp_id = int(emp_id)
                except ValueError:
                    continue # Skip if not a valid ID
                    
                new_qual = str(row[qual_col]).strip()
                
                if new_qual.lower() == 'nan' or not new_qual:
                    continue
                    
                # Parse positions
                new_qual = new_qual.replace(' and ', ',')
                positions = [p.strip() for p in new_qual.split(',') if p.strip()]
                    
                for pos_name in positions:
                    if not pos_name:
                        continue
                        
                    # Find position_id
                    pos_sql = text("""
                        SELECT position_id, description FROM position
                        WHERE LOWER(description) = :pos_name
                        LIMIT 1
                    """)
                    pos_res = conn.execute(pos_sql, {"pos_name": pos_name.lower()}).fetchone()
                    
                    if not pos_res:
                        # Try fuzzy match
                        pos_sql = text("""
                            SELECT position_id, description FROM position
                            WHERE LOWER(description) LIKE :pos_name
                            LIMIT 1
                        """)
                        pos_res = conn.execute(pos_sql, {"pos_name": f"%{pos_name.lower()}%"}).fetchone()
                        
                    if not pos_res:
                        print(f"Warning: Position '{pos_name}' not found for Employee {emp_id}")
                        continue
                        
                    position_id = pos_res[0]
                    
                    # Check if employee has that position
                    emp_pos_sql = text("""
                        SELECT employee_position_id FROM employee_position
                        WHERE employee_id = :employee_id AND position_id = :position_id
                        LIMIT 1
                    """)
                    emp_pos_res = conn.execute(emp_pos_sql, {"employee_id": emp_id, "position_id": position_id}).fetchone()
                    
                    if emp_pos_res:
                        # Update existing
                        update_sql = text("""
                            UPDATE employee_position
                            SET eligible = 1, status = 1
                            WHERE employee_position_id = :employee_position_id
                        """)
                        conn.execute(update_sql, {"employee_position_id": emp_pos_res[0]})
                        print(f"Updated: Employee {emp_id} - {pos_res[1]} (Eligible: Yes)")
                    else:
                        # Insert new
                        insert_sql = text("""
                            INSERT INTO employee_position (employee_id, position_id, eligible, status, sub_type_id)
                            VALUES (:employee_id, :position_id, 1, 1, -1)
                        """)
                        conn.execute(insert_sql, {"employee_id": emp_id, "position_id": position_id})
                        print(f"Added: Employee {emp_id} - {pos_res[1]} (Eligible: Yes)")
            conn.commit()
            print("Successfully processed all updates.")
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        engine.dispose()
        
if __name__ == "__main__":
    process_updates()
