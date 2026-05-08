import sys
from sqlalchemy import create_engine, text
sys.path.append(r'c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing')
from scripts.find_event_supervisors import _get_db_url
engine = create_engine(_get_db_url())
with engine.connect() as conn:
    tables = conn.execute(text("SHOW TABLES LIKE '%position%'")).fetchall()
    print('Position tables:', tables)
    if tables and ('position',) in tables:
        pos = conn.execute(text('SELECT * FROM position LIMIT 5')).fetchall()
        print('Positions:', pos)
    
    emp_pos_tables = conn.execute(text("SHOW TABLES LIKE '%employee_position%'")).fetchall()
    print('Employee Position tables:', emp_pos_tables)
    if emp_pos_tables and ('employee_position',) in emp_pos_tables:
        emp_pos = conn.execute(text('SELECT * FROM employee_position LIMIT 5')).fetchall()
        print('Employee Positions:', emp_pos)
