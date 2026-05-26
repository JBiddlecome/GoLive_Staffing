import sys
import os
import time
import subprocess
import pandas as pd
from sqlalchemy import text

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

env_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'golive-staffing-tools.env')
if os.path.exists(env_file):
    with open(env_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                parts = line.split('=', 1)
                if len(parts) == 2:
                    os.environ[parts[0]] = parts[1]

pem_key = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'golive-bastion-key.pem')
bastion_host = os.environ.get('BASTION_HOST')
bastion_user = os.environ.get('BASTION_USER')
rds_host = os.environ.get('RDS_HOST')
local_port = os.environ.get('DB_PORT', '3307')

tunnel_proc = None
if bastion_host:
    tunnel_cmd = [
        'ssh', '-o', 'StrictHostKeyChecking=no',
        '-o', 'ExitOnForwardFailure=yes',
        '-N', '-L', f'127.0.0.1:{local_port}:{rds_host}:3306',
        '-i', pem_key,
        f'{bastion_user}@{bastion_host}'
    ]
    tunnel_proc = subprocess.Popen(tunnel_cmd)
    time.sleep(5)

from apps.client_notifications.utils import _engine

client_id = 1210
start_date = '2026-04-01'
end_date = '2026-04-30'

engine = _engine()

sql_test = text("""
    SELECT 
        t.timesheet_id,
        t.employee_id,
        e.date as event_date,
        s.start as shift_start,
        t.employee_worked,
        t.client_worked,
        t.use_sheet,
        t.employee_seconds / 3600.0 as employee_hours,
        t.client_seconds / 3600.0 as client_hours,
        CASE 
            WHEN t.use_sheet = 'EMPLOYEE' THEN t.employee_seconds
            WHEN t.use_sheet = 'CLIENT' THEN t.client_seconds
            ELSE t.client_seconds
        END / 3600.0 as app_calculated_hours
    FROM timesheet t
    JOIN shift_employee se ON t.shift_employee_id = se.shift_employee_id
    JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
    JOIN shift s ON sp.shift_id = s.shift_id
    JOIN position p ON sp.position_id = p.position_id
    JOIN event e ON t.event_id = e.event_id
    LEFT JOIN dnr d ON t.employee_id = d.employee_id AND d.client_id = :client_id
    WHERE e.client_id = :client_id
      AND e.date BETWEEN :start_date AND :end_date
      AND p.description = 'Server 2'
      AND d.employee_id IS NULL
""")

try:
    with engine.begin() as conn:
        df = pd.read_sql(sql_test, conn, params={
            "client_id": client_id, 
            "start_date": start_date, 
            "end_date": end_date
        })

    print("=== TIMESHEETS FOR SERVER 2 at Client 1210 (April) ===")
    print(df.to_string())
    print(f"\nSum of app_calculated_hours (only where WORKED): {df[df['employee_worked'] == 'WORKED']['app_calculated_hours'].sum()}")
    print(f"Sum of app_calculated_hours (all): {df['app_calculated_hours'].sum()}")
    print(f"Sum of employee_hours: {df['employee_hours'].sum()}")
    print(f"Sum of client_hours: {df['client_hours'].sum()}")

finally:
    if tunnel_proc:
        tunnel_proc.terminate()
