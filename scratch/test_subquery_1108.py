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

client_id = 1108
start_date = '2026-04-01'
end_date = '2026-04-30'

engine = _engine()

sql_test = text("""
    SELECT 
        timing_group,
        SUM(needed) as total_needed,
        SUM(LEAST(needed, actual_filled)) as total_filled
    FROM (
        SELECT 
            CASE WHEN TIMESTAMPDIFF(HOUR, sp.created_at, s.start) > 24 THEN 'over_24h' ELSE 'under_24h' END as timing_group,
            sp.count as needed,
            (
                SELECT COUNT(*) 
                FROM shift_employee se 
                WHERE se.shift_position_id = sp.shift_position_id 
                  AND se.deleted_at IS NULL 
                  AND se.cancel_reason = 0 
                  AND se.confirmed = 1
            ) as actual_filled
        FROM shift_position sp
        JOIN shift s ON sp.shift_id = s.shift_id
        JOIN event e ON s.event_id = e.event_id
        WHERE e.client_id = :client_id
          AND e.date BETWEEN :start_date AND :end_date
          AND sp.deleted_at IS NULL
    ) sub
    GROUP BY timing_group
""")

try:
    with engine.begin() as conn:
        df = pd.read_sql(sql_test, conn, params={
            "client_id": client_id, 
            "start_date": start_date, 
            "end_date": end_date
        })

    print("=== NEW QUERY RESULTS ===")
    print(df.to_string())
    print(f"\nOverall Needed: {df['total_needed'].sum()}")
    print(f"Overall Filled: {df['total_filled'].sum()}")

finally:
    if tunnel_proc:
        tunnel_proc.terminate()
