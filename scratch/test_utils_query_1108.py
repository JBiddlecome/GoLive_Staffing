import sys
import os
import time
import subprocess
import pandas as pd
from sqlalchemy import text

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load env variables from golive-staffing-tools.env
env_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'golive-staffing-tools.env')
if os.path.exists(env_file):
    with open(env_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                parts = line.split('=', 1)
                if len(parts) == 2:
                    os.environ[parts[0]] = parts[1]

# Start SSH tunnel
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
    time.sleep(5) # wait for tunnel to establish

from apps.client_notifications.utils import _engine

client_id = 1108
start_date = '2026-04-01'
end_date = '2026-04-30'

engine = _engine()

# What utils.py sees
sql_utils = text("""
    SELECT 
        s.shift_id,
        sp.shift_position_id,
        sp.count,
        sp.filled,
        LEAST(sp.count, sp.filled) as filled_capped
    FROM shift_position sp
    JOIN shift s ON sp.shift_id = s.shift_id
    JOIN event e ON s.event_id = e.event_id
    WHERE e.client_id = :client_id
      AND e.date BETWEEN :start_date AND :end_date
      AND sp.deleted_at IS NULL
""")

try:
    with engine.begin() as conn:
        df_utils = pd.read_sql(sql_utils, conn, params={
            "client_id": client_id, 
            "start_date": start_date, 
            "end_date": end_date
        })

    print("=== shift_position records for client 1108 ===")
    print(df_utils.to_string())
    print(f"\nTotal count (needed): {df_utils['count'].sum()}")
    print(f"Total filled: {df_utils['filled'].sum()}")
    print(f"Total filled (capped): {df_utils['filled_capped'].sum()}")

finally:
    if tunnel_proc:
        tunnel_proc.terminate()
