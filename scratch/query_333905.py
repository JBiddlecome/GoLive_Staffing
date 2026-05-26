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

# Start SSH tunnel
pem_key = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'golive-bastion-key.pem')
bastion_host = os.environ.get('BASTION_HOST')
bastion_user = os.environ.get('BASTION_USER')
rds_host = os.environ.get('RDS_HOST')
local_port = os.environ.get('DB_PORT', '3307')

tunnel_cmd = [
    'ssh', '-o', 'StrictHostKeyChecking=no',
    '-o', 'ExitOnForwardFailure=yes',
    '-N', '-L', f'127.0.0.1:{local_port}:{rds_host}:3306',
    '-i', pem_key,
    f'{bastion_user}@{bastion_host}'
]

print("Starting SSH tunnel:", " ".join(tunnel_cmd))
tunnel_proc = subprocess.Popen(tunnel_cmd)
time.sleep(5) # wait for tunnel to establish

from apps.client_notifications.utils import _engine

engine = _engine()

try:
    sql_sp = text("SELECT * FROM shift_position WHERE shift_position_id = 333905")
    sql_se = text("SELECT * FROM shift_employee WHERE shift_position_id = 333905")

    with engine.begin() as conn:
        df_sp = pd.read_sql(sql_sp, conn)
        df_se = pd.read_sql(sql_se, conn)

    print("--- shift_position 333905 ---")
    print(df_sp.to_string())
    print("--- shift_employee for 333905 ---")
    print(df_se.to_string())
finally:
    tunnel_proc.terminate()
