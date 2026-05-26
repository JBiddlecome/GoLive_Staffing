import sys
import os
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
else:
    print(f"Env file not found at {env_file}")

from apps.client_notifications.utils import _engine

client_id = 1210
start_date = '2026-04-01'
end_date = '2026-04-30'

engine = _engine()

sql = text("""
    SELECT 
        e.date as event_date,
        s.shift_id,
        s.start as shift_start,
        sp.shift_position_id,
        sp.created_at as position_created_at,
        p.description as position,
        sp.count as needed_count,
        sp.filled as filled_count,
        se.shift_employee_id,
        se.employee_id,
        emp.first_name,
        emp.last_name,
        se.confirmed,
        se.confirmed_at,
        se.deleted_at as employee_deleted_at,
        CASE WHEN TIMESTAMPDIFF(HOUR, sp.created_at, s.start) > 24 THEN 'over_24h' ELSE 'under_24h' END as timing_group
    FROM shift_position sp
    JOIN shift s ON sp.shift_id = s.shift_id
    JOIN event e ON s.event_id = e.event_id
    LEFT JOIN position p ON sp.position_id = p.position_id
    LEFT JOIN shift_employee se ON sp.shift_position_id = se.shift_position_id
    LEFT JOIN employee emp ON se.employee_id = emp.employee_id
    WHERE e.client_id = :client_id
      AND e.date BETWEEN :start_date AND :end_date
      AND sp.deleted_at IS NULL
    ORDER BY e.date, s.start, sp.shift_position_id
""")

with engine.begin() as conn:
    df = pd.read_sql(sql, conn, params={
        "client_id": client_id, 
        "start_date": start_date, 
        "end_date": end_date
    })

output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scratch', 'april_shifts_1210.csv')
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df.to_csv(output_path, index=False)
print(f"Data saved to {output_path}")
