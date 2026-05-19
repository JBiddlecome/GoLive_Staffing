import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv(r'C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env')

engine = create_engine(URL.create(
    drivername='mysql+pymysql',
    username=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=int(os.getenv('DB_PORT', '3306')),
    database=os.getenv('DB_NAME', 'cstaffing_live')
))

query = """
SELECT 
    c.name AS client_name,
    COALESCE(c.bundle, 'BASIC') AS client_bundle,
    GROUP_CONCAT(DISTINCT DATE_FORMAT(e.date, '%Y-%m-%d') ORDER BY e.date ASC SEPARATOR ', ') AS holiday_dates
FROM client c
JOIN event e ON c.client_id = e.client_id
JOIN shift s ON e.event_id = s.event_id
JOIN shift_position sp ON s.shift_id = sp.shift_id
WHERE sp.holiday_rate = 1
  AND YEAR(e.date) = 2025
  AND c.deleted_at IS NULL
  AND e.deleted_at IS NULL
  AND s.deleted_at IS NULL
  AND sp.deleted_at IS NULL
GROUP BY c.client_id, c.name, c.bundle
ORDER BY c.name ASC
"""

with engine.connect() as conn:
    results = conn.execute(text(query)).mappings().all()

out_path = r'C:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\holiday_rate_report_2025.md'
with open(out_path, 'w') as f:
    f.write('# 2025 Holiday Rate Clients Report\n\n')
    f.write('| Client Name | Bundle | Dates Holiday Rate Applied |\n')
    f.write('|---|---|---|\n')
    for r in results:
        f.write(f"| {r['client_name']} | {r['client_bundle']} | {r['holiday_dates']} |\n")

print('Report generated to', out_path)
