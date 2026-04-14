import sys
sys.path.append('apps/reports')
import pandas as pd
from sqlalchemy import text
from views import _get_staffing_engine

try:
    e = _get_staffing_engine()
    with e.connect() as conn:
        df = pd.read_sql(text('DESCRIBE shift_employee'), conn)
        print(df.to_string())
except Exception as ex:
    print(f"Error: {ex}")
