import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from fastapi.concurrency import run_in_threadpool

# Simple TTL Cache implementation for dropdown filters since they are broad
class SimpleTTLCache:
    def __init__(self, ttl_seconds: int = 600):
        self.ttl = ttl_seconds
        self.cache = None
        self.timestamp = 0

    def get(self):
        if self.cache is not None and (time.time() - self.timestamp) < self.ttl:
            return self.cache
        return None

    def set(self, data):
        self.cache = data
        self.timestamp = time.time()

rate_data_cache = SimpleTTLCache(ttl_seconds=600)  # 10 minute cache

def _db_url_from_env() -> URL:
    host = os.getenv("DB_HOST", "localhost")
    name = os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    port = int(os.getenv("DB_PORT", "3306"))

    return URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name,
    )

def _engine():
    return create_engine(_db_url_from_env(), pool_pre_ping=True)

def fetch_raw_rate_data(date_start: str = None, date_end: str = None) -> pd.DataFrame:
    # Use cache only for the broad filter dropdowns query (no dates)
    if not date_start and not date_end:
        cached_df = rate_data_cache.get()
        if cached_df is not None:
            return cached_df

    engine = _engine()
    
    if date_start and date_end:
        sql = text("""
            SELECT
                cpa.id AS rate_record_id,
                cpa.start_date,
                cpa.end_date,
                c.client_id,
                c.name AS client_name,
                c.industry,
                c.industry_other,
                p.position_id,
                p.description AS position_name,
                co.id AS county_id,
                co.name AS county_name,
                c.sales_executive_id,
                CONCAT(u.first_name, ' ', u.last_name) AS sales_executive_name,
                c.won_date,
                c.bundle,
                cpa.pay_rate,
                cpa.bill_rate,
                cpa.surcharge,
                cpa.created_at AS record_created_at
            FROM client_position_amount cpa
            JOIN client_position cp ON cpa.client_position_id = cp.id
            JOIN client c ON cp.client_id = c.client_id
            JOIN position p ON cp.position_id = p.position_id
            JOIN venue v ON c.client_id = v.client_id
            LEFT JOIN county co ON v.county_id = co.id
            LEFT JOIN user u ON c.sales_executive_id = u.id
            WHERE c.deleted_at IS NULL AND p.deleted_at IS NULL
              AND cpa.bill_rate IS NOT NULL AND cpa.pay_rate IS NOT NULL
              AND cpa.bill_rate > 0 AND cpa.pay_rate > 0
              AND EXISTS (
                  SELECT 1 
                  FROM event e 
                  JOIN shift s ON e.event_id = s.event_id
                  JOIN shift_position sp ON s.shift_id = sp.shift_id
                  WHERE e.venue_id = v.venue_id
                    AND e.client_id = c.client_id
                    AND sp.position_id = p.position_id
                    AND e.date >= :date_start
                    AND e.date <= :date_end
                    AND e.deleted_at IS NULL
                    AND s.deleted_at IS NULL
                    AND sp.deleted_at IS NULL
              )
        """)
        params = {"date_start": date_start, "date_end": date_end}
    else:
        sql = text("""
            SELECT DISTINCT
                co.id AS county_id,
                co.name AS county_name,
                p.position_id,
                p.description AS position_name,
                c.industry,
                c.industry_other
            FROM client_position_amount cpa
            JOIN client_position cp ON cpa.client_position_id = cp.id
            JOIN client c ON cp.client_id = c.client_id
            JOIN position p ON cp.position_id = p.position_id
            JOIN venue v ON c.client_id = v.client_id
            LEFT JOIN county co ON v.county_id = co.id
            WHERE c.deleted_at IS NULL AND p.deleted_at IS NULL
              AND cpa.bill_rate IS NOT NULL AND cpa.pay_rate IS NOT NULL
              AND cpa.bill_rate > 0 AND cpa.pay_rate > 0
        """)
        params = {}

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
        
    engine.dispose()
    
    if df.empty:
        if not date_start and not date_end:
            rate_data_cache.set(df)
        return df

    # Normalize industry
    def normalize_ind(row):
        ind = row['industry'] if pd.notna(row['industry']) else row.get('industry_other')
        if pd.isna(ind) or str(ind).strip() == '':
            return 'Unknown'
        
        ind_str = str(ind).strip().title()
        
        if 'Hospital' in ind_str: return 'Hospital'
        if 'School' in ind_str or 'University' in ind_str or 'College' in ind_str or 'Education' in ind_str: return 'Education'
        if 'Hotel' in ind_str or 'Resort' in ind_str or 'Hospitality' in ind_str: return 'Hotel & Hospitality'
        if 'Catering' in ind_str: return 'Catering'
        if 'Corporate' in ind_str or 'Office' in ind_str: return 'Corporate Dining'
        if 'Stadium' in ind_str or 'Arena' in ind_str or 'Concession' in ind_str: return 'Stadium & Concessions'
            
        return ind_str

    df['industry_normalized'] = df.apply(normalize_ind, axis=1)

    # If it's the broad filter query, just return
    if not date_start and not date_end:
        rate_data_cache.set(df)
        return df
    
    # Precompute metrics
    # Ensure they are numeric
    df['pay_rate'] = pd.to_numeric(df['pay_rate'], errors='coerce')
    df['bill_rate'] = pd.to_numeric(df['bill_rate'], errors='coerce')
    df['surcharge'] = pd.to_numeric(df['surcharge'], errors='coerce').fillna(0)

    df['spread_dollars'] = df['bill_rate'] - df['pay_rate']
    df['markup_percent'] = (df['spread_dollars'] / df['bill_rate']) * 100
    
    # Dates
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce').dt.date
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce').dt.date
    df['won_date'] = pd.to_datetime(df['won_date'], errors='coerce').dt.date
    df['record_created_at'] = pd.to_datetime(df['record_created_at'], errors='coerce')

    return df

async def get_raw_rate_data_async(date_start: str = None, date_end: str = None) -> pd.DataFrame:
    # Run pandas processing in threadpool since this is synchronous block and memory heavy
    return await run_in_threadpool(fetch_raw_rate_data, date_start, date_end)
