from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


def _db_url_from_env() -> URL:
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))

    if host in {"127.0.0.1", "localhost"} and not reportable_host:
        tunnel_port = os.getenv("LOCAL_TUNNEL_PORT")
        rds_host = os.getenv("RDS_HOST")
        if rds_host and (not tunnel_port or str(port) != tunnel_port):
            host = rds_host

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


def get_drop_off_data() -> List[Dict[str, Any]]:
    engine = _engine()
    try:
        # Logic: 
        # 1. Had a shift in the last 60 days (relative to today).
        # 2. No shifts in the last 7 days OR the future (relative to today).
        # This implies the MAX(date) is between CURDATE - 60 and CURDATE - 7.
        
        sql = text(
            """
            WITH LatestEvents AS (
                SELECT 
                    client_id,
                    MAX(date) AS last_shift_date
                FROM event
                WHERE deleted_at IS NULL
                GROUP BY client_id
                HAVING last_shift_date >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
                   AND last_shift_date < DATE_SUB(CURDATE(), INTERVAL 7 DAY)
            ),
            EventVenue AS (
                SELECT 
                    le.client_id,
                    le.last_shift_date,
                    e.venue_id
                FROM LatestEvents le
                JOIN event e ON le.client_id = e.client_id AND le.last_shift_date = e.date
                WHERE e.deleted_at IS NULL
                GROUP BY le.client_id, le.last_shift_date
            )
            SELECT 
                c.name AS Client,
                COALESCE(NULLIF(CONCAT_WS(' ', u.first_name, u.last_name), ''), 'Unassigned') AS `Staffing Manager`,
                ev.last_shift_date AS `Last Shift`
            FROM EventVenue ev
            JOIN client c ON ev.client_id = c.client_id
            LEFT JOIN venue v ON ev.venue_id = v.venue_id
            LEFT JOIN user u ON v.staffing_manager_id = u.id
            WHERE c.deleted_at IS NULL
              AND c.name NOT LIKE '%%Private%%'
            """
        )
        
        with engine.connect() as connection:
            df = pd.read_sql(sql, connection)
            
        if df.empty:
            return []
            
        today = date.today()
        df["Last Shift"] = pd.to_datetime(df["Last Shift"], errors="coerce")
        df["Days Since Last Shift"] = (pd.Timestamp(today) - df["Last Shift"]).dt.days.fillna(0).astype(int)
        df["Last Shift"] = df["Last Shift"].dt.date
        
        # Sort by Days Since Last Shift (Largest to smallest)
        df = df.sort_values(by="Days Since Last Shift", ascending=False)
        
        return df.to_dict(orient="records")
    finally:
        engine.dispose()
