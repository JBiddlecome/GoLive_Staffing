import asyncio
import os
from datetime import datetime
from typing import Dict, Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# A dictionary to hold user activity data in memory
# Format: { user_id: { "first_name": str, "last_name": str, "first_seen": datetime, "last_seen": datetime } }
ADMIN_ACTIVITY: Dict[int, Dict[str, Any]] = {}

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

async def admin_tracking_loop():
    engine = _engine()
    while True:
        try:
            with engine.connect() as conn:
                # Query the 'touch' and 'user' tables for ADMIN activity
                query = text('''
                    SELECT t.user_id, t.updated_at, u.first_name, u.last_name
                    FROM touch t
                    JOIN user u ON t.user_id = u.id
                    WHERE u.group = 'ADMIN'
                ''')
                result = conn.execute(query).fetchall()
                for row in result:
                    uid = row.user_id
                    updated = row.updated_at
                    fname = row.first_name or ""
                    lname = row.last_name or ""
                    
                    if uid not in ADMIN_ACTIVITY:
                        ADMIN_ACTIVITY[uid] = {
                            "first_name": fname,
                            "last_name": lname,
                            "first_seen": updated,
                            "last_seen": updated
                        }
                    else:
                        ADMIN_ACTIVITY[uid]["last_seen"] = updated
        except Exception as e:
            print(f"Error in admin_tracking_loop: {e}")
        
        # Polling every 7 seconds as requested
        await asyncio.sleep(7)

def get_admin_activity():
    """Calculates time spent for each admin and returns a list."""
    res = []
    for uid, data in ADMIN_ACTIVITY.items():
        if data["last_seen"] and data["first_seen"]:
            time_spent = data["last_seen"] - data["first_seen"]
            total_seconds = int(time_spent.total_seconds())
        else:
            total_seconds = 0
            
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        time_spent_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        res.append({
            "user_id": uid,
            "name": f"{data['first_name']} {data['last_name']}".strip() or f"User {uid}",
            "time_spent_str": time_spent_str,
            "total_seconds": total_seconds,
            "first_seen": data["first_seen"].isoformat() if data["first_seen"] else "",
            "last_seen": data["last_seen"].isoformat() if data["last_seen"] else ""
        })
    return res

def get_admin_history(start_date: str, end_date: str):
    """Calculates time spent per admin by finding history activity across a date range."""
    engine = _engine()
    res = []
    try:
        with engine.connect() as conn:
            query = text('''
                SELECT 
                    h.created_by,
                    u.first_name,
                    u.last_name,
                    MIN(h.created_at) as first_action,
                    MAX(h.created_at) as last_action,
                    COUNT(h.id) as item_count
                FROM history_entry h
                JOIN user u ON h.created_by = u.id
                WHERE u.group = 'ADMIN'
                  AND h.created_at >= :start_date
                  AND h.created_at <= :end_date
                GROUP BY h.created_by, u.first_name, u.last_name
            ''')
            
            # Making end_date inclusive by setting time to end of day
            start_timestamp = f"{start_date} 00:00:00"
            end_timestamp = f"{end_date} 23:59:59"
            
            result = conn.execute(query, {"start_date": start_timestamp, "end_date": end_timestamp}).fetchall()
            
            for row in result:
                first = row.first_action
                last = row.last_action
                time_spent = last - first if first and last else None
                
                if time_spent is not None:
                    total_seconds = int(time_spent.total_seconds())
                else:
                    total_seconds = 0
                    
                hours, remainder = divmod(total_seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                time_spent_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

                res.append({
                    "user_id": row.created_by,
                    "name": f"{row.first_name or ''} {row.last_name or ''}".strip() or f"User {row.created_by}",
                    "time_spent_str": time_spent_str,
                    "item_count": row.item_count,
                    "total_seconds": total_seconds
                })
    except Exception as e:
        print(f"Error in get_admin_history: {e}")
        
    return res

