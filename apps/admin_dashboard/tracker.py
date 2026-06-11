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

_cached_engine = None

def _engine():
    global _cached_engine
    if _cached_engine is None:
        _cached_engine = create_engine(_db_url_from_env(), pool_pre_ping=True)
    return _cached_engine

async def admin_tracking_loop():
    await asyncio.sleep(10)
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

def get_staffing_activity(start_date: str, end_date: str):
    """
    Retrieves activity counts for predefined Staffing Managers and Staffing Coordinators.
    Counts Events and Shifts from history_entry, and Publications from the publishing table.
    """
    engine = _engine()
    
    start_timestamp = f"{start_date} 00:00:00"
    end_timestamp = f"{end_date} 23:59:59"
    
    managers = [1803, 36528, 36956]
    coordinators = [1804, 14989, 21151, 21152, 25929]
    all_users = managers + coordinators
    
    user_stats = {uid: {
        "user_id": uid, 
        "name": f"User {uid}", 
        "role": "Manager" if uid in managers else "Coordinator", 
        "total_records": 0,
        "events": 0, 
        "shifts": 0, 
        "publications": 0
    } for uid in all_users}
    
    try:
        with engine.connect() as conn:
            # 1. Fetch User names
            u_query = text(f"SELECT id, first_name, last_name FROM user WHERE id IN ({','.join(map(str, all_users))})")
            for row in conn.execute(u_query).fetchall():
                user_stats[row.id]["name"] = f"{row.first_name or ''} {row.last_name or ''}".strip() or f"User {row.id}"
            
            # 2. Fetch Event and Shift counts from history_entry
            h_query = text(f'''
                SELECT created_by, model, COUNT(id) as count
                FROM history_entry
                WHERE created_by IN ({','.join(map(str, all_users))})
                  AND model IN ('Event', 'Shift', 'ShiftPosition', 'ShiftEmployee')
                  AND created_at >= :start_date AND created_at <= :end_date
                GROUP BY created_by, model
            ''')
            h_results = conn.execute(h_query, {"start_date": start_timestamp, "end_date": end_timestamp}).fetchall()
            
            for row in h_results:
                if row.created_by in user_stats:
                    if row.model == 'Event':
                        user_stats[row.created_by]["events"] += row.count
                        user_stats[row.created_by]["total_records"] += row.count
                    else:
                        user_stats[row.created_by]["shifts"] += row.count
                        user_stats[row.created_by]["total_records"] += row.count
            
            # 3. Fetch Publications
            p_query = text(f'''
                SELECT created_by, COUNT(id) as count
                FROM publishing
                WHERE created_by IN ({','.join(map(str, all_users))})
                  AND created_at >= :start_date AND created_at <= :end_date
                GROUP BY created_by
            ''')
            p_results = conn.execute(p_query, {"start_date": start_timestamp, "end_date": end_timestamp}).fetchall()
            
            for row in p_results:
                if row.created_by in user_stats:
                    user_stats[row.created_by]["publications"] += row.count
                    user_stats[row.created_by]["total_records"] += row.count
                    
    except Exception as e:
        print(f"Error in get_staffing_activity: {e}")
        
    managers_list = [u for u in user_stats.values() if u["role"] == "Manager"]
    coordinators_list = [u for u in user_stats.values() if u["role"] == "Coordinator"]
    
    managers_list.sort(key=lambda x: x["total_records"], reverse=True)
    coordinators_list.sort(key=lambda x: x["total_records"], reverse=True)
    
    return {"managers": managers_list, "coordinators": coordinators_list}
