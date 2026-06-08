from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

router = APIRouter()
templates = Jinja2Templates(directory="templates")

def _db_url_from_env() -> URL:
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
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

@router.get("", response_class=HTMLResponse)
async def duplicate_other_work_report_page(request: Request):
    # Default date range: previous Monday to Sunday
    today = datetime.now()
    days_since_monday = today.weekday()
    last_monday = today - timedelta(days=days_since_monday + 7)
    last_sunday = last_monday + timedelta(days=6)
    
    start_date = last_monday.strftime("%Y-%m-%d")
    end_date = last_sunday.strftime("%Y-%m-%d")
    
    return templates.TemplateResponse(
        "apps/duplicate_other_work_report.html",
        {
            "request": request,
            "start_date": start_date,
            "end_date": end_date
        }
    )

@router.get("/data")
async def get_duplicate_other_work_data(
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    engine = _engine()
    start_datetime = f"{start_date} 00:00:00"
    end_datetime = f"{end_date} 23:59:59"
    
    try:
        sql = text("""
            SELECT 
                eow.id AS payment_id,
                eow.employee_id,
                CONCAT(e.first_name, ' ', e.last_name) AS employee_name,
                eow.other_work_type_id,
                owt.name AS other_work_type_text,
                eow.work_hours,
                eow.non_work_hours,
                eow.rate,
                eow.cost,
                eow.notes,
                eow.date AS work_date,
                eow.created_at,
                COALESCE(CONCAT(u.first_name, ' ', u.last_name), u.email, 'System') AS created_by_name
            FROM employee_other_work eow
            JOIN employee e ON eow.employee_id = e.employee_id
            JOIN other_work_type owt ON eow.other_work_type_id = owt.id
            LEFT JOIN user u ON eow.created_by = u.id
            WHERE eow.other_work_type_id NOT IN (8, 20)
              AND (eow.employee_id, eow.other_work_type_id) IN (
                SELECT range_eow.employee_id, range_eow.other_work_type_id
                FROM employee_other_work range_eow
                WHERE range_eow.other_work_type_id NOT IN (8, 20)
                GROUP BY range_eow.employee_id, range_eow.other_work_type_id
                HAVING COUNT(*) > 1
                   AND SUM(CASE WHEN range_eow.created_at >= :start_datetime AND range_eow.created_at <= :end_datetime THEN 1 ELSE 0 END) > 0
            )
            ORDER BY eow.employee_id, eow.other_work_type_id, eow.created_at DESC
        """)
        
        with engine.begin() as connection:
            result = connection.execute(sql, {
                "start_datetime": start_datetime,
                "end_datetime": end_datetime
            }).mappings().all()
            
            groups = {}
            for row in result:
                item = dict(row)
                
                # Format created_at date/time representation in-place
                if isinstance(item["created_at"], datetime):
                    item["created_at"] = item["created_at"].strftime("%Y-%m-%d %H:%M:%S")
                else:
                    item["created_at"] = str(item["created_at"])
                
                if item["work_date"]:
                    try:
                        item["work_date"] = item["work_date"].strftime("%Y-%m-%d")
                    except AttributeError:
                        item["work_date"] = str(item["work_date"])
                else:
                    item["work_date"] = ""
                
                # Format decimals nicely
                for col in ["work_hours", "non_work_hours", "rate", "cost"]:
                    if item[col] is not None:
                        item[col] = float(item[col])
                    else:
                        item[col] = 0.0
                
                # Determine range status
                is_in_range = (start_datetime <= item["created_at"] <= end_datetime)
                item["in_range"] = is_in_range
                
                key = (item["employee_id"], item["other_work_type_id"])
                if key not in groups:
                    groups[key] = {
                        "employee_id": item["employee_id"],
                        "employee_name": item["employee_name"],
                        "other_work_type_id": item["other_work_type_id"],
                        "other_work_type_text": item["other_work_type_text"],
                        "times_used": 0,
                        "times_used_in_range": 0,
                        "payments": []
                    }
                
                groups[key]["payments"].append(item)
                groups[key]["times_used"] += 1
                if is_in_range:
                    groups[key]["times_used_in_range"] += 1
            
            # Format output groups list
            grouped_data = list(groups.values())
            # Sort by total duplicate count descending, then alphabetically by employee name
            grouped_data.sort(key=lambda x: (-x["times_used"], x["employee_name"]))
            
            return JSONResponse({"data": grouped_data})
            
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()
