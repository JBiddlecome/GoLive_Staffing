import os
from datetime import datetime, date
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

router = APIRouter()
templates = Jinja2Templates(directory="templates")

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
        database=name
    )

def _get_engine():
    return create_engine(_db_url_from_env(), pool_pre_ping=True)

FLAG_COLORS = {
    0: "Orange",
    1: "Red",
    3: "Brown",
    5: "Purple",
    6: "Yellow"
}

@router.get("", response_class=HTMLResponse)
async def page(
    request: Request,
    flags: list[int] = Query(default=[0, 1]),
    has_dnr: str = Query(default="All"),
    has_da: str = Query(default="All")
):
    engine = _get_engine()
    employees = []
    error = None

    try:
        if not flags:
            flags = [-1] # prevent empty IN clause error
            
        with engine.connect() as conn:
            # First, fetch eligible employees:
            # Active statuses: 1 (Active), 3 (Inactive 60), 10 (Hiatus), 14 (Other Status)
            # flags in the requested list
            flags_placeholder = ', '.join([str(int(f)) for f in flags])
            
            sql = text(f'''
                SELECT 
                    e.employee_id, 
                    e.first_name, 
                    e.last_name, 
                    e.flag
                FROM employee e
                WHERE e.status IN (1, 3, 10, 14)
                  AND e.flag IN ({flags_placeholder})
                ORDER BY e.first_name, e.last_name
            ''')
            
            emp_result = conn.execute(sql).mappings().all()
            
            if emp_result:
                emp_ids = [r['employee_id'] for r in emp_result]
                emp_ids_placeholder = ', '.join([str(eid) for eid in emp_ids])
                
                # Check for DNR within last 2 years
                dnr_sql = text(f'''
                    SELECT DISTINCT employee_id
                    FROM dnr
                    WHERE employee_id IN ({emp_ids_placeholder})
                      AND created_at >= DATE_SUB(NOW(), INTERVAL 2 YEAR)
                ''')
                dnr_res = conn.execute(dnr_sql).mappings().all()
                dnr_emp_ids = {r['employee_id'] for r in dnr_res}
                
                # Check for DA (Warning in history_entry) within last 2 years
                da_sql = text(f'''
                    SELECT DISTINCT related_id as employee_id
                    FROM history_entry
                    WHERE related = 'Employee'
                      AND related_id IN ({emp_ids_placeholder})
                      AND created_at >= DATE_SUB(NOW(), INTERVAL 2 YEAR)
                      AND changes LIKE '%Warning%'
                ''')
                da_res = conn.execute(da_sql).mappings().all()
                da_emp_ids = {r['employee_id'] for r in da_res}
                
                for r in emp_result:
                    emp_id = r['employee_id']
                    dnr_val = "Yes" if emp_id in dnr_emp_ids else "No"
                    da_val = "Yes" if emp_id in da_emp_ids else "No"
                    
                    if has_dnr != "All" and dnr_val != has_dnr:
                        continue
                    if has_da != "All" and da_val != has_da:
                        continue
                        
                    employees.append({
                        "employee_id": emp_id,
                        "name": f"{r['first_name']} {r['last_name']}",
                        "flag_color": FLAG_COLORS.get(r['flag'], f"Unknown ({r['flag']})"),
                        "has_dnr_last_2_years": dnr_val,
                        "has_da_last_2_years": da_val
                    })

    except Exception as e:
        error = f"Database error: {e}"
    finally:
        engine.dispose()

    return templates.TemplateResponse("apps/flag_audit.html", {
        "request": request,
        "employees": employees,
        "selected_flags": flags,
        "selected_dnr": has_dnr,
        "selected_da": has_da,
        "all_flags": FLAG_COLORS,
        "error": error
    })
