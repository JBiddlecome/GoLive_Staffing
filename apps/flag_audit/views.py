import os
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Tuple
from fastapi import APIRouter, Request, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pandas as pd
from io import BytesIO

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
    "0": "Orange",
    "1": "Red",
    "2": "Green",
    "3": "Brown",
    "5": "Purple",
    "6": "Yellow",
    "none": "No Flag"
}

def _get_employees_data(
    flags: list[str],
    has_dnr: str,
    has_da: str
) -> Tuple[list[dict], Optional[str]]:
    engine = _get_engine()
    employees = []
    error = None

    try:
        with engine.connect() as conn:
            # First, fetch eligible employees:
            # Active statuses: 1 (Active), 3 (Inactive 60), 10 (Hiatus), 14 (Other Status)
            conditions = []
            selected_int_flags = []
            for f in flags:
                if f != "none":
                    try:
                        selected_int_flags.append(int(f))
                    except ValueError:
                        pass
            
            if selected_int_flags:
                flags_placeholder = ', '.join([str(f) for f in selected_int_flags])
                conditions.append(f"e.flag IN ({flags_placeholder})")
            
            if "none" in flags:
                conditions.append("e.flag IS NULL")
                
            if conditions:
                flag_filter_sql = "AND (" + " OR ".join(conditions) + ")"
            else:
                flag_filter_sql = "AND 1=0"
            
            sql = text(f'''
                SELECT 
                    e.employee_id, 
                    e.first_name, 
                    e.last_name, 
                    e.flag
                FROM employee e
                WHERE e.status IN (1, 3, 10, 14)
                  {flag_filter_sql}
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
                
                # Check for shifts worked in the last year
                shifts_sql = text(f'''
                    SELECT 
                        t.employee_id,
                        COUNT(DISTINCT t.timesheet_id) as shifts_last_year
                    FROM timesheet t
                    JOIN shift_employee se ON t.shift_employee_id = se.shift_employee_id
                    JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
                    JOIN shift s ON sp.shift_id = s.shift_id
                    WHERE t.employee_id IN ({emp_ids_placeholder})
                      AND t.employee_worked = 'WORKED'
                      AND s.start >= DATE_SUB(NOW(), INTERVAL 1 YEAR)
                      AND s.start <= NOW()
                    GROUP BY t.employee_id
                ''')
                shifts_res = conn.execute(shifts_sql).mappings().all()
                shifts_lookup = {row['employee_id']: row['shifts_last_year'] for row in shifts_res}
                
                for r in emp_result:
                    emp_id = r['employee_id']
                    dnr_val = "Yes" if emp_id in dnr_emp_ids else "No"
                    da_val = "Yes" if emp_id in da_emp_ids else "No"
                    
                    if has_dnr != "All" and dnr_val != has_dnr:
                        continue
                    if has_da != "All" and da_val != has_da:
                        continue
                    
                    flag_key = "none" if r['flag'] is None else str(r['flag'])
                    flag_color = FLAG_COLORS.get(flag_key, f"Unknown ({r['flag']})")
                    shifts_count = shifts_lookup.get(emp_id, 0)
                        
                    employees.append({
                        "employee_id": emp_id,
                        "name": f"{r['first_name']} {r['last_name']}",
                        "flag_color": flag_color,
                        "has_dnr_last_2_years": dnr_val,
                        "has_da_last_2_years": da_val,
                        "shifts_last_year": shifts_count
                    })

    except Exception as e:
        error = f"Database error: {e}"
    finally:
        engine.dispose()
        
    return employees, error

@router.get("", response_class=HTMLResponse)
async def page(
    request: Request,
    flags: list[str] = Query(default=["0", "1"]),
    has_dnr: str = Query(default="All"),
    has_da: str = Query(default="All")
):
    employees, error = _get_employees_data(flags, has_dnr, has_da)
    return templates.TemplateResponse("apps/flag_audit.html", {
        "request": request,
        "employees": employees,
        "selected_flags": flags,
        "selected_dnr": has_dnr,
        "selected_da": has_da,
        "all_flags": FLAG_COLORS,
        "error": error
    })

@router.get("/export")
async def export_excel(
    flags: list[str] = Query(default=["0", "1"]),
    has_dnr: str = Query(default="All"),
    has_da: str = Query(default="All")
):
    employees, error = _get_employees_data(flags, has_dnr, has_da)
    if error:
        raise HTTPException(status_code=500, detail=error)
        
    data = []
    for emp in employees:
        data.append({
            "Employee ID": emp["employee_id"],
            "Employee Name": emp["name"],
            "Flag Color": emp["flag_color"],
            "DNR (Last 2 Years)": emp["has_dnr_last_2_years"],
            "Disciplinary Action (Last 2 Years)": emp["has_da_last_2_years"],
            "Shifts (Last Year)": emp["shifts_last_year"]
        })
        
    df = pd.DataFrame(data)
    
    output_buffer = BytesIO()
    df.to_excel(output_buffer, index=False)
    output_buffer.seek(0)
    
    filename = f"flag_audit_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        output_buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


