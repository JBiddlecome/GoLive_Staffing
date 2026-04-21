import os
from datetime import datetime, date
from fastapi import APIRouter, Request, Query, Form
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

@router.get("", response_class=HTMLResponse)
async def page(
    request: Request,
    employee_id: int | None = Query(None),
    start_date: str | None = Query(None),
    end_date: str | None = Query(None)
):
    engine = _get_engine()
    employees = []
    shifts = []
    error = None

    try:
        with engine.connect() as conn:
            if employee_id:
                emp_sql = text('''
                    SELECT employee_id, first_name, last_name, email
                    FROM employee
                    WHERE employee_id = :emp_id
                ''')
                result = conn.execute(emp_sql, {"emp_id": employee_id}).mappings().first()
                if result:
                    employees = [{"id": result["employee_id"], "name": f"{result['first_name']} {result['last_name']} ({result['email']})"}]

            if employee_id and start_date and end_date:
                # Fetch shift history
                shift_sql = text('''
                    SELECT 
                        e.first_name,
                        e.last_name,
                        ev.date AS event_date,
                        ev.title AS event_title,
                        c.name AS client_name,
                        s.start AS shift_start,
                        s.end AS shift_end,
                        p.description AS position,
                        sp.rate AS pay_rate,
                        pe.created_on AS published_at
                    FROM 
                        publish_employee pe
                        INNER JOIN employee e 
                            ON e.employee_id = pe.employee_id
                        INNER JOIN shift_position sp 
                            ON sp.shift_position_id = pe.shift_position_id
                        INNER JOIN shift s 
                            ON s.shift_id = sp.shift_id
                        INNER JOIN event ev 
                            ON ev.event_id = pe.event_id
                        INNER JOIN client c 
                            ON c.client_id = ev.client_id
                        INNER JOIN position p 
                            ON p.position_id = sp.position_id
                    WHERE 
                        pe.employee_id = :employee_id
                        AND ev.date BETWEEN :start_date AND :end_date
                    ORDER BY 
                        pe.created_on DESC, ev.date, s.start;
                ''')
                shift_results = conn.execute(shift_sql, {"employee_id": employee_id, "start_date": start_date, "end_date": end_date}).mappings().all()
                for r in shift_results:
                    shifts.append({
                        "event_date": r["event_date"].isoformat() if hasattr(r["event_date"], 'isoformat') else str(r["event_date"]),
                        "event_title": r["event_title"],
                        "client_name": r["client_name"],
                        "shift_start": r["shift_start"].strftime("%I:%M %p").lstrip("0") if hasattr(r["shift_start"], 'strftime') else str(r["shift_start"]),
                        "shift_end": r["shift_end"].strftime("%I:%M %p").lstrip("0") if hasattr(r["shift_end"], 'strftime') else str(r["shift_end"]),
                        "position": r["position"],
                        "pay_rate": float(r["pay_rate"]) if r["pay_rate"] else 0.0,
                        "published_at": r["published_at"].strftime("%m/%d/%Y %I:%M %p") if hasattr(r["published_at"], 'strftime') else str(r["published_at"])
                    })
    except Exception as e:
        error = f"Database error: {e}"
    finally:
        engine.dispose()

    return templates.TemplateResponse("apps/employee_available_shift_history.html", {
        "request": request,
        "employees": employees,
        "shifts": shifts,
        "selected_employee_id": employee_id,
        "start_date": start_date,
        "end_date": end_date,
        "error": error
    })


@router.get("/api/search")
async def search_employees(q: str = Query("")):
    if len(q) < 2:
        return JSONResponse([])
    
    engine = _get_engine()
    results = []
    try:
        with engine.connect() as conn:
            sql = text('''
                SELECT employee_id, first_name, last_name, email
                FROM employee
                WHERE first_name LIKE :q 
                   OR last_name LIKE :q 
                   OR CONCAT(first_name, ' ', last_name) LIKE :q
                   OR email LIKE :q
                ORDER BY first_name, last_name
                LIMIT 50
            ''')
            res = conn.execute(sql, {"q": f"%{q}%"}).mappings().all()
            for r in res:
                results.append({
                    "id": r["employee_id"], 
                    "name": f"{r['first_name']} {r['last_name']} ({r['email']})"
                })
    except Exception:
        pass
    finally:
        engine.dispose()
    
    return JSONResponse(results)
