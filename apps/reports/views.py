from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import duckdb
import pandas as pd
from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
import io
from fastapi.templating import Jinja2Templates
from openai import OpenAI
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from .schema_docs import COLUMN_DOCS

router = APIRouter()
templates = Jinja2Templates(directory="templates")
client = OpenAI()

# --- AI Analytics Config (PostgreSQL/DuckDB) ---
DATABASE_URL = os.environ.get(
    "REPORTS_DATABASE_URL",
    os.environ.get(
        "DATABASE_URL",
        "postgresql://jakebiddlecome:ae7QYzwVnGPED65@clientdata.coq6m1rznxjt.us-east-1.rds.amazonaws.com:5432/clientdata",
    ),
)
DATA_TABLE_NAME = os.environ.get("REPORTS_TABLE_NAME", "shifts")

# --- MariaDB Config (Staffing DB) ---
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

def _get_staffing_engine():
    return create_engine(_db_url_from_env(), pool_pre_ping=True)

# --- AI Analytics Logic ---
MODULE_BASE_DIR = Path(__file__).resolve().parents[2]
RENDER_DATA_DIR = Path("/opt/render/project/src/data")
if any(os.getenv(env_var) for env_var in ("RENDER", "RENDER_SERVICE_ID", "RENDER_EXTERNAL_URL")):
    DATA_DIR = RENDER_DATA_DIR
elif RENDER_DATA_DIR.exists():
    DATA_DIR = RENDER_DATA_DIR
else:
    DATA_DIR = MODULE_BASE_DIR / "data"

DB = duckdb.connect(database=":memory:")
IGNORE_COLUMNS = COLUMN_DOCS["IGNORE_COLUMNS"]
DATA_READY = False
DATA_LOAD_ERROR: str | None = None

def load_data() -> int:
    df: pd.DataFrame | None = None
    db_error: Exception | None = None

    if DATABASE_URL:
        url = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1) if DATABASE_URL.startswith("postgresql://") else DATABASE_URL
        engine = create_engine(url)
        try:
            with engine.begin() as connection:
                df = pd.read_sql(text(f"SELECT * FROM {DATA_TABLE_NAME}"), connection)
        except Exception as exc:
            db_error = exc
        finally:
            engine.dispose()

    if df is None:
        try:
            candidates = (DATA_DIR / "Payroll 2.csv", DATA_DIR / "payroll.csv", DATA_DIR / "payroll.xlsx")
            for path in candidates:
                if not path.exists(): continue
                df = pd.read_csv(path) if path.suffix.lower() == ".csv" else pd.read_excel(path)
                break
        except Exception as exc:
            if db_error: raise RuntimeError(f"Unable to load data: {db_error}")
            raise

    if df is not None:
        DB.register("raw_df", df)
        DB.execute("""
            CREATE OR REPLACE TABLE shifts AS
            SELECT *, (COALESCE("First Name", '') || ' ' || COALESCE("Last Name", '')) AS "Employee Name",
            (COALESCE("Reg H (e)", 0) + COALESCE("OT H (e)", 0) + COALESCE("DT H (e)", 0)) AS "Hours Worked"
            FROM raw_df
        """)
        return DB.execute("SELECT COUNT(*) FROM shifts").fetchone()[0]
    return 0

def ensure_data_loaded():
    global DATA_READY, DATA_LOAD_ERROR
    if not DATA_READY:
        try: load_data(); DATA_READY = True
        except Exception as exc: DATA_LOAD_ERROR = str(exc); raise

schema_text = "\n".join(f"- {col}: {desc}" for col, desc in COLUMN_DOCS.items() if col not in ("IGNORE_COLUMNS", "RULES"))
rules_text = COLUMN_DOCS["RULES"]
SYSTEM_SQL = f"You are an expert DuckDB SQL generator... Table: shifts\nColumns:\n{schema_text}\nRules:\n{rules_text}"

def generate_sql(question: str) -> str:
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": SYSTEM_SQL}, {"role": "user", "content": question}]
    )
    return response.choices[0].message.content.strip()

# --- Common API Routes ---
@router.get("/api/clients")
async def get_clients():
    engine = _get_staffing_engine()
    try:
        with engine.begin() as conn:
            df = pd.read_sql(text("SELECT client_id, name FROM client ORDER BY name"), conn)
            return df.to_dict(orient="records")
    finally:
        engine.dispose()

# --- Preferred List Routes ---
@router.get("/preferred", response_class=HTMLResponse)
async def preferred_list_page(request: Request):
    return templates.TemplateResponse("reports/preferred_list.html", {"request": request})

async def _get_preferred_df(client_id: int):
    engine = _get_staffing_engine()
    try:
        sql = text("""
            SELECT 
                e.date_created, 
                c.name as "Client Name", 
                COALESCE(v.name, 'All Venues') as venue_name, 
                CONCAT(emp.first_name, ' ', emp.last_name) as employee_name, 
                e.reason, 
                e.notes, 
                COALESCE(NULLIF(CONCAT(u.first_name, ' ', u.last_name), ' '), u.username) as created_by
            FROM exclusive e
            JOIN employee emp ON e.employee_id = emp.employee_id
            JOIN client c ON e.client_id = c.client_id
            LEFT JOIN venue v ON e.venue_id = v.venue_id
            LEFT JOIN user u ON e.created_by = u.id
            WHERE e.client_id = :client_id
            ORDER BY e.date_created DESC
        """)
        with engine.begin() as conn:
            df = pd.read_sql(sql, conn, params={"client_id": client_id})
            if "date_created" in df.columns and not df.empty:
                df["date_created"] = pd.to_datetime(df["date_created"]).dt.strftime('%m/%d/%Y %H:%M')
            return df
    finally:
        engine.dispose()

@router.get("/api/preferred/{client_id}")
async def get_preferred_data(client_id: int):
    df = await _get_preferred_df(client_id)
    # Robustly handle NaN values for JSON compliance
    return df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")

@router.get("/api/preferred/export/{client_id}")
async def export_preferred_data(client_id: int):
    df = await _get_preferred_df(client_id)
    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for this client")
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Preferred List')
    output.seek(0)
    
    filename = f"preferred_list_{client_id}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

# --- DNR List Routes ---
@router.get("/dnr", response_class=HTMLResponse)
async def dnr_list_page(request: Request):
    return templates.TemplateResponse("reports/dnr_list.html", {"request": request})

async def _get_dnr_df(client_id: int):
    engine = _get_staffing_engine()
    try:
        sql = text("""
            SELECT 
                d.created_at as date_created, 
                c.name as "Client Name", 
                COALESCE(v.name, 'All Venues') as venue_name, 
                CONCAT(emp.first_name, ' ', emp.last_name) as employee_name, 
                COALESCE(sr.reason, d.other_reason) as reason, 
                d.notes, 
                COALESCE(NULLIF(CONCAT(u.first_name, ' ', u.last_name), ' '), u.username) as created_by
            FROM dnr d
            JOIN employee emp ON d.employee_id = emp.employee_id
            JOIN client c ON d.client_id = c.client_id
            LEFT JOIN status_reason sr ON d.reason_id = sr.id
            LEFT JOIN venue v ON d.venue_id = v.venue_id
            LEFT JOIN user u ON d.created_by = u.id
            WHERE d.client_id = :client_id
            ORDER BY d.created_at DESC
        """)
        with engine.begin() as conn:
            df = pd.read_sql(sql, conn, params={"client_id": client_id})
            if "date_created" in df.columns and not df.empty:
                df["date_created"] = pd.to_datetime(df["date_created"]).dt.strftime('%m/%d/%Y %H:%M')
            return df
    finally:
        engine.dispose()

@router.get("/api/dnr/{client_id}")
async def get_dnr_data(client_id: int):
    df = await _get_dnr_df(client_id)
    return df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")

@router.get("/api/dnr/export/{client_id}")
async def export_dnr_data(client_id: int):
    df = await _get_dnr_df(client_id)
    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for this client")
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='DNR List')
    output.seek(0)
    
    filename = f"dnr_list_{client_id}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

# --- AI Analytics Routes ---
@router.get("/ai-analytics", response_class=HTMLResponse)
async def ai_analytics_page(request: Request):
    return templates.TemplateResponse("reports/ai_analytics.html", {"request": request})

class AskRequest(BaseModel):
    question: str

def run_sql(sql: str) -> pd.DataFrame | Dict[str, Any]:
    try:
        return DB.execute(sql).fetchdf()
    except Exception as exc:
        return {"error": str(exc), "sql": sql}

SYSTEM_EXPLAIN = """
You are an analytics assistant for a staffing/payroll dataset.
Explain results clearly:
- Reference revenue using Total Bill.
- Reference pay using Gross Pay.
- Reference hours using Hours Worked.
- Highlight totals, averages, top clients, etc.
- Avoid mentioning SQL unless helpful.
"""

def explain_result(question: str, sql: str, df_or_error: pd.DataFrame | Dict[str, Any]) -> str:
    if isinstance(df_or_error, dict) and "error" in df_or_error:
        content = f"The SQL query failed.\n\nSQL:\n{sql}\n\nError:\n{df_or_error['error']}"
    else:
        table_csv = df_or_error.to_csv(index=False)
        content = f"User question:\n{question}\n\nSQL executed:\n{sql}\n\nResult (CSV):\n{table_csv}"

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": SYSTEM_EXPLAIN}, {"role": "user", "content": content}]
    )
    return response.choices[0].message.content.strip()

@router.post("/api/ask")
async def api_ask(payload: AskRequest):
    question = payload.question.strip()
    if not question: raise HTTPException(status_code=400, detail="No question provided")
    try: await run_in_threadpool(ensure_data_loaded)
    except Exception as exc: raise HTTPException(status_code=500, detail=f"Data load failed: {exc}")

    sql = await run_in_threadpool(generate_sql, question)
    result = await run_in_threadpool(run_sql, sql)
    answer = await run_in_threadpool(explain_result, question, sql, result)
    return JSONResponse({"answer": answer, "sql": sql})

# --- Hub Route ---
@router.get("/", response_class=HTMLResponse)
async def reports_home(request: Request):
    return templates.TemplateResponse("reports/index.html", {"request": request})

if __name__ == "__main__":
    import uvicorn
    from fastapi import FastAPI
    dev_app = FastAPI()
    dev_app.include_router(router, prefix="/reports")
    uvicorn.run(dev_app, host="0.0.0.0", port=5000, reload=True)
