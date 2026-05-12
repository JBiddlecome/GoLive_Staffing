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

import json
from .schema_docs import COLUMN_DOCS
from .business_logic import get_business_logic_prompt, get_planner_context

router = APIRouter()
templates = Jinja2Templates(directory="templates")
client = OpenAI()

# Tables that will be loaded into DuckDB for querying.
# This list is dynamically built from the database at load time (see load_data).
# Kept here for reference; actual list comes from information_schema at runtime.
PRIMARY_TABLES = [
    "client", "venue", "event", "shift", "employee",
    "shift_position", "shift_employee", "certification",
    "venue_certification", "timesheet", "user",
]

# Tables the AI is never allowed to see or query — excluded from schema AND DuckDB
EXCLUDED_TABLES = {
    "oauth_access_tokens",
    "oauth_authorization_codes",
    "oauth_clients",
    "oauth_jwt",
    "oauth_public_keys",
    "oauth_refresh_tokens",
    "oauth_scopes",
    "oauth_users",
    "session",
    "migration",
    "golive_app_state",
}

# Specific columns blocked per table — stripped before loading into DuckDB
BLACKLIST = {
    "employee": ["sex", "dob", "ssn"],
}

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

# Tables excluded entirely — AI cannot query or know about these
EXCLUDED_TABLES = {
    # Security / system tables
    "oauth_access_tokens", "oauth_authorization_codes", "oauth_clients",
    "oauth_jwt", "oauth_public_keys", "oauth_refresh_tokens",
    "oauth_scopes", "oauth_users", "session", "migration", "golive_app_state",
    # Large / low-analytics-value tables excluded for performance
    "onboard_forms", "event_document", "user_party", "publish",
    "notification_seen", "employee_profile_updates", "notification_subscription",
    "employee_uniform", "employee_certification", "debug_publishing",
    "shift_position_tool", "shift_position_grooming_tool",
    "shift_position_uniform", "notification_addressee", "mail_queue",
    "notification", "publish_employee2", "push_notifications",
    "publish_employee",
}

# Specific columns blocked per table — stripped before loading into DuckDB
BLACKLIST = {
    "employee": ["sex", "dob", "ssn"],
}

# Per-table SQL WHERE clauses or custom queries to limit loaded data.
# Use {cols} as a placeholder for the safe column list.
TABLE_FILTERS = {
    "history_entry": "SELECT {cols} FROM `history_entry` WHERE `created_at` >= '2024-01-01'",
    "event":         "SELECT {cols} FROM `event` WHERE `date` >= '2024-01-01'",
    "timesheet":     "SELECT {cols} FROM `timesheet` ORDER BY `timesheet_id` DESC LIMIT 150000",
}

def load_data() -> int:
    total_loaded = 0
    engine = _get_staffing_engine()

    try:
        with engine.begin() as connection:
            # Dynamically discover all tables in the database
            db_name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
            result = connection.execute(text(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = :db AND TABLE_TYPE = 'BASE TABLE' "
                "ORDER BY TABLE_NAME"
            ), {"db": db_name})
            all_tables = [row[0] for row in result]

            # Load each allowed table into DuckDB
            for table in all_tables:
                if table in EXCLUDED_TABLES:
                    continue

                try:
                    # Get safe column list
                    cols_res = connection.execute(text(f"DESCRIBE `{table}`")).fetchall()
                    cols = [row[0] for row in cols_res]

                    # Strip blacklisted columns
                    if table in BLACKLIST:
                        cols = [c for c in cols if c not in BLACKLIST[table]]

                    cols_str = ", ".join([f"`{c}`" for c in cols])
                    df = pd.read_sql(text(f"SELECT {cols_str} FROM `{table}`"), connection)

                    DB.register(f"raw_{table}", df)
                    DB.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM raw_{table}")
                    count = DB.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    total_loaded += count
                    print(f"  [{table}] {count} rows loaded")

                except Exception as e:
                    print(f"  WARN: Could not load table '{table}': {e}")

    finally:
        engine.dispose()

    print(f"Data load complete. {total_loaded} total rows across all tables.")
    return total_loaded

def ensure_data_loaded():
    global DATA_READY, DATA_LOAD_ERROR
    if not DATA_READY:
        try: load_data(); DATA_READY = True
        except Exception as exc: DATA_LOAD_ERROR = str(exc); raise

def _load_dynamic_schema() -> dict:
    """Load the pre-generated schema file. Returns empty dict if not found."""
    schema_path = Path(__file__).parent / "dynamic_schema.json"
    if schema_path.exists():
        with open(schema_path, "r") as f:
            return json.load(f)
    return {}


def _schema_table_list() -> str:
    """Return a compact table-name + column-name listing for the planner prompt."""
    schema = _load_dynamic_schema()
    lines = []
    for table, cols in schema.items():
        col_names = ", ".join(c["name"] for c in cols)
        lines.append(f"{table}: {col_names}")
    return "\n".join(lines)


def _schema_for_tables(tables: list[str]) -> str:
    """Return a detailed schema block for only the requested tables."""
    schema = _load_dynamic_schema()
    lines = []
    for table in tables:
        if table not in schema:
            continue
        cols = schema[table]
        col_detail = ", ".join(f"{c['name']} ({c['type']})" for c in cols)
        lines.append(f"Table `{table}`:\n  Columns: {col_detail}")
    return "\n\n".join(lines)


# ── Step A: Planner — pick which tables are needed ──────────────────────────
SYSTEM_PLANNER = """\
You are a database query planner for a staffing company's MySQL database (queried via DuckDB).
Given a natural-language question, identify ONLY the table names needed to answer it.

Respond with a JSON array of table name strings and nothing else.
Example: ["client", "event", "timesheet"]

Use the table catalog below to guide your selection:
"""

def plan_tables(question: str) -> list[str]:
    """Step A: Ask the AI which tables are needed."""
    catalog = _schema_table_list()
    planner_ctx = get_planner_context()
    prompt = SYSTEM_PLANNER + "\n" + catalog + planner_ctx
    response = client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL_AUTOMATION", "gpt-4.1-nano"),
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": f"Question: {question}\n\nReturn a JSON object with a single key \"tables\" containing an array of table names needed."}
        ],
        temperature=0,
    )
    import re
    raw = response.choices[0].message.content.strip()
    try:
        parsed = json.loads(raw)
        tables = parsed.get("tables", [])
        if isinstance(tables, list):
            return [t for t in tables if isinstance(t, str)]
    except Exception:
        pass
    # Fallback: extract anything that looks like a table name
    return re.findall(r'"([a-z_]+)"', raw)


# ── Step B: Generator — write SQL against the chosen tables ─────────────────
SYSTEM_GENERATOR_BASE = """\
You are an expert DuckDB SQL generator for a staffing and payroll system.

Database relationship guide:
- client → event (client.client_id = event.client_id)
- event → shift (event.event_id = shift.event_id)
- shift → shift_position (shift.shift_id = shift_position.shift_id)
- shift_position → shift_employee (shift_position.shift_position_id = shift_employee.shift_position_id)
- shift_employee → timesheet (shift_employee.shift_employee_id = timesheet.shift_employee_id)
- shift_employee → employee (shift_employee.employee_id = employee.employee_id)
- event → venue (event.venue_id = venue.venue_id)
- client → venue (venue.client_id = client.client_id)

CRITICAL RULES:
- **Schema fidelity**: ONLY reference columns that appear in the "Available schemas" section below.
  Do NOT invent, guess, or assume column names. If a column is not listed in the schema, do NOT use it.
- If a table's schema includes a `deleted_at` column, add `AND table.deleted_at IS NULL` to filter
  soft-deleted rows. If the table does NOT have a `deleted_at` column, do NOT add that filter.
- Output ONLY a raw SQL SELECT statement — no markdown, no code fences, no explanation.
- Always use DuckDB syntax (use double-quoted identifiers for reserved words if needed).
- For name fields, use CONCAT(employee.first_name, ' ', employee.last_name).
- For date filtering, use CURRENT_DATE, date arithmetic, or CAST to date.
- Limit result sets to 500 rows unless the user asks for all.
- Never use INSERT, UPDATE, DELETE, DROP, CREATE, or any mutation statement.

Available schemas (only the tables you need):
"""

def generate_sql(question: str, tables: list[str]) -> str:
    """Step B: Generate SQL given the question and which tables to use."""
    schema_block = _schema_for_tables(tables)
    # Append business logic domain knowledge relevant to the chosen tables and question
    biz_logic = get_business_logic_prompt(tables, question)
    system_prompt = SYSTEM_GENERATOR_BASE + "\n" + schema_block + biz_logic
    response = client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL_PRODUCTION", "gpt-5.4-mini"),
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question}
        ],
        temperature=0,
    )
    sql = response.choices[0].message.content.strip()
    # Strip markdown fences if the model wrapped the output
    import re
    sql = re.sub(r'^```(?:sql)?\s*', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\s*```$', '', sql)
    return sql.strip()


# ── SQL Safety Guard ─────────────────────────────────────────────────────────
def _is_safe_sql(sql: str) -> bool:
    """Only allow SELECT statements. Block all mutation keywords."""
    import re
    cleaned = re.sub(r'--.*', '', sql)          # strip line comments
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)  # block comments
    cleaned = cleaned.strip().upper()
    forbidden = re.compile(
        r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|EXEC|EXECUTE|CALL|MERGE)\b'
    )
    return cleaned.startswith('SELECT') and not forbidden.search(cleaned)

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

@router.get("/api/clients/12-months")
async def get_active_clients_12m():
    engine = _get_staffing_engine()
    try:
        with engine.begin() as conn:
            sql = text("""
                SELECT c.client_id, c.name 
                FROM client c
                WHERE c.client_id IN (
                    SELECT DISTINCT e.client_id 
                    FROM event e 
                    WHERE e.date >= DATE_SUB(LAST_DAY(NOW()), INTERVAL 12 MONTH)
                ) AND c.deleted_at IS NULL
                ORDER BY c.name
            """)
            df = pd.read_sql(sql, conn)
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

# --- Commerce Casino Hours Routes ---
@router.get("/commerce-hours", response_class=HTMLResponse)
async def commerce_hours_page(request: Request):
    return templates.TemplateResponse("reports/commerce_hours.html", {"request": request})

async def _get_commerce_hours_df(start_date: str, end_date: str):
    engine = _get_staffing_engine()
    try:
        sql = text("""
            SELECT 
                CONCAT(emp.first_name, ' ', emp.last_name) AS "Employee Name",
                GROUP_CONCAT(DISTINCT p.description SEPARATOR ', ') AS "Positions Worked",
                SUM(
                    CASE 
                        WHEN t.use_sheet = 'EMPLOYEE' THEN t.employee_seconds / 3600.0
                        WHEN t.use_sheet = 'CLIENT' THEN t.client_seconds / 3600.0
                        ELSE t.client_seconds / 3600.0
                    END
                ) AS "Sum of Hours"
            FROM timesheet t
            JOIN shift_employee se ON t.shift_employee_id = se.shift_employee_id
            JOIN event e ON se.event_id = e.event_id
            JOIN employee emp ON t.employee_id = emp.employee_id
            LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            LEFT JOIN position p ON sp.position_id = p.position_id
            WHERE e.client_id = 183
              AND t.employee_worked = 'WORKED'
              AND e.date >= :start_date AND e.date <= :end_date
              AND t.employee_id NOT IN (
                  SELECT dnr.employee_id FROM dnr WHERE dnr.client_id = 183
              )
            GROUP BY t.employee_id
            ORDER BY emp.first_name ASC, emp.last_name ASC
        """)
        with engine.begin() as conn:
            df = pd.read_sql(sql, conn, params={"start_date": start_date, "end_date": end_date})
            return df
    finally:
        engine.dispose()

@router.get("/api/commerce-hours")
async def get_commerce_hours_data(start_date: str, end_date: str):
    df = await _get_commerce_hours_df(start_date, end_date)
    return df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")

@router.get("/api/commerce-hours/export")
async def export_commerce_hours_data(start_date: str, end_date: str):
    df = await _get_commerce_hours_df(start_date, end_date)
    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for this date range")
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Commerce Casino Hours')
    output.seek(0)
    
    filename = f"commerce_hours_{start_date}_to_{end_date}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

# --- AI Analytics Routes ---
@router.get("/ai-analytics", response_class=HTMLResponse)
async def ai_analytics_page(request: Request):
    return templates.TemplateResponse("reports/ai_analytics.html", {"request": request})


@router.get("/api/ai-analytics/schema")
async def get_ai_schema():
    """Return the available table names and their column counts for the UI sidebar."""
    schema = _load_dynamic_schema()
    result = []
    for table, cols in schema.items():
        primary = any(c.get("primary") for c in cols)
        result.append({
            "table": table,
            "columns": len(cols),
            "primary": primary,
        })
    return JSONResponse(result)


class AskRequest(BaseModel):
    question: str


def run_sql(sql: str) -> pd.DataFrame | Dict[str, Any]:
    try:
        return DB.execute(sql).fetchdf()
    except Exception as exc:
        return {"error": str(exc), "sql": sql}


SYSTEM_EXPLAIN = """\
You are an expert analytics assistant for a staffing company.
Your job is to explain query results clearly and concisely in plain English.

Guidelines:
- Summarize the key findings in 1-3 sentences first.
- If there are multiple rows, highlight the top/bottom values and any notable patterns.
- Use business-friendly language (e.g., "client revenue" instead of "Total Bill column").
- Format numbers with commas and 2 decimal places where appropriate.
- If the query returned no results, say so clearly and suggest why.
- Do NOT reproduce the raw data table — synthesize it.
- Do NOT explain what SQL was run.
"""


def explain_result(
    question: str,
    sql: str,
    tables_used: list[str],
    df_or_error: pd.DataFrame | Dict[str, Any],
) -> str:
    if isinstance(df_or_error, dict) and "error" in df_or_error:
        content = (
            f"The SQL query failed to execute.\n"
            f"Error: {df_or_error['error']}\n\n"
            f"Briefly explain what went wrong in user-friendly terms and suggest "
            f"how to rephrase the question."
        )
    else:
        row_count = len(df_or_error)
        # Limit CSV to first 100 rows to stay within token budget
        preview_df = df_or_error.head(100)
        table_csv = preview_df.to_csv(index=False)
        content = (
            f"User question: {question}\n"
            f"Tables queried: {', '.join(tables_used)}\n"
            f"Total rows returned: {row_count}\n\n"
            f"Data (first {min(row_count, 100)} rows):\n{table_csv}"
        )

    response = client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL_AUTOMATION", "gpt-4.1-nano"),
        messages=[
            {"role": "system", "content": SYSTEM_EXPLAIN},
            {"role": "user", "content": content},
        ],
    )
    return response.choices[0].message.content.strip()


# In-memory cache: store the last query result for download
_last_result_cache: Dict[str, Any] = {}


@router.post("/api/ask")
async def api_ask(payload: AskRequest):
    question = payload.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="No question provided")

    # Ensure data is loaded into DuckDB
    try:
        await run_in_threadpool(ensure_data_loaded)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Data load failed: {exc}")

    # Step A — Planner: which tables are needed?
    tables_used = await run_in_threadpool(plan_tables, question)
    if not tables_used:
        raise HTTPException(status_code=400, detail="Could not determine which tables to query. Try rephrasing your question.")

    # Step B — Generator: write SQL
    sql = await run_in_threadpool(generate_sql, question, tables_used)

    # Safety guard — only allow SELECT statements
    if not _is_safe_sql(sql):
        raise HTTPException(status_code=400, detail="Generated query was not a safe SELECT statement. Please rephrase your question.")

    # Execute in DuckDB
    result = await run_in_threadpool(run_sql, sql)

    # Cache result for download
    has_data = isinstance(result, pd.DataFrame) and not result.empty
    if has_data:
        _last_result_cache["df"] = result
        _last_result_cache["question"] = question
    else:
        _last_result_cache.clear()

    # Step C — Explain: synthesize a natural-language answer
    answer = await run_in_threadpool(explain_result, question, sql, tables_used, result)

    # Serialize result rows (cap at 500 for UI)
    rows = []
    columns = []
    if isinstance(result, pd.DataFrame) and not result.empty:
        display_df = result.head(500).astype(object).where(pd.notnull(result.head(500)), None)
        rows = display_df.to_dict(orient="records")
        columns = list(display_df.columns)

    return JSONResponse({
        "answer": answer,
        "sql": sql,
        "tables_used": tables_used,
        "row_count": len(result) if isinstance(result, pd.DataFrame) else 0,
        "rows": rows,
        "columns": columns,
        "has_more": isinstance(result, pd.DataFrame) and len(result) > 500,
    })


@router.get("/api/ai-analytics/download")
async def download_last_result():
    """Download the most recent AI query result as an Excel file."""
    df = _last_result_cache.get("df")
    question = _last_result_cache.get("question", "ai_result")
    if df is None or df.empty:
        raise HTTPException(status_code=404, detail="No result available to download")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="AI Result")
    output.seek(0)

    safe_name = "".join(c if c.isalnum() or c in "_-" else "_" for c in question[:40])
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="ai_result_{safe_name}.xlsx"'},
    )

# --- Client Fill Rates Routes ---
@router.get("/client-fill-rates", response_class=HTMLResponse)
async def client_fill_rates_page(request: Request):
    return templates.TemplateResponse("reports/client_fill_rates.html", {"request": request})

@router.get("/api/client-fill-rates")
async def get_client_fill_rates(start_date: str, end_date: str):
    engine = _get_staffing_engine()
    try:
        sql = text("""
            WITH valid_placements AS (
                SELECT 
                    se.shift_position_id,
                    se.shift_employee_id,
                    se.created_at AS placement_created_at,
                    se.confirmed_at AS placement_confirmed_at
                FROM shift_employee se
                INNER JOIN timesheet t ON se.shift_employee_id = t.shift_employee_id
                WHERE se.deleted_at IS NULL 
                  AND (se.cancel_reason IS NULL OR se.cancel_reason IN ('0',''))
            ),
            position_stats AS (
                SELECT 
                    sp.shift_position_id,
                    sp.shift_id,
                    sp.count AS required_count,
                    COUNT(vp.shift_employee_id) AS validated_employee_count,
                    SUM(
                        GREATEST(
                            TIMESTAMPDIFF(
                                SECOND, 
                                COALESCE(sp.created_at, vp.placement_created_at), 
                                COALESCE(vp.placement_confirmed_at, vp.placement_created_at)
                            ), 
                            0
                        )
                    ) AS total_fill_seconds
                FROM shift_position sp
                LEFT JOIN valid_placements vp ON sp.shift_position_id = vp.shift_position_id
                WHERE sp.deleted_at IS NULL AND sp.count > 0
                GROUP BY sp.shift_position_id
            )
            SELECT
                c.client_id,
                c.name AS client_name,
                COUNT(DISTINCT e.event_id) AS total_events,
                SUM(ps.required_count) AS total_shifts_created,
                SUM(ps.validated_employee_count) AS total_shifts_filled,
                SUM(ps.total_fill_seconds) / NULLIF(SUM(ps.validated_employee_count), 0) AS avg_fill_time_seconds
            FROM client c
            JOIN event e ON c.client_id = e.client_id
            JOIN shift s ON e.event_id = s.event_id
            JOIN position_stats ps ON s.shift_id = ps.shift_id
            WHERE c.deleted_at IS NULL
              AND e.deleted_at IS NULL
              AND s.deleted_at IS NULL
              AND e.date >= :start_date 
              AND e.date <= :end_date
            GROUP BY c.client_id, c.name
            ORDER BY c.name
        """)
        with engine.begin() as conn:
            df = pd.read_sql(sql, conn, params={"start_date": start_date, "end_date": end_date})
            
            if df.empty:
                return []
            
            df['fill_rate'] = ((df['total_shifts_filled'] / df['total_shifts_created']) * 100).fillna(0).round(1)
            
            def format_duration(seconds):
                if pd.isna(seconds):
                    return None
                
                seconds = max(0, seconds)
                days = int(seconds // 86400)
                hours = int((seconds % 86400) // 3600)
                minutes = int((seconds % 3600) // 60)
                
                if days > 0:
                    return f"{days}d {hours}h {minutes}m"
                elif hours > 0:
                    return f"{hours}h {minutes}m"
                else:
                    return f"{minutes}m"
            
            df['avg_fill_time'] = df['avg_fill_time_seconds'].apply(format_duration)
            
            return df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")
    finally:
        engine.dispose()

# --- Less Than 24 Hr Cancellations Routes ---
@router.get("/less-24-cancellations", response_class=HTMLResponse)
async def less_24_cancellations_page(request: Request):
    return templates.TemplateResponse("reports/less_24_cancellations.html", {"request": request})

@router.get("/api/less-24-cancellations")
async def get_less_24_cancellations(start_date: str, end_date: str):
    engine = _get_staffing_engine()
    try:
        sql = text("""
            WITH target_cancellations AS (
                SELECT 
                    se.shift_position_id,
                    se.shift_employee_id,
                    se.cancelled_at,
                    s.start as shift_start,
                    e.client_id,
                    TIMESTAMPDIFF(SECOND, se.cancelled_at, s.start) as seconds_before_start
                FROM shift_employee se
                JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
                JOIN shift s ON sp.shift_id = s.shift_id
                JOIN event e ON s.event_id = e.event_id
                WHERE se.cancel_reason = '2'
                  AND e.deleted_at IS NULL
                  AND s.deleted_at IS NULL
                  AND e.date >= :start_date 
                  AND e.date <= :end_date
            ),
            refills AS (
                SELECT 
                    tc.shift_employee_id as cancelled_employee_id,
                    MIN(se_new.confirmed_at) as refill_confirmed_at
                FROM target_cancellations tc
                JOIN shift_employee se_new ON tc.shift_position_id = se_new.shift_position_id
                WHERE se_new.confirmed = 1
                  AND (se_new.cancel_reason = '0' OR se_new.cancel_reason IS NULL OR se_new.cancel_reason = '')
                  AND se_new.confirmed_at > tc.cancelled_at
                GROUP BY tc.shift_employee_id
            ),
            cancellation_stats AS (
                SELECT 
                    tc.client_id,
                    tc.shift_employee_id,
                    tc.seconds_before_start,
                    r.refill_confirmed_at,
                    TIMESTAMPDIFF(SECOND, tc.cancelled_at, r.refill_confirmed_at) as refill_seconds
                FROM target_cancellations tc
                LEFT JOIN refills r ON tc.shift_employee_id = r.cancelled_employee_id
            )
            SELECT 
                c.client_id,
                c.name AS client_name,
                COUNT(cs.shift_employee_id) AS total_cancellations,
                AVG(cs.seconds_before_start) AS avg_seconds_before_start,
                AVG(cs.refill_seconds) AS avg_refill_seconds,
                SUM(CASE WHEN cs.refill_confirmed_at IS NULL THEN 1 ELSE 0 END) AS never_refilled_count
            FROM client c
            JOIN cancellation_stats cs ON c.client_id = cs.client_id
            WHERE c.deleted_at IS NULL
            GROUP BY c.client_id, c.name
            ORDER BY c.name
        """)
        with engine.begin() as conn:
            df = pd.read_sql(sql, conn, params={"start_date": start_date, "end_date": end_date})
            
            if df.empty:
                return []
            
            def format_duration(seconds):
                if pd.isna(seconds):
                    return None
                
                is_negative = seconds < 0
                seconds = abs(seconds)
                days = int(seconds // 86400)
                hours = int((seconds % 86400) // 3600)
                minutes = int((seconds % 3600) // 60)
                
                prefix = "-" if is_negative else ""
                
                if days > 0:
                    return f"{prefix}{days}d {hours}h {minutes}m"
                elif hours > 0:
                    return f"{prefix}{hours}h {minutes}m"
                else:
                    return f"{prefix}{minutes}m"
            
            df['avg_time_before_start'] = df['avg_seconds_before_start'].apply(format_duration)
            df['avg_time_to_refill'] = df['avg_refill_seconds'].apply(format_duration)
            
            return df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")
    finally:
        engine.dispose()

@router.get("/api/less-24-cancellations/chart/{client_id}")
async def get_less_24_cancellations_chart(client_id: int):
    engine = _get_staffing_engine()
    try:
        sql = text("""
            WITH target_cancellations AS (
                SELECT 
                    se.shift_position_id,
                    se.shift_employee_id,
                    se.cancelled_at,
                    s.start as shift_start,
                    e.client_id,
                    DATE_FORMAT(e.date, '%Y-%m') as month_str,
                    TIMESTAMPDIFF(SECOND, se.cancelled_at, s.start) as seconds_before_start
                FROM shift_employee se
                JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
                JOIN shift s ON sp.shift_id = s.shift_id
                JOIN event e ON s.event_id = e.event_id
                WHERE se.cancel_reason = '2'
                  AND e.deleted_at IS NULL
                  AND s.deleted_at IS NULL
                  AND e.client_id = :client_id
                  AND e.date >= DATE_SUB(LAST_DAY(NOW()), INTERVAL 12 MONTH)
            ),
            refills AS (
                SELECT 
                    tc.shift_employee_id as cancelled_employee_id,
                    MIN(se_new.confirmed_at) as refill_confirmed_at
                FROM target_cancellations tc
                JOIN shift_employee se_new ON tc.shift_position_id = se_new.shift_position_id
                WHERE se_new.confirmed = 1
                  AND (se_new.cancel_reason = '0' OR se_new.cancel_reason IS NULL OR se_new.cancel_reason = '')
                  AND se_new.confirmed_at > tc.cancelled_at
                GROUP BY tc.shift_employee_id
            ),
            cancellation_stats AS (
                SELECT 
                    tc.month_str,
                    tc.shift_employee_id,
                    tc.seconds_before_start,
                    r.refill_confirmed_at,
                    TIMESTAMPDIFF(SECOND, tc.cancelled_at, r.refill_confirmed_at) as refill_seconds
                FROM target_cancellations tc
                LEFT JOIN refills r ON tc.shift_employee_id = r.cancelled_employee_id
            ),
            monthly_stats AS (
                SELECT 
                    cs.month_str as month,
                    COUNT(cs.shift_employee_id) AS total_cancellations,
                    AVG(cs.seconds_before_start) AS avg_seconds_before_start,
                    AVG(cs.refill_seconds) AS avg_refill_seconds,
                    SUM(CASE WHEN cs.refill_confirmed_at IS NULL THEN 1 ELSE 0 END) AS never_refilled_count
                FROM cancellation_stats cs
                GROUP BY cs.month_str
            ),
            monthly_totals AS (
                SELECT 
                    DATE_FORMAT(e.date, '%Y-%m') as month_str,
                    SUM(sp.count) as total_shifts_created
                FROM event e
                JOIN shift s ON e.event_id = s.event_id
                JOIN shift_position sp ON s.shift_id = sp.shift_id
                WHERE e.deleted_at IS NULL
                  AND s.deleted_at IS NULL
                  AND sp.deleted_at IS NULL
                  AND sp.count > 0
                  AND e.client_id = :client_id
                  AND e.date >= DATE_SUB(LAST_DAY(NOW()), INTERVAL 12 MONTH)
                GROUP BY month_str
            )
            SELECT 
                mt.month_str as month,
                COALESCE(ms.total_cancellations, 0) AS total_cancellations,
                ms.avg_seconds_before_start,
                ms.avg_refill_seconds,
                COALESCE(ms.never_refilled_count, 0) AS never_refilled_count,
                mt.total_shifts_created,
                (COALESCE(ms.total_cancellations, 0) / mt.total_shifts_created) * 100 AS cancellation_rate,
                (COALESCE(ms.never_refilled_count, 0) / mt.total_shifts_created) * 100 AS never_refilled_rate
            FROM monthly_totals mt
            LEFT JOIN monthly_stats ms ON mt.month_str = ms.month
            ORDER BY mt.month_str ASC
        """)
        with engine.begin() as conn:
            df = pd.read_sql(sql, conn, params={"client_id": client_id})
            
            # Backfill missing months for the last 12 months
            import datetime
            from dateutil.relativedelta import relativedelta
            
            end_date = datetime.date.today().replace(day=1)
            start_date = end_date - relativedelta(months=11)
            all_months = [(start_date + relativedelta(months=i)).strftime('%Y-%m') for i in range(12)]
            
            if df.empty:
                full_df = pd.DataFrame({'month': all_months})
                full_df['total_cancellations'] = 0
                full_df['avg_seconds_before_start'] = 0
                full_df['avg_refill_seconds'] = 0
                full_df['never_refilled_count'] = 0
                full_df['cancellation_rate'] = 0.0
                full_df['never_refilled_rate'] = 0.0
                return full_df.to_dict(orient="records")
            
            # Set index & reindex to include all months
            df.set_index('month', inplace=True)
            df = df.reindex(all_months).fillna(0).reset_index()
            df.rename(columns={'index': 'month'}, inplace=True)
            
            # Convert seconds to hours for charting (since averages in seconds are huge numbers)
            df['avg_hours_before_start'] = (df['avg_seconds_before_start'] / 3600.0).round(2)
            df['avg_refill_hours'] = (df['avg_refill_seconds'] / 3600.0).round(2)
            
            return df.to_dict(orient="records")
    finally:
        engine.dispose()

@router.get("/api/less-24-cancellations/top-frequency")
async def get_top_frequency_cancellations():
    engine = _get_staffing_engine()
    try:
        sql = text("""
            WITH target_cancellations AS (
                SELECT 
                    se.shift_position_id,
                    se.shift_employee_id,
                    se.cancelled_at,
                    s.start as shift_start,
                    e.client_id,
                    HOUR(se.cancelled_at) as cancel_hour
                FROM shift_employee se
                JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
                JOIN shift s ON sp.shift_id = s.shift_id
                JOIN event e ON s.event_id = e.event_id
                WHERE se.cancel_reason = '2'
                  AND e.deleted_at IS NULL
                  AND s.deleted_at IS NULL
                  AND e.date >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
            ),
            refills AS (
                SELECT 
                    tc.shift_employee_id as cancelled_employee_id,
                    MIN(se_new.confirmed_at) as refill_confirmed_at
                FROM target_cancellations tc
                JOIN shift_employee se_new ON tc.shift_position_id = se_new.shift_position_id
                WHERE se_new.confirmed = 1
                  AND (se_new.cancel_reason = '0' OR se_new.cancel_reason IS NULL OR se_new.cancel_reason = '')
                  AND se_new.confirmed_at > tc.cancelled_at
                GROUP BY tc.shift_employee_id
            ),
            client_cancellations AS (
                SELECT 
                    tc.client_id,
                    COUNT(tc.shift_employee_id) as total_cancellations,
                    SUM(CASE WHEN r.refill_confirmed_at IS NULL THEN 1 ELSE 0 END) as unfilled_shifts
                FROM target_cancellations tc
                LEFT JOIN refills r ON tc.shift_employee_id = r.cancelled_employee_id
                GROUP BY tc.client_id
            ),
            client_shifts AS (
                SELECT 
                    e.client_id,
                    SUM(sp.count) as total_shifts
                FROM event e
                JOIN shift s ON e.event_id = s.event_id
                JOIN shift_position sp ON s.shift_id = sp.shift_id
                WHERE e.deleted_at IS NULL
                  AND s.deleted_at IS NULL
                  AND sp.deleted_at IS NULL
                  AND sp.count > 0
                  AND e.date >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
                GROUP BY e.client_id
            ),
            client_peak_hours AS (
                SELECT
                    client_id,
                    cancel_hour
                FROM (
                    SELECT 
                        client_id,
                        cancel_hour,
                        COUNT(*) as hour_count,
                        ROW_NUMBER() OVER(PARTITION BY client_id ORDER BY COUNT(*) DESC) as rn
                    FROM target_cancellations
                    GROUP BY client_id, cancel_hour
                ) ranked
                WHERE rn = 1
            )
            SELECT 
                c.name as client_name,
                cs.total_shifts,
                COALESCE(cc.total_cancellations, 0) as total_cancellations,
                (COALESCE(cc.total_cancellations, 0) / cs.total_shifts) * 100 as cancellation_rate,
                cph.cancel_hour,
                COALESCE(cc.unfilled_shifts, 0) as unfilled_shifts
            FROM client c
            JOIN client_shifts cs ON c.client_id = cs.client_id
            JOIN client_cancellations cc ON c.client_id = cc.client_id
            LEFT JOIN client_peak_hours cph ON c.client_id = cph.client_id
            WHERE c.deleted_at IS NULL
              AND cs.total_shifts > 0
            ORDER BY cancellation_rate DESC
            LIMIT 10
        """)
        with engine.begin() as conn:
            df = pd.read_sql(sql, conn)
            
            if df.empty:
                return []
            
            # Format cancellation rate
            df['cancellation_rate'] = df['cancellation_rate'].round(1)
            
            # Format peak hour
            def format_hour(h):
                if pd.isna(h): return "N/A"
                h = int(h)
                period = "AM" if h < 12 else "PM"
                h_12 = h if h <= 12 else h - 12
                if h_12 == 0: h_12 = 12
                return f"{h_12}:00 {period} - {h_12}:59 {period}"
                
            df['peak_cancellation_time'] = df['cancel_hour'].apply(format_hour)
            
            return df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")
    finally:
        engine.dispose()

# --- Basic Client Report Routes ---
@router.get("/basic-client-report", response_class=HTMLResponse)
async def basic_client_report_page(request: Request):
    return templates.TemplateResponse("reports/basic_client_report.html", {"request": request})

async def _get_basic_client_summary_df(client_id: int, start_date: str, end_date: str, by_venue: bool = False):
    engine = _get_staffing_engine()
    venue_select = "COALESCE(v.name, 'No Venue') AS venue_name," if by_venue else ""
    venue_join = "LEFT JOIN venue v ON e.venue_id = v.venue_id" if by_venue else ""
    venue_group = ", e.venue_id" if by_venue else ""
    venue_order = "venue_name ASC, " if by_venue else ""
    try:
        sql = text(f"""
            SELECT 
                {venue_select}
                CONCAT(emp.first_name, ' ', emp.last_name) AS employee_name,
                GROUP_CONCAT(DISTINCT p.description SEPARATOR ', ') AS positions_worked,
                SUM(
                    CASE 
                        WHEN t.use_sheet = 'EMPLOYEE' THEN t.employee_seconds / 3600.0
                        WHEN t.use_sheet = 'CLIENT' THEN t.client_seconds / 3600.0
                        ELSE t.client_seconds / 3600.0
                    END
                ) AS total_hours,
                CASE
                    WHEN EXISTS (SELECT 1 FROM exclusive ex WHERE ex.employee_id = t.employee_id AND ex.client_id = :client_id)
                         AND EXISTS (SELECT 1 FROM dnr d WHERE d.employee_id = t.employee_id AND d.client_id = :client_id)
                    THEN 'Preferred, DNR'
                    WHEN EXISTS (SELECT 1 FROM exclusive ex WHERE ex.employee_id = t.employee_id AND ex.client_id = :client_id)
                    THEN 'Preferred'
                    WHEN EXISTS (SELECT 1 FROM dnr d WHERE d.employee_id = t.employee_id AND d.client_id = :client_id)
                    THEN 'DNR'
                    ELSE ''
                END AS list_status
            FROM timesheet t
            JOIN shift_employee se ON t.shift_employee_id = se.shift_employee_id
            JOIN event e ON se.event_id = e.event_id
            {venue_join}
            JOIN employee emp ON t.employee_id = emp.employee_id
            LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            LEFT JOIN position p ON sp.position_id = p.position_id
            WHERE e.client_id = :client_id
              AND t.employee_worked = 'WORKED'
              AND e.date >= :start_date AND e.date <= :end_date
            GROUP BY t.employee_id {venue_group}
            ORDER BY {venue_order} emp.first_name ASC, emp.last_name ASC
        """)
        with engine.begin() as conn:
            df = pd.read_sql(sql, conn, params={
                "client_id": client_id,
                "start_date": start_date,
                "end_date": end_date,
            })
            return df
    finally:
        engine.dispose()

async def _get_basic_client_detail_df(client_id: int, start_date: str, end_date: str):
    engine = _get_staffing_engine()
    try:
        sql = text("""
            SELECT 
                e.date AS shift_date,
                COALESCE(v.name, 'No Venue') AS venue_name,
                CONCAT(emp.first_name, ' ', emp.last_name) AS employee_name,
                COALESCE(p.description, '') AS position,
                COALESCE(
                    TIMESTAMPDIFF(SECOND, s.start, s.end) / 3600.0,
                    0
                ) AS scheduled_hours,
                CASE 
                    WHEN t.use_sheet = 'EMPLOYEE' THEN t.employee_seconds / 3600.0
                    WHEN t.use_sheet = 'CLIENT' THEN t.client_seconds / 3600.0
                    ELSE t.client_seconds / 3600.0
                END AS worked_hours,
                COALESCE(se.rate, 0) AS pay_rate,
                COALESCE(se.bill_rate, 0) AS bill_rate,
                CASE
                    WHEN EXISTS (SELECT 1 FROM exclusive ex WHERE ex.employee_id = t.employee_id AND ex.client_id = :client_id)
                         AND EXISTS (SELECT 1 FROM dnr d WHERE d.employee_id = t.employee_id AND d.client_id = :client_id)
                    THEN 'Preferred, DNR'
                    WHEN EXISTS (SELECT 1 FROM exclusive ex WHERE ex.employee_id = t.employee_id AND ex.client_id = :client_id)
                    THEN 'Preferred'
                    WHEN EXISTS (SELECT 1 FROM dnr d WHERE d.employee_id = t.employee_id AND d.client_id = :client_id)
                    THEN 'DNR'
                    ELSE ''
                END AS list_status
            FROM timesheet t
            JOIN shift_employee se ON t.shift_employee_id = se.shift_employee_id
            JOIN event e ON se.event_id = e.event_id
            LEFT JOIN venue v ON e.venue_id = v.venue_id
            JOIN employee emp ON t.employee_id = emp.employee_id
            LEFT JOIN shift_position sp ON se.shift_position_id = sp.shift_position_id
            LEFT JOIN position p ON sp.position_id = p.position_id
            LEFT JOIN shift s ON sp.shift_id = s.shift_id
            WHERE e.client_id = :client_id
              AND t.employee_worked = 'WORKED'
              AND e.date >= :start_date AND e.date <= :end_date
            ORDER BY e.date ASC, emp.first_name ASC, emp.last_name ASC
        """)
        with engine.begin() as conn:
            df = pd.read_sql(sql, conn, params={
                "client_id": client_id,
                "start_date": start_date,
                "end_date": end_date,
            })
            if "shift_date" in df.columns and not df.empty:
                df["shift_date"] = pd.to_datetime(df["shift_date"]).dt.strftime('%m/%d/%Y')
            return df
    finally:
        engine.dispose()

@router.get("/api/basic-client-report")
async def get_basic_client_report(client_id: int, start_date: str, end_date: str, by_venue: bool = False):
    df = await _get_basic_client_summary_df(client_id, start_date, end_date, by_venue=by_venue)
    return df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")

@router.get("/api/basic-client-report/export")
async def export_basic_client_report(client_id: int, start_date: str, end_date: str, by_venue: bool = False):
    summary_df = await _get_basic_client_summary_df(client_id, start_date, end_date, by_venue=by_venue)
    detail_df = await _get_basic_client_detail_df(client_id, start_date, end_date)

    if summary_df.empty:
        raise HTTPException(status_code=404, detail="No data found for this client and date range")

    # Fetch client name for the filename
    engine = _get_staffing_engine()
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("SELECT name FROM client WHERE client_id = :cid"),
                {"cid": client_id},
            )
            row = result.fetchone()
            client_name = row[0] if row else str(client_id)
    finally:
        engine.dispose()

    # Clean client name for filename
    safe_name = "".join(c if c.isalnum() or c in (' ', '-', '_') else '' for c in client_name).strip().replace(' ', '_')

    # Rename columns for nicer Excel headers
    summary_columns = {
        "employee_name": "Employee Name",
        "positions_worked": "Positions Worked",
        "total_hours": "Total Hours",
        "list_status": "List Status",
    }
    if by_venue:
        summary_columns["venue_name"] = "Venue Name"
        
    summary_export = summary_df.rename(columns=summary_columns)

    detail_export = detail_df.rename(columns={
        "shift_date": "Date of Shift",
        "venue_name": "Venue Name",
        "employee_name": "Employee Name",
        "position": "Position",
        "scheduled_hours": "Scheduled Hours",
        "worked_hours": "Worked Hours",
        "pay_rate": "Pay Rate",
        "bill_rate": "Bill Rate",
        "list_status": "List Status",
    })

    # Reorder summary columns to put Venue first if present
    if by_venue and "Venue Name" in summary_export.columns:
        cols = ["Venue Name"] + [c for c in summary_export.columns if c != "Venue Name"]
        summary_export = summary_export[cols]

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        summary_export.to_excel(writer, index=False, sheet_name='Employee Summary')
        detail_export.to_excel(writer, index=False, sheet_name='Shift Details')
    output.seek(0)

    filename = f"{safe_name}_report_{start_date}_to_{end_date}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )

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
