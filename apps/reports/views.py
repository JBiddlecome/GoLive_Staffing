from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import duckdb
import pandas as pd
from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from openai import OpenAI
from pydantic import BaseModel
from sqlalchemy import create_engine, text

from .schema_docs import COLUMN_DOCS

router = APIRouter()
templates = Jinja2Templates(directory="templates")
client = OpenAI()

DATABASE_URL = os.environ.get(
    "REPORTS_DATABASE_URL",
    os.environ.get(
        "DATABASE_URL",
        "postgresql://jakebiddlecome:ae7QYzwVnGPED65@clientdata.coq6m1rznxjt.us-east-1.rds.amazonaws.com:5432/clientdata",
    ),
)
DATA_TABLE_NAME = os.environ.get("REPORTS_TABLE_NAME", "shifts")

# Paths
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


# -------------------------------------------------------------------
# 1. LOAD DATA FROM POSTGRES
# -------------------------------------------------------------------
def ensure_dataframe_has_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Validate that the downloaded data produced real columns."""

    if df.shape[1] == 0:
        raise RuntimeError(
            "Downloaded table contains no columns. "
            "Confirm REPORTS_DATABASE_URL points to a valid PostgreSQL database."
        )

    return df


def _load_local_payroll_dataframe() -> pd.DataFrame:
    """Load payroll data from local CSV/Excel files as a fallback."""

    candidates = (
        DATA_DIR / "Payroll 2.csv",
        DATA_DIR / "payroll.csv",
        DATA_DIR / "payroll.xlsx",
        MODULE_BASE_DIR / "Payroll 2.csv",
        MODULE_BASE_DIR / "payroll.csv",
        MODULE_BASE_DIR / "payroll.xlsx",
    )

    for path in candidates:
        if not path.exists():
            continue

        if path.suffix.lower() == ".csv":
            df = pd.read_csv(path)
        else:
            df = pd.read_excel(path)

        return ensure_dataframe_has_columns(df)

    raise RuntimeError(
        "No payroll data found locally. Upload payroll.csv/payroll.xlsx or set REPORTS_DATABASE_URL."
    )


def load_data() -> int:
    df: pd.DataFrame | None = None
    db_error: Exception | None = None

    if not DATABASE_URL:
        db_error = RuntimeError("REPORTS_DATABASE_URL environment variable not set.")

    if DATABASE_URL:
        url = (
            DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)
            if DATABASE_URL.startswith("postgresql://")
            else DATABASE_URL
        )

        engine = create_engine(url)
        try:
            with engine.begin() as connection:
                df = pd.read_sql(text(f"SELECT * FROM {DATA_TABLE_NAME}"), connection)
        except Exception as exc:  # pragma: no cover - runtime safeguard
            db_error = exc
        finally:
            engine.dispose()

        if df is not None:
            df = ensure_dataframe_has_columns(df)

    if df is None:
        try:
            df = _load_local_payroll_dataframe()
        except Exception as exc:  # pragma: no cover - runtime safeguard
            if db_error:
                raise RuntimeError(
                    f"Unable to load data from database ({db_error}) and fallback failed: {exc}"
                )
            raise

    DB.register("raw_df", df)
    DB.execute(
        """
        CREATE OR REPLACE TABLE shifts AS
        SELECT
            *,
            (COALESCE("First Name", '') || ' ' || COALESCE("Last Name", '')) AS "Employee Name",
            (COALESCE("Reg H (e)", 0) +
             COALESCE("OT H (e)", 0) +
             COALESCE("DT H (e)", 0)
            ) AS "Hours Worked"
        FROM raw_df
        """
    )

    return DB.execute("SELECT COUNT(*) FROM shifts").fetchone()[0]


def ensure_data_loaded() -> int:
    global DATA_READY, DATA_LOAD_ERROR

    if DATA_READY:
        return DB.execute("SELECT COUNT(*) FROM shifts").fetchone()[0]

    try:
        row_count = load_data()
    except Exception as exc:  # pragma: no cover - runtime safeguard
        DATA_LOAD_ERROR = str(exc)
        raise

    DATA_READY = True
    DATA_LOAD_ERROR = None
    return row_count


# -------------------------------------------------------------------
# 3. BUILD SQL SYSTEM PROMPT
# -------------------------------------------------------------------
schema_text = "\n".join(
    f"- {col}: {desc}"
    for col, desc in COLUMN_DOCS.items()
    if col not in ("IGNORE_COLUMNS", "RULES")
)

ignore_text = ", ".join(IGNORE_COLUMNS)
rules_text = COLUMN_DOCS["RULES"]

SYSTEM_SQL = f"""
You are an expert DuckDB SQL generator for a staffing agency analytics system.

Table name: shifts

USE THESE COLUMNS:
{schema_text}

IGNORE THESE COLUMNS:
{ignore_text}

IMPORTANT RULES:
{rules_text}

SQL REQUIREMENTS:
- Return ONLY SQL, no explanation.
- Use DuckDB SQL syntax.
- Use "Employee Name" when filtering by employee.
- Use "Hours Worked" for hour calculations.
- Use "Total Bill" for revenue.
- Aggregate with SUM(), COUNT(), GROUP BY when appropriate.
- NEVER reference ignored columns unless directly asked.
- NEVER fabricate columns.
"""


def generate_sql(question: str) -> str:
    response = client.responses.create(
        model="gpt-5-mini",
        input=[
            {"role": "system", "content": SYSTEM_SQL},
            {"role": "user", "content": f"Convert this question into SQL:\n{question}"},
        ],
    )

    return response.output_text.strip()


# -------------------------------------------------------------------
# 4. EXECUTE SQL
# -------------------------------------------------------------------
def run_sql(sql: str) -> pd.DataFrame | Dict[str, Any]:
    try:
        return DB.execute(sql).fetchdf()
    except Exception as exc:  # pragma: no cover - runtime safeguard
        return {"error": str(exc), "sql": sql}


# -------------------------------------------------------------------
# 5. EXPLAIN SQL RESULTS IN NATURAL LANGUAGE
# -------------------------------------------------------------------
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
        content = f"""
The SQL query failed.

SQL:
{sql}

Error:
{df_or_error['error']}

Help the user adjust or clarify their question.
"""
    else:
        df = df_or_error
        table_csv = df.to_csv(index=False)
        content = f"""
User question:
{question}

SQL executed:
{sql}

Result (CSV):
{table_csv}

Explain these results clearly and concisely.
"""

    response = client.responses.create(
        model="gpt-5-mini",
        input=[
            {"role": "system", "content": SYSTEM_EXPLAIN},
            {"role": "user", "content": content},
        ],
    )

    return response.output_text.strip()


# -------------------------------------------------------------------
# 6. API ROUTE
# -------------------------------------------------------------------
class AskRequest(BaseModel):
    question: str


@router.post("/api/ask")
async def api_ask(payload: AskRequest):
    question = payload.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="No question provided")

    try:
        await run_in_threadpool(ensure_data_loaded)
    except Exception as exc:  # pragma: no cover - runtime safeguard
        raise HTTPException(status_code=500, detail=f"Data load failed: {exc}")

    sql = await run_in_threadpool(generate_sql, question)
    result = await run_in_threadpool(run_sql, sql)
    answer = await run_in_threadpool(explain_result, question, sql, result)

    return JSONResponse({"answer": answer, "sql": sql})


# -------------------------------------------------------------------
# 7. FRONT END ROUTE
# -------------------------------------------------------------------
@router.get("/", response_class=HTMLResponse)
async def reports_home(request: Request):
    return templates.TemplateResponse("reports/index.html", {"request": request})


# Local dev mode for standalone running
if __name__ == "__main__":  # pragma: no cover - convenience entrypoint
    import uvicorn
    from fastapi import FastAPI

    dev_app = FastAPI()
    dev_app.include_router(router, prefix="/reports")

    uvicorn.run(dev_app, host="0.0.0.0", port=5000, reload=True)
