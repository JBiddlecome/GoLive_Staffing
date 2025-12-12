from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import duckdb
import pandas as pd
import requests
from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from openai import OpenAI
from pydantic import BaseModel

from .schema_docs import COLUMN_DOCS

router = APIRouter()
templates = Jinja2Templates(directory="templates")
client = OpenAI()

ONEDRIVE_URL = os.environ.get("ONEDRIVE_CSV_URL")

# Paths
MODULE_BASE_DIR = Path(__file__).resolve().parents[2]
RENDER_DATA_DIR = Path("/opt/render/project/src/data")
if any(os.getenv(env_var) for env_var in ("RENDER", "RENDER_SERVICE_ID", "RENDER_EXTERNAL_URL")):
    DATA_DIR = RENDER_DATA_DIR
elif RENDER_DATA_DIR.exists():
    DATA_DIR = RENDER_DATA_DIR
else:
    DATA_DIR = MODULE_BASE_DIR / "data"

LOCAL_CSV_PATH = DATA_DIR / "shifts.csv"
DB = duckdb.connect(database=":memory:")
IGNORE_COLUMNS = COLUMN_DOCS["IGNORE_COLUMNS"]
DATA_READY = False
DATA_LOAD_ERROR: str | None = None


# -------------------------------------------------------------------
# 1. DOWNLOAD CSV FROM ONEDRIVE
# -------------------------------------------------------------------
def download_csv_from_onedrive() -> None:
    if not ONEDRIVE_URL:
        raise RuntimeError("ONEDRIVE_CSV_URL environment variable not set.")

    response = requests.get(ONEDRIVE_URL, timeout=30)
    response.raise_for_status()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_CSV_PATH.write_bytes(response.content)


# -------------------------------------------------------------------
# 2. LOAD DATA INTO DUCKDB
# -------------------------------------------------------------------
def load_data() -> int:
    download_csv_from_onedrive()
    df = pd.read_csv(LOCAL_CSV_PATH)

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
