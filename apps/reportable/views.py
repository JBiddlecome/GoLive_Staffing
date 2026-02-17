from __future__ import annotations

import os
from io import BytesIO
from typing import Any

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL

router = APIRouter()
templates = Jinja2Templates(directory="templates")


class ExportPayload(BaseModel):
    table: str = Field(..., min_length=1)
    columns: list[str] = Field(default_factory=list)
    date_column: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    limit: int = Field(default=50000, ge=1, le=100000)


def _db_url_from_env() -> URL:
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = int(os.getenv("DB_PORT", "3306"))

    missing = [
        env_name
        for env_name, value in (
            ("DB_HOST", host),
            ("DB_NAME", name),
            ("DB_USER", user),
            ("DB_PASSWORD", password),
        )
        if not value
    ]
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"Missing required database environment variables: {', '.join(missing)}",
        )

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


def _list_tables() -> list[str]:
    engine = _engine()
    try:
        inspector = inspect(engine)
        return sorted(inspector.get_table_names())
    finally:
        engine.dispose()


def _list_columns(table: str) -> list[dict[str, Any]]:
    engine = _engine()
    try:
        inspector = inspect(engine)
        table_names = set(inspector.get_table_names())
        if table not in table_names:
            raise HTTPException(status_code=404, detail=f"Unknown table '{table}'.")

        columns = inspector.get_columns(table)
        return [
            {
                "name": col["name"],
                "type": str(col.get("type", "")),
                "nullable": bool(col.get("nullable", True)),
            }
            for col in columns
        ]
    finally:
        engine.dispose()


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


@router.get("")
async def reportable_page(request: Request):
    return templates.TemplateResponse("apps/reportable.html", {"request": request})


@router.get("/schema/tables")
async def reportable_schema_tables() -> JSONResponse:
    return JSONResponse({"tables": _list_tables()})


@router.get("/schema/columns")
async def reportable_schema_columns(table: str = Query(..., min_length=1)) -> JSONResponse:
    return JSONResponse({"table": table, "columns": _list_columns(table)})


@router.post("/export")
async def reportable_export(payload: ExportPayload) -> StreamingResponse:
    engine = _engine()
    try:
        inspector = inspect(engine)
        table_names = set(inspector.get_table_names())
        if payload.table not in table_names:
            raise HTTPException(status_code=404, detail=f"Unknown table '{payload.table}'.")

        columns_meta = inspector.get_columns(payload.table)
        allowed_columns = {col["name"] for col in columns_meta}

        selected_columns = payload.columns or sorted(allowed_columns)
        invalid_columns = sorted(set(selected_columns) - allowed_columns)
        if invalid_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid columns for table '{payload.table}': {', '.join(invalid_columns)}",
            )

        sql_columns = ", ".join(_quote_identifier(col) for col in selected_columns)
        sql = f"SELECT {sql_columns} FROM {_quote_identifier(payload.table)}"
        params: dict[str, Any] = {}

        if payload.date_column and (payload.start_date or payload.end_date):
            if payload.date_column not in allowed_columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid date_column '{payload.date_column}' for table '{payload.table}'.",
                )

            filters: list[str] = []
            if payload.start_date:
                filters.append(f"{_quote_identifier(payload.date_column)} >= :start_date")
                params["start_date"] = payload.start_date
            if payload.end_date:
                filters.append(f"{_quote_identifier(payload.date_column)} <= :end_date")
                params["end_date"] = payload.end_date

            if filters:
                sql += " WHERE " + " AND ".join(filters)

        sql += " LIMIT :limit"
        params["limit"] = payload.limit

        with engine.begin() as connection:
            dataframe = pd.read_sql(text(sql), connection, params=params)
    finally:
        engine.dispose()

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        dataframe.to_excel(writer, index=False, sheet_name="report")
    output.seek(0)

    filename = f"{payload.table}_report.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )
