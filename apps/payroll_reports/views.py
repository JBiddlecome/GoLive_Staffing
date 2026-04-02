from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

router = APIRouter()
templates = Jinja2Templates(directory="templates")

_STATUS_LABELS = {
    0: "Inactive",
    1: "Active",
    2: "Pending",
    3: "Suspended",
}

_PAYMENT_TYPE_LABELS = {
    1: "Credit Card",
    2: "Check",
    3: "ACH",
    4: "Wire",
    5: "Cash",
}


def _status_label_expr(column: str) -> str:
    return (
        "CASE "
        f"WHEN {column} = 0 THEN 'Inactive' "
        f"WHEN {column} = 1 THEN 'Active' "
        f"WHEN {column} = 2 THEN 'Pending' "
        f"WHEN {column} = 3 THEN 'Suspended' "
        f"WHEN {column} IS NULL THEN 'Not Set' "
        f"ELSE CONCAT('Unknown (', {column}, ')') END"
    )


def _payment_type_label_expr(column: str) -> str:
    return (
        "CASE "
        f"WHEN {column} = 1 THEN 'Credit Card' "
        f"WHEN {column} = 2 THEN 'Check' "
        f"WHEN {column} = 3 THEN 'ACH' "
        f"WHEN {column} = 4 THEN 'Wire' "
        f"WHEN {column} = 5 THEN 'Cash' "
        f"WHEN {column} IS NULL THEN 'Not Set' "
        f"ELSE CONCAT('Unknown (', {column}, ')') END"
    )


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


def _engine():
    return create_engine(_db_url_from_env(), pool_pre_ping=True)


@router.get("", response_class=HTMLResponse)
async def payroll_reports_page(
    request: Request,
    status: int | None = Query(default=None),
):
    status_expr = _status_label_expr("c.status")
    payment_expr = _payment_type_label_expr("c.payment_type")

    sql = f"""
        SELECT
            c.name,
            {status_expr} AS status_text,
            {payment_expr} AS payment_type_text,
            c.pay_notes,
            COALESCE(nte.net_terms, 'Not Set') AS net_terms_entry_text
        FROM client c
        LEFT JOIN net_terms_entry nte
          ON nte.id = c.net_terms_entry_id
        WHERE c.deleted_at IS NULL
          AND (:status_filter IS NULL OR c.status = :status_filter)
        ORDER BY c.name ASC
        LIMIT 10000
    """

    options_sql = text(
        """
        SELECT DISTINCT status
        FROM client
        WHERE deleted_at IS NULL
        ORDER BY status
        """
    )

    engine = _engine()
    try:
        with engine.begin() as connection:
            rows = connection.execute(text(sql), {"status_filter": status}).mappings().all()
            raw_statuses = connection.execute(options_sql).scalars().all()
    finally:
        engine.dispose()

    reports: list[dict[str, Any]] = [dict(row) for row in rows]
    status_options = [
        {
            "value": value,
            "label": _STATUS_LABELS.get(value, f"Unknown ({value})") if value is not None else "Not Set",
        }
        for value in raw_statuses
    ]

    return templates.TemplateResponse(
        "apps/payroll_reports.html",
        {
            "request": request,
            "reports": reports,
            "status_options": status_options,
            "selected_status": status,
        },
    )
