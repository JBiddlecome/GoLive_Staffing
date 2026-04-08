from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import URL

router = APIRouter()
templates = Jinja2Templates(directory="templates")

_STATUS_LABELS = {
    0: "Terminated",
    1: "Active",
    3: "Prospect",
    4: "Candidate Partner",
    10: "Inactive 60",
    11: "Inactive 180",
    12: "Inactive 365",
}

_PAYMENT_TYPE_LABELS = {
    1: "Credit Card",
    2: "Invoice",
}


def _status_label_expr(column: str) -> str:
    return (
        "CASE "
        f"WHEN {column} = 0 THEN 'Terminated' "
        f"WHEN {column} = 1 THEN 'Active' "
        f"WHEN {column} = 3 THEN 'Prospect' "
        f"WHEN {column} = 4 THEN 'Candidate Partner' "
        f"WHEN {column} = 10 THEN 'Inactive 60' "
        f"WHEN {column} = 11 THEN 'Inactive 180' "
        f"WHEN {column} = 12 THEN 'Inactive 365' "
        f"WHEN {column} IS NULL THEN 'Not Set' "
        f"ELSE CONCAT('Unknown (', {column}, ')') END"
    )


def _payment_type_label_expr(column: str) -> str:
    return (
        "CASE "
        f"WHEN {column} = 1 THEN 'Credit Card' "
        f"WHEN {column} = 2 THEN 'Invoice' "
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
    statuses: list[int] = Query(default=[]),
    payment_types: list[int] = Query(default=[]),
):
    status_expr = _status_label_expr("c.status")
    payment_expr = _payment_type_label_expr("c.payment_type")

    sql = text(
        f"""
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
          AND (:apply_status_filter = 0 OR c.status IN :statuses)
          AND (:apply_payment_filter = 0 OR c.payment_type IN :payment_types)
        ORDER BY c.name ASC
        LIMIT 10000
    """
    ).bindparams(
        bindparam("statuses", expanding=True),
        bindparam("payment_types", expanding=True),
    )

    engine = _engine()
    try:
        with engine.begin() as connection:
            rows = connection.execute(
                sql,
                {
                    "apply_status_filter": 1 if statuses else 0,
                    "statuses": statuses,
                    "apply_payment_filter": 1 if payment_types else 0,
                    "payment_types": payment_types,
                },
            ).mappings().all()
    finally:
        engine.dispose()

    reports: list[dict[str, Any]] = [dict(row) for row in rows]
    status_options = [{"value": value, "label": label} for value, label in _STATUS_LABELS.items()]
    payment_type_options = [{"value": value, "label": label} for value, label in _PAYMENT_TYPE_LABELS.items()]

    return templates.TemplateResponse(
        "apps/payroll_reports.html",
        {
            "request": request,
            "reports": reports,
            "status_options": status_options,
            "payment_type_options": payment_type_options,
            "selected_statuses": statuses,
            "selected_payment_types": payment_types,
        },
    )
