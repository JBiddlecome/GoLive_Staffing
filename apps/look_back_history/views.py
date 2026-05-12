from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from apps.profit_tracker.billing import total_bill_by_client

router = APIRouter()
templates = Jinja2Templates(directory="templates")

CLIENT_STATUS_LABELS = {
    0: "Terminated",
    1: "Active",
    3: "Prospect",
    4: "Candidate Partner",
    10: "Inactive 60 days",
    11: "Inactive 180 days",
    12: "Inactive 365 days",
}


class LookBackPayload(BaseModel):
    start_date: str
    end_date: str


def _db_url_from_env() -> URL:
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = int(os.getenv("DB_PORT", "3306"))

    missing = [
        env_name
        for env_name, value in (
            ("DB_HOST", host),
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


def parse_iso_date(value: str, field_name: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=400, detail=f"{field_name} must be in YYYY-MM-DD format."
        )


def subtract_calendar_year(value: date) -> date:
    try:
        return value.replace(year=value.year - 1)
    except ValueError:
        return value.replace(year=value.year - 1, day=28)


def _fetch_order_summary(engine: Any, start_date: date, end_date: date) -> pd.DataFrame:
    sql = text(
        """
        SELECT
            c.client_id,
            c.name AS client_name,
            c.status AS client_status,
            COALESCE(NULLIF(p.description, ''), NULLIF(sp.additional_title, ''), 'Unknown') AS position_name,
            COUNT(DISTINCT sp.shift_position_id) AS order_lines,
            SUM(COALESCE(sp.count, 0)) AS staff_requested
        FROM client c
        JOIN event e ON c.client_id = e.client_id
        JOIN shift s ON e.event_id = s.event_id
        JOIN shift_position sp ON s.shift_id = sp.shift_id
        LEFT JOIN position p ON sp.position_id = p.position_id
        WHERE c.deleted_at IS NULL
          AND e.deleted_at IS NULL
          AND s.deleted_at IS NULL
          AND sp.deleted_at IS NULL
          AND e.date >= :start_date
          AND e.date <= :end_date
        GROUP BY c.client_id, c.name, c.status, position_name
        ORDER BY c.name, order_lines DESC, position_name
        """
    )
    with engine.begin() as connection:
        return pd.read_sql(
            sql,
            connection,
            params={"start_date": start_date, "end_date": end_date},
        )


def _fetch_current_activity(engine: Any, start_date: date, end_date: date) -> pd.DataFrame:
    sql = text(
        """
        SELECT
            c.client_id,
            COUNT(DISTINCT sp.shift_position_id) AS current_order_lines_last_30_days,
            SUM(COALESCE(sp.count, 0)) AS current_staff_requested_last_30_days,
            MAX(e.date) AS last_current_shift_date
        FROM client c
        JOIN event e ON c.client_id = e.client_id
        JOIN shift s ON e.event_id = s.event_id
        JOIN shift_position sp ON s.shift_id = sp.shift_id
        WHERE c.deleted_at IS NULL
          AND e.deleted_at IS NULL
          AND s.deleted_at IS NULL
          AND sp.deleted_at IS NULL
          AND e.date >= :start_date
          AND e.date <= :end_date
        GROUP BY c.client_id
        """
    )
    with engine.begin() as connection:
        return pd.read_sql(
            sql,
            connection,
            params={"start_date": start_date, "end_date": end_date},
        )


def build_client_rows(
    orders_df: pd.DataFrame,
    current_df: pd.DataFrame,
    revenue_by_client: dict[int, float],
) -> list[dict[str, Any]]:
    if orders_df.empty:
        return []

    current_lookup: dict[int, dict[str, Any]] = {}
    if not current_df.empty:
        for row in current_df.to_dict(orient="records"):
            current_lookup[int(row["client_id"])] = row

    rows: list[dict[str, Any]] = []
    for client_id, group in orders_df.groupby("client_id", sort=False):
        client_id_int = int(client_id)
        current = current_lookup.get(client_id_int, {})
        current_order_lines = int(
            current.get("current_order_lines_last_30_days") or 0
        )
        current_staff_requested = int(
            current.get("current_staff_requested_last_30_days") or 0
        )
        last_current_shift_date = current.get("last_current_shift_date")
        if pd.notna(last_current_shift_date) and last_current_shift_date is not None:
            last_current_shift_date = str(last_current_shift_date)
        else:
            last_current_shift_date = None

        positions = [
            {
                "position": str(row["position_name"]),
                "order_lines": int(row["order_lines"] or 0),
                "staff_requested": int(row["staff_requested"] or 0),
            }
            for row in group.sort_values(
                by=["order_lines", "position_name"], ascending=[False, True]
            ).to_dict(orient="records")
        ]

        status = "active" if current_order_lines > 0 else "inactive"
        raw_client_status = group.iloc[0].get("client_status")
        try:
            client_status_code = (
                int(raw_client_status) if pd.notna(raw_client_status) else None
            )
        except (TypeError, ValueError):
            client_status_code = None
        client_status_label = CLIENT_STATUS_LABELS.get(
            client_status_code,
            f"Status {client_status_code}" if client_status_code is not None else "Unknown",
        )
        rows.append(
            {
                "client_id": client_id_int,
                "client_name": str(group.iloc[0]["client_name"]),
                "client_status": client_status_label,
                "client_status_code": client_status_code,
                "status": status,
                "prior_year_order_lines": int(group["order_lines"].sum()),
                "prior_year_staff_requested": int(group["staff_requested"].sum()),
                "prior_year_revenue": round(
                    float(revenue_by_client.get(client_id_int, 0.0)), 2
                ),
                "positions_ordered": positions,
                "current_order_lines_last_30_days": current_order_lines,
                "current_staff_requested_last_30_days": current_staff_requested,
                "last_current_shift_date": last_current_shift_date,
            }
        )

    rows.sort(
        key=lambda row: (
            -row["prior_year_order_lines"],
            -row["prior_year_revenue"],
            row["client_name"].lower(),
        )
    )
    return rows


@router.get("", response_class=HTMLResponse)
async def look_back_history_page(request: Request):
    today = date.today()
    default_end = today + timedelta(days=30)
    return templates.TemplateResponse(
        "apps/look_back_history.html",
        {
            "request": request,
            "start_date": today.isoformat(),
            "end_date": default_end.isoformat(),
        },
    )


@router.post("/api/data")
async def get_look_back_history(payload: LookBackPayload):
    selected_start = parse_iso_date(payload.start_date, "start_date")
    selected_end = parse_iso_date(payload.end_date, "end_date")
    if selected_start > selected_end:
        raise HTTPException(status_code=400, detail="start_date must be before end_date.")

    lookback_start = subtract_calendar_year(selected_start)
    lookback_end = subtract_calendar_year(selected_end)
    today = date.today()
    activity_start = today - timedelta(days=30)

    engine = _engine()
    try:
        orders_df = _fetch_order_summary(engine, lookback_start, lookback_end)
        current_df = _fetch_current_activity(engine, activity_start, today)
        revenue_by_client = total_bill_by_client(
            engine, lookback_start.isoformat(), lookback_end.isoformat()
        )
    finally:
        engine.dispose()

    rows = build_client_rows(orders_df, current_df, revenue_by_client)
    inactive_clients = [row for row in rows if row["status"] == "inactive"]
    active_clients = [row for row in rows if row["status"] == "active"]

    summary = {
        "total_clients": len(rows),
        "active_clients": len(active_clients),
        "inactive_clients": len(inactive_clients),
        "total_order_lines": sum(row["prior_year_order_lines"] for row in rows),
        "total_revenue": round(sum(row["prior_year_revenue"] for row in rows), 2),
    }

    return JSONResponse(
        {
            "selected_range": {
                "start": selected_start.isoformat(),
                "end": selected_end.isoformat(),
            },
            "lookback_range": {
                "start": lookback_start.isoformat(),
                "end": lookback_end.isoformat(),
            },
            "activity_range": {
                "start": activity_start.isoformat(),
                "end": today.isoformat(),
            },
            "summary": summary,
            "inactive_clients": inactive_clients,
            "active_clients": active_clients,
        }
    )
