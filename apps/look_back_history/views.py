from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
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

LOOKBACK_YEARS = 5


def _resolve_data_dir() -> Path:
    env_dir = os.getenv("DATA_DIR") or os.getenv("RENDER_DISK_PATH")
    if env_dir:
        return Path(env_dir)
    if Path("/var/data").exists():
        return Path("/var/data")
    if any(os.getenv(e) for e in ("RENDER", "RENDER_SERVICE_ID")):
        return Path("/var/data")
    return Path("data")

NOTES_FILE = _resolve_data_dir() / "lookback_history_notes.json"
NOTES_FILE.parent.mkdir(parents=True, exist_ok=True)


def _load_notes() -> dict[str, str]:
    if not NOTES_FILE.exists():
        return {}
    try:
        with NOTES_FILE.open("r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_notes(notes: dict[str, str]):
    NOTES_FILE.parent.mkdir(parents=True, exist_ok=True)
    with NOTES_FILE.open("w") as f:
        json.dump(notes, f, indent=2)


class LookBackPayload(BaseModel):
    start_date: str
    end_date: str


class NotePayload(BaseModel):
    note: str


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


def subtract_years(value: date, years: int) -> date:
    """Subtract N calendar years from a date, handling Feb-29 edge cases."""
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def build_year_ranges(selected_start: date, selected_end: date) -> list[tuple[int, date, date]]:
    """Return a list of (year_offset, start, end) for each of the 5 prior years."""
    ranges = []
    for offset in range(1, LOOKBACK_YEARS + 1):
        s = subtract_years(selected_start, offset)
        e = subtract_years(selected_end, offset)
        ranges.append((offset, s, e))
    return ranges


def _fetch_order_summary_for_range(
    engine: Any, start_date: date, end_date: date
) -> pd.DataFrame:
    sql = text(
        """
        SELECT
            c.client_id,
            c.name AS client_name,
            c.status AS client_status,
            CONCAT_WS(' ', u_mgr.first_name, u_mgr.last_name) AS staffing_manager_name,
            COALESCE(NULLIF(p.description, ''), NULLIF(sp.additional_title, ''), 'Unknown') AS position_name,
            COUNT(DISTINCT sp.shift_position_id) AS order_lines,
            SUM(COALESCE(sp.count, 0)) AS staff_requested
        FROM client c
        JOIN event e ON c.client_id = e.client_id
        JOIN venue v ON e.venue_id = v.venue_id
        LEFT JOIN user u_mgr ON v.staffing_manager_id = u_mgr.id
        JOIN shift s ON e.event_id = s.event_id
        JOIN shift_position sp ON s.shift_id = sp.shift_id
        LEFT JOIN position p ON sp.position_id = p.position_id
        WHERE c.deleted_at IS NULL
          AND e.deleted_at IS NULL
          AND v.deleted_at IS NULL
          AND s.deleted_at IS NULL
          AND sp.deleted_at IS NULL
          AND e.date >= :start_date
          AND e.date <= :end_date
        GROUP BY c.client_id, c.name, c.status, staffing_manager_name, position_name
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
    year_data: list[tuple[int, date, date, pd.DataFrame, dict[int, float]]],
    current_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    """
    Aggregate 5-year data per client.

    year_data: list of (offset, start, end, orders_df, revenue_by_client)
    Sorting: clients active in more years rank higher; within same year-count,
             most recent year(s) present break the tie, then total order lines.
    """
    # Build a unified client registry: client_id -> base info
    client_registry: dict[int, dict[str, Any]] = {}
    # Per-year order lines and revenue: client_id -> {offset: {lines, revenue, staff}}
    client_year_stats: dict[int, dict[int, dict]] = {}

    for offset, lb_start, lb_end, orders_df, revenue_by_client in year_data:
        if orders_df.empty:
            continue
        year_label = lb_start.year  # e.g. 2025, 2024 …

        for client_id, group in orders_df.groupby("client_id", sort=False):
            client_id_int = int(client_id)

            if client_id_int not in client_registry:
                raw_status = group.iloc[0].get("client_status")
                try:
                    status_code = int(raw_status) if pd.notna(raw_status) else None
                except (TypeError, ValueError):
                    status_code = None
                client_registry[client_id_int] = {
                    "client_id": client_id_int,
                    "client_name": str(group.iloc[0]["client_name"]),
                    "staffing_manager_name": str(group.iloc[0].get("staffing_manager_name", "")),
                    "client_status_code": status_code,
                    "client_status": CLIENT_STATUS_LABELS.get(
                        status_code,
                        f"Status {status_code}" if status_code is not None else "Unknown",
                    ),
                }
                client_year_stats[client_id_int] = {}

            total_lines = int(group["order_lines"].sum())
            total_staff = int(group["staff_requested"].sum())
            revenue = float(revenue_by_client.get(client_id_int, 0.0))

            # Aggregate positions across all years
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

            client_year_stats[client_id_int][offset] = {
                "year_label": year_label,
                "order_lines": total_lines,
                "staff_requested": total_staff,
                "revenue": revenue,
                "positions": positions,
            }

    # Build current activity lookup
    current_lookup: dict[int, dict[str, Any]] = {}
    if not current_df.empty:
        for row in current_df.to_dict(orient="records"):
            current_lookup[int(row["client_id"])] = row

    rows: list[dict[str, Any]] = []
    for client_id_int, info in client_registry.items():
        year_stats = client_year_stats[client_id_int]
        years_active = len(year_stats)  # how many of the 5 years had orders

        # Aggregate totals across all 5 years
        total_order_lines = sum(y["order_lines"] for y in year_stats.values())
        total_staff_requested = sum(y["staff_requested"] for y in year_stats.values())
        total_revenue = sum(y["revenue"] for y in year_stats.values())

        # Build per-year breakdown (offset 1 = most recent prior year)
        year_breakdown = []
        for offset in sorted(year_stats.keys()):
            ys = year_stats[offset]
            year_breakdown.append({
                "year": ys["year_label"],
                "order_lines": ys["order_lines"],
                "staff_requested": ys["staff_requested"],
                "revenue": round(ys["revenue"], 2),
            })

        # Aggregate positions across all years, merging duplicates
        position_map: dict[str, dict] = {}
        for ys in year_stats.values():
            for pos in ys["positions"]:
                key = pos["position"]
                if key not in position_map:
                    position_map[key] = {"position": key, "order_lines": 0, "staff_requested": 0}
                position_map[key]["order_lines"] += pos["order_lines"]
                position_map[key]["staff_requested"] += pos["staff_requested"]
        positions_ordered = sorted(
            position_map.values(), key=lambda x: -x["order_lines"]
        )

        # Current (last 30 days) activity
        current = current_lookup.get(client_id_int, {})
        current_order_lines = int(current.get("current_order_lines_last_30_days") or 0)
        current_staff_requested = int(current.get("current_staff_requested_last_30_days") or 0)
        last_current_shift_date = current.get("last_current_shift_date")
        if pd.notna(last_current_shift_date) and last_current_shift_date is not None:
            last_current_shift_date = str(last_current_shift_date)
        else:
            last_current_shift_date = None

        status = "active" if current_order_lines > 0 else "inactive"

        # Which of the 5 years had orders (most recent = offset 1)
        years_with_orders = sorted(year_stats.keys())  # ascending offsets

        rows.append({
            "client_id": client_id_int,
            "client_name": info["client_name"],
            "staffing_manager_name": info["staffing_manager_name"],
            "client_status": info["client_status"],
            "client_status_code": info["client_status_code"],
            "status": status,
            "years_active": years_active,
            "years_with_orders": years_with_orders,  # list of offsets (1=most recent)
            "year_breakdown": year_breakdown,
            "total_order_lines": total_order_lines,
            "total_staff_requested": total_staff_requested,
            "total_revenue": round(total_revenue, 2),
            "positions_ordered": positions_ordered,
            "current_order_lines_last_30_days": current_order_lines,
            "current_staff_requested_last_30_days": current_staff_requested,
            "last_current_shift_date": last_current_shift_date,
        })

    # Sort: most years active first, then most recent year present (offset 1 < offset 5),
    # then total order lines descending, then name ascending
    def sort_key(row):
        # Most years active first (negate for descending)
        # Tie-break: was there activity in the most recent prior year (offset 1)?
        most_recent_year_present = 1 if 1 in row["years_with_orders"] else 0
        return (
            -row["years_active"],
            -most_recent_year_present,
            -row["total_order_lines"],
            row["client_name"].lower(),
        )

    rows.sort(key=sort_key)
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

    year_ranges = build_year_ranges(selected_start, selected_end)
    today = date.today()
    activity_start = today - timedelta(days=30)

    engine = _engine()
    try:
        year_data = []
        for offset, lb_start, lb_end in year_ranges:
            orders_df = _fetch_order_summary_for_range(engine, lb_start, lb_end)
            revenue_by_client = total_bill_by_client(
                engine, lb_start.isoformat(), lb_end.isoformat()
            )
            year_data.append((offset, lb_start, lb_end, orders_df, revenue_by_client))

        current_df = _fetch_current_activity(engine, activity_start, today)
    finally:
        engine.dispose()

    rows = build_client_rows(year_data, current_df)
    inactive_clients = [row for row in rows if row["status"] == "inactive"]
    active_clients = [row for row in rows if row["status"] == "active"]

    summary = {
        "total_clients": len(rows),
        "active_clients": len(active_clients),
        "inactive_clients": len(inactive_clients),
        "total_order_lines": sum(row["total_order_lines"] for row in rows),
        "total_revenue": round(sum(row["total_revenue"] for row in rows), 2),
    }

    # Build lookback ranges info for display
    lookback_ranges = [
        {"year": lb_start.year, "start": lb_start.isoformat(), "end": lb_end.isoformat()}
        for _, lb_start, lb_end, _, _ in year_data
    ]

    return JSONResponse(
        {
            "selected_range": {
                "start": selected_start.isoformat(),
                "end": selected_end.isoformat(),
            },
            "lookback_ranges": lookback_ranges,
            "activity_range": {
                "start": activity_start.isoformat(),
                "end": today.isoformat(),
            },
            "summary": summary,
            "inactive_clients": inactive_clients,
            "active_clients": active_clients,
        }
    )


@router.get("/api/contacts/{client_id}")
async def get_client_contacts(client_id: int):
    engine = _engine()
    sql = text(
        """
        SELECT name, title, email, phone, mobile
        FROM client_contact
        WHERE client_id = :client_id
        ORDER BY preferred DESC, name ASC
        """
    )
    try:
        with engine.begin() as connection:
            df = pd.read_sql(sql, connection, params={"client_id": client_id})
            return df.to_dict(orient="records")
    finally:
        engine.dispose()


@router.get("/api/notes/{client_id}")
async def get_client_note(client_id: int):
    notes = _load_notes()
    return {"note": notes.get(str(client_id), "")}


@router.post("/api/notes/{client_id}")
async def save_client_note(client_id: int, payload: NotePayload):
    notes = _load_notes()
    notes[str(client_id)] = payload.note
    _save_notes(notes)
    return {"status": "success"}


@router.get("/api/chart/{client_id}")
async def get_client_chart_data(client_id: int):
    engine = _engine()
    sql = text(
        """
        SELECT 
            DATE_FORMAT(e.date, '%Y-%m') AS month,
            COUNT(t.timesheet_id) AS shift_count
        FROM event e
        JOIN timesheet t ON t.event_id = e.event_id
        WHERE e.client_id = :client_id
          AND e.deleted_at IS NULL
          AND e.date >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)
          AND e.date <= CURDATE()
        GROUP BY DATE_FORMAT(e.date, '%Y-%m')
        ORDER BY month ASC
        """
    )
    try:
        with engine.begin() as connection:
            df = pd.read_sql(sql, connection, params={"client_id": client_id})
            
            today = date.today()
            start_date = subtract_years(today, 5)
            # Create a full sequence of months to ensure no gaps
            all_months = pd.date_range(
                start=start_date.replace(day=1), 
                end=today, 
                freq='MS'
            ).strftime('%Y-%m').tolist()
            
            data_dict = dict(zip(df['month'], df['shift_count']))
            
            labels = []
            counts = []
            for m in all_months:
                labels.append(m)
                counts.append(int(data_dict.get(m, 0)))
                
            return {"labels": labels, "counts": counts}
    finally:
        engine.dispose()
