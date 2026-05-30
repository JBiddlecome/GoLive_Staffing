from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

router = APIRouter()
templates = Jinja2Templates(directory="templates")


def _resolve_data_dir() -> Path:
    env_dir = os.getenv("DATA_DIR") or os.getenv("RENDER_DISK_PATH")
    if env_dir:
        return Path(env_dir)
    if Path("/var/data").exists():
        return Path("/var/data")
    if any(os.getenv(e) for e in ("RENDER", "RENDER_SERVICE_ID")):
        return Path("/var/data")
    return Path("data")


PERSISTENCE_FILE = _resolve_data_dir() / "sick_pay_requests.json"
PERSISTENCE_FILE.parent.mkdir(parents=True, exist_ok=True)


def _load_persistence() -> dict[str, dict[str, Any]]:
    if not PERSISTENCE_FILE.exists():
        return {}
    try:
        with PERSISTENCE_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_persistence(data: dict[str, dict[str, Any]]) -> None:
    PERSISTENCE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with PERSISTENCE_FILE.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


class SavePayload(BaseModel):
    id: int
    note: str
    completed: bool


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


@router.get("", response_class=HTMLResponse)
async def sick_pay_requests_page(request: Request):
    return templates.TemplateResponse(
        "apps/sick_pay_requests.html",
        {
            "request": request,
            "start_date": "2026-05-25",
        },
    )


@router.get("/data")
async def get_sick_pay_requests_data(
    start_date: str = Query("2026-05-25"),
    end_date: str = Query(None),
):
    # Enforce hard filter on 05/25/2026 onwards
    if not start_date or start_date < "2026-05-25":
        start_date = "2026-05-25"

    engine = _engine()
    persistence = _load_persistence()

    sql_parts = ["""
        SELECT 
            eow.id,
            eow.created_at,
            eow.non_work_hours,
            eow.employee_id,
            eow.notes,
            e.payroll_id,
            e.first_name,
            e.last_name
        FROM employee_other_work eow
        JOIN employee e ON eow.employee_id = e.employee_id
        WHERE eow.other_work_type_id = 8
          AND DATE(eow.created_at) >= :start_date
    """]
    params = {"start_date": start_date}

    if end_date:
        sql_parts.append("  AND DATE(eow.created_at) <= :end_date")
        params["end_date"] = end_date

    sql_parts.append("ORDER BY eow.created_at DESC")
    sql = text("\n".join(sql_parts))

    try:
        with engine.begin() as connection:
            results = connection.execute(sql, params).mappings().all()

            data = []
            for row in results:
                item = dict(row)
                ticket_id_str = str(item["id"])

                # Get persistent data from JSON file
                saved_state = persistence.get(ticket_id_str, {})
                item["note"] = item.get("notes") or saved_state.get("note", "")
                item["completed"] = saved_state.get("completed", False)
                item["completed_at"] = saved_state.get("completed_at", "")

                # Format created_at date nicely
                if isinstance(item["created_at"], datetime):
                    item["created_at_raw"] = item["created_at"].isoformat()
                    item["created_at"] = item["created_at"].strftime("%m/%d/%Y %I:%M %p")
                elif item["created_at"]:
                    item["created_at_raw"] = str(item["created_at"])
                    item["created_at"] = str(item["created_at"])
                else:
                    item["created_at_raw"] = ""
                    item["created_at"] = ""

                # Format hours beautifully (e.g. 5.5 instead of 5.50)
                if item["non_work_hours"] is not None:
                    item["non_work_hours"] = float(item["non_work_hours"])
                else:
                    item["non_work_hours"] = 0.0

                data.append(item)

            return JSONResponse({"data": data})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()


@router.post("/api/save")
async def save_ticket_state(payload: SavePayload):
    try:
        ticket_id_str = str(payload.id)
        persistence = _load_persistence()

        existing = persistence.get(ticket_id_str, {})
        was_completed = existing.get("completed", False)

        completed_at = existing.get("completed_at", "")
        if payload.completed and not was_completed:
            completed_at = datetime.now().strftime("%m/%d/%Y %I:%M %p")
        elif not payload.completed:
            completed_at = ""

        persistence[ticket_id_str] = {
            "note": payload.note,
            "completed": payload.completed,
            "completed_at": completed_at,
            "updated_at": datetime.now().isoformat(),
        }

        _save_persistence(persistence)

        # Update notes in the database table employee_other_work
        engine = _engine()
        try:
            with engine.begin() as connection:
                connection.execute(
                    text("UPDATE employee_other_work SET notes = :notes WHERE id = :id"),
                    {"notes": payload.note, "id": payload.id}
                )
        finally:
            engine.dispose()

        return {"status": "success", "completed_at": completed_at}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
