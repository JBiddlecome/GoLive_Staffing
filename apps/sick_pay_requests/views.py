from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
import requests

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
            e.last_name,
            e.email
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
                item["email"] = item.get("email") or ""
                item["emailed"] = saved_state.get("emailed", False)
                item["emailed_at"] = saved_state.get("emailed_at", "")
                item["emailed_message"] = saved_state.get("emailed_message", "")

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


class EmailPayload(BaseModel):
    id: int
    employee_email: str
    first_name: str
    subject: str
    message: str


def send_sick_pay_email(employee_email: str, subject: str, message: str) -> bool:
    sender_email = "golive@culinarystaffing.com"
    tenant_id = os.getenv("O365_TENANT_ID")
    client_id = os.getenv("O365_CLIENT_ID")
    client_secret = os.getenv("O365_CLIENT_SECRET")
    
    if not all([tenant_id, client_id, client_secret, employee_email]):
        print("Skipping email: Microsoft 365 OAuth credentials missing or employee email is empty.")
        return False
        
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default"
    }
    
    try:
        r = requests.post(token_url, data=token_data)
        r.raise_for_status()
        access_token = r.json().get("access_token")
    except Exception as e:
        print(f"Failed to authenticate with Microsoft Graph: {e}")
        return False

    html_content = message.replace("\n", "<br>")
    
    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
        <h2 style="color: #059669;">Sick Pay Request Update</h2>
        <p>{html_content}</p>
        <p>Best regards,<br>The Culinary Staffing Team</p>
      </body>
    </html>
    """
    
    email_msg = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "HTML",
                "content": html_body
            },
            "toRecipients": [
                {"emailAddress": {"address": employee_email}}
            ]
        },
        "saveToSentItems": "true"
    }

    send_url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        send_res = requests.post(send_url, headers=headers, json=email_msg)
        send_res.raise_for_status()
        return True
    except Exception as e:
        print(f"Failed to send email via MS Graph: {e}")
        return False


@router.post("/api/send-email")
async def send_email_endpoint(payload: EmailPayload):
    try:
        if not payload.employee_email:
            raise HTTPException(status_code=400, detail="Employee has no email address in database.")
            
        success = send_sick_pay_email(payload.employee_email, payload.subject, payload.message)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to send email via Microsoft Graph API.")
        
        ticket_id_str = str(payload.id)
        persistence = _load_persistence()
        
        existing = persistence.get(ticket_id_str, {})
        existing["emailed"] = True
        existing["emailed_at"] = datetime.now().strftime("%m/%d/%Y %I:%M %p")
        existing["emailed_message"] = payload.message
        
        # Append a log entry to notes for traceability
        note_log = f"\n[{datetime.now().strftime('%m/%d/%Y %I:%M %p')}] Emailed employee: \"{payload.message}\""
        current_note = existing.get("note") or ""
        new_note = (current_note.rstrip() + note_log).strip()
        existing["note"] = new_note
        existing["updated_at"] = datetime.now().isoformat()
        
        persistence[ticket_id_str] = existing
        _save_persistence(persistence)
        
        # Update notes in employee_other_work table
        engine = _engine()
        try:
            with engine.begin() as connection:
                connection.execute(
                    text("UPDATE employee_other_work SET notes = :notes WHERE id = :id"),
                    {"notes": new_note, "id": payload.id}
                )
        finally:
            engine.dispose()
            
        return {
            "status": "success",
            "emailed_at": existing["emailed_at"],
            "new_note": new_note
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
