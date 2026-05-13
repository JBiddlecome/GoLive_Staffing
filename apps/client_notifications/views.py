from fastapi import APIRouter, Request, HTTPException, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
import os
import asyncio
from datetime import datetime, timedelta
from apps.client_notifications.utils import (
    _engine, _get_setting, _set_setting, 
    get_prior_month_data, _ensure_state_table
)

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# --- State Management Wrappers ---
def ensure_state_table_wrapper():
    # Move actual table creation logic to utils if needed, or keep here
    # For now, it's in utils.py (Wait, I need to make sure I moved _ensure_state_table to utils.py)
    _ensure_state_table()

# --- Routes ---

@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    _ensure_state_table()
    settings = {
        "global_send_enabled": _get_setting("global_send_enabled", False),
        "test_email_only": _get_setting("test_email_only", False),
        "staffing_manager_only": _get_setting("staffing_manager_only", False),
    }
    return templates.TemplateResponse("client_notifications/index.html", {"request": request, "settings": settings})

@router.post("/api/settings")
async def update_settings(
    global_send_enabled: bool = Form(False),
    test_email_only: bool = Form(False),
    staffing_manager_only: bool = Form(False),
):
    _set_setting("global_send_enabled", global_send_enabled)
    _set_setting("test_email_only", test_email_only)
    _set_setting("staffing_manager_only", staffing_manager_only)
    return JSONResponse({"status": "success"})

@router.post("/api/process-test")
async def process_test_clients():
    engine = _engine()
    try:
        # Get 10 clients who had orders last month
        today = datetime.now()
        first_of_this_month = today.replace(day=1)
        last_month_end = first_of_this_month - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        
        sql_clients = text("""
            SELECT DISTINCT client_id 
            FROM event 
            WHERE date BETWEEN :start AND :end 
              AND deleted_at IS NULL 
            LIMIT 10
        """)
        with engine.begin() as conn:
            clients = conn.execute(sql_clients, {
                "start": last_month_start.strftime("%Y-%m-%d"), 
                "end": last_month_end.strftime("%Y-%m-%d")
            }).fetchall()
            client_ids = [c[0] for c in clients]
        
        results = []
        from apps.client_notifications.scheduler import send_client_monthly_notification

        for cid in client_ids:
            activity = await get_prior_month_data(cid)
            # Add client name
            with engine.begin() as conn:
                client_name = conn.execute(text("SELECT name FROM client WHERE client_id = :id"), {"id": cid}).scalar()
            activity['client_name'] = client_name
            activity['client_id'] = cid
            
            # Force send to test email during this manual test phase
            await send_client_monthly_notification(cid, activity, ["test@culinarystaffing.com"])
            await asyncio.sleep(1)
            
            results.append(activity)
            
        return JSONResponse({"status": "success", "data": results})
    finally:
        engine.dispose()
