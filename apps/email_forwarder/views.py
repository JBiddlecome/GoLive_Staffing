from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("", response_class=HTMLResponse)
async def email_forwarder_page(request: Request):
    return templates.TemplateResponse(
        "apps/email_forwarder.html",
        {"request": request},
    )


@router.get("/status")
async def get_status():
    """Return current toggle state, schedule info, and recent forwarding log."""
    from apps.email_forwarder.scheduler import get_enabled, get_log, _is_within_schedule
    from datetime import datetime
    from zoneinfo import ZoneInfo

    PST = ZoneInfo("America/Los_Angeles")
    now = datetime.now(PST)

    return JSONResponse({
        "enabled": get_enabled(),
        "within_schedule": _is_within_schedule(),
        "current_time_pst": now.strftime("%A, %b %d %Y  %I:%M %p PST"),
        "log": get_log(),
    })


@router.post("/toggle")
async def toggle_forwarder(request: Request):
    """Enable or disable the forwarder."""
    from apps.email_forwarder.scheduler import set_enabled, get_enabled
    body = await request.json()
    new_state = bool(body.get("enabled", True))
    set_enabled(new_state)
    return JSONResponse({"enabled": get_enabled()})
