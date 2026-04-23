from __future__ import annotations

import os
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


router = APIRouter()
templates = Jinja2Templates(directory="templates")

def _db_url_from_env() -> URL:
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = int(os.getenv("DB_PORT", "3306"))

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
async def ar_contact_updates_page(request: Request):
    return templates.TemplateResponse(
        "apps/ar_contact_updates.html",
        {"request": request},
    )

@router.get("/data")
async def get_ar_contact_changes():
    """Return the log of AR contact changes."""
    from apps.ar_contact_updates.scheduler import get_ar_changes_log
    changes = get_ar_changes_log()
    return JSONResponse({"data": changes})
