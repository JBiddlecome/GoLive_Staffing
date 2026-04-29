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
async def client_profile_changes_page(request: Request):
    return templates.TemplateResponse(
        "apps/client_profile_changes.html",
        {"request": request},
    )

@router.get("/data")
async def get_client_profile_changes():
    """Run a fresh check against the DB, then return the full change log."""
    from apps.client_profile_changes.scheduler import (
        _ensure_initialized, run_check, get_changes_log,
    )
    _ensure_initialized()
    run_check()
    changes = get_changes_log()
    return JSONResponse({"data": changes})
