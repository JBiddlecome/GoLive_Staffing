from __future__ import annotations

import os
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
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
    """Return the log of client profile changes."""
    from apps.client_profile_changes.scheduler import get_changes_log
    changes = get_changes_log()
    return JSONResponse({"data": changes})

@router.get("/debug")
async def debug_client_profile_changes():
    """Diagnostic endpoint — shows cache state, DB sample, and diff preview."""
    from apps.client_profile_changes.scheduler import (
        _client_cache, _venue_cache, _is_initialized, _changes_log,
        _fetch_clients, _serialise_val, _CLIENT_FIELDS,
    )
    debug = {
        "is_initialized": _is_initialized,
        "cache_client_count": len(_client_cache),
        "cache_venue_count": len(_venue_cache),
        "changes_count": len(_changes_log),
    }

    # Try to fetch a few clients from DB and compare against cache
    try:
        current = _fetch_clients()
        if current is None:
            debug["db_fetch"] = "FAILED — returned None"
        else:
            debug["db_client_count"] = len(current)

            # Show up to 5 diffs
            diffs = []
            if _client_cache:
                for cid in list(current.keys())[:500]:
                    cached = _client_cache.get(cid)
                    if cached is None:
                        continue
                    for field in _CLIENT_FIELDS:
                        old_v = cached.get(field)
                        new_v = current[cid].get(field)
                        if old_v != new_v:
                            diffs.append({
                                "client_id": cid,
                                "client_name": current[cid].get("name", "?"),
                                "field": field,
                                "cached": repr(old_v),
                                "current": repr(new_v),
                            })
                            if len(diffs) >= 5:
                                break
                    if len(diffs) >= 5:
                        break
            debug["sample_diffs"] = diffs if diffs else "no differences found"

            # Show a sample cached vs current for the first client
            if _client_cache:
                sample_id = list(_client_cache.keys())[0]
                debug["sample_cached"] = {
                    "client_id": sample_id,
                    "fields": _client_cache[sample_id],
                }
                if sample_id in current:
                    debug["sample_current"] = {
                        "client_id": sample_id,
                        "fields": current[sample_id],
                    }
    except Exception as e:
        debug["db_error"] = str(e)

    return JSONResponse(debug)
