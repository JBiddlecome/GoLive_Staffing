from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, Request, Form
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
async def database_update_page(request: Request):
    return templates.TemplateResponse(
        "apps/database_update.html",
        {
            "request": request
        }
    )

@router.get("/count")
async def get_records_count():
    engine = _engine()
    try:
        sql = text("SELECT COUNT(*) as count FROM venue_attestation_question WHERE attestation_question_id = 89")
        with engine.connect() as connection:
            result = connection.execute(sql).fetchone()
            return JSONResponse({"count": result[0]})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.post("/update")
async def update_records():
    engine = _engine()
    try:
        # First check the count to be safe
        check_sql = text("SELECT COUNT(*) as count FROM venue_attestation_question WHERE attestation_question_id = 89")
        update_sql = text("UPDATE venue_attestation_question SET attestation_question_id = 88 WHERE attestation_question_id = 89")
        
        with engine.begin() as connection:
            count_result = connection.execute(check_sql).fetchone()
            count_before = count_result[0]
            
            if count_before == 0:
                return JSONResponse({"message": "No records found with ID 89.", "updated": 0})
            
            result = connection.execute(update_sql)
            return JSONResponse({
                "message": f"Successfully updated {result.rowcount} records.",
                "updated": result.rowcount
            })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.get("/count-net-terms")
async def get_net_terms_count():
    engine = _engine()
    try:
        sql = text("SELECT COUNT(*) as count FROM client WHERE net_terms_entry_id = 10")
        with engine.connect() as connection:
            result = connection.execute(sql).fetchone()
            return JSONResponse({"count": result[0]})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.post("/update-net-terms")
async def update_net_terms():
    engine = _engine()
    try:
        check_sql = text("SELECT COUNT(*) as count FROM client WHERE net_terms_entry_id = 10")
        update_sql = text("UPDATE client SET net_terms_entry_id = 14 WHERE net_terms_entry_id = 10")
        
        with engine.begin() as connection:
            count_result = connection.execute(check_sql).fetchone()
            count_before = count_result[0]
            
            if count_before == 0:
                return JSONResponse({"message": "No records found with Net Terms ID 10.", "updated": 0})
            
            result = connection.execute(update_sql)
            return JSONResponse({
                "message": f"Successfully updated {result.rowcount} clients to Net Terms 14.",
                "updated": result.rowcount
            })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()
