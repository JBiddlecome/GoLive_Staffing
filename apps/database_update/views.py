from __future__ import annotations

import os
from typing import Any
import pandas as pd

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

@router.get("/count-staffing-manager")
async def get_staffing_manager_count():
    engine = _engine()
    try:
        sql = text("SELECT COUNT(*) as count FROM venue WHERE staffing_manager_id = 11747")
        with engine.connect() as connection:
            result = connection.execute(sql).fetchone()
            return JSONResponse({"count": result[0]})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.post("/update-staffing-manager")
async def update_staffing_manager():
    engine = _engine()
    try:
        check_sql = text("SELECT COUNT(*) as count FROM venue WHERE staffing_manager_id = 11747")
        update_sql = text("UPDATE venue SET staffing_manager_id = 74 WHERE staffing_manager_id = 11747")
        
        with engine.begin() as connection:
            count_result = connection.execute(check_sql).fetchone()
            count_before = count_result[0]
            
            if count_before == 0:
                return JSONResponse({"message": "No venues found with Staffing Manager ID 11747.", "updated": 0})
            
            result = connection.execute(update_sql)
            return JSONResponse({
                "message": f"Successfully updated {result.rowcount} venues to Staffing Manager 74.",
                "updated": result.rowcount
            })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.post("/undo-staffing-manager")
async def undo_staffing_manager():
    engine = _engine()
    try:
        # Note: This simply reverses 74 back to 11747. 
        # In a more complex system, we might want to track specific IDs, 
        # but here we follow the user's manual "Undo" request.
        update_sql = text("UPDATE venue SET staffing_manager_id = 11747 WHERE staffing_manager_id = 74")
        
        with engine.begin() as connection:
            result = connection.execute(update_sql)
            return JSONResponse({
                "message": f"Successfully reverted {result.rowcount} venues back to Staffing Manager 11747.",
                "updated": result.rowcount
            })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.get("/count-dan-sm")
async def get_dan_sm_count():
    engine = _engine()
    try:
        sql = text("SELECT COUNT(*) as count FROM venue WHERE staffing_manager_id = 74")
        with engine.connect() as connection:
            result = connection.execute(sql).fetchone()
            return JSONResponse({"count": result[0]})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.post("/update-dan-sm")
async def update_dan_sm():
    engine = _engine()
    try:
        check_sql = text("SELECT COUNT(*) as count FROM venue WHERE staffing_manager_id = 74")
        update_sql = text("UPDATE venue SET staffing_manager_id = 1803 WHERE staffing_manager_id = 74")
        
        with engine.begin() as connection:
            count_result = connection.execute(check_sql).fetchone()
            count_before = count_result[0]
            
            if count_before == 0:
                return JSONResponse({"message": "No venues found with Staffing Manager ID 74.", "updated": 0})
            
            result = connection.execute(update_sql)
            return JSONResponse({
                "message": f"Successfully updated {result.rowcount} venues to Staffing Manager 1803.",
                "updated": result.rowcount
            })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.post("/undo-dan-sm")
async def undo_dan_sm():
    engine = _engine()
    try:
        update_sql = text("UPDATE venue SET staffing_manager_id = 74 WHERE staffing_manager_id = 1803")
        
        with engine.begin() as connection:
            result = connection.execute(update_sql)
            return JSONResponse({
                "message": f"Successfully reverted {result.rowcount} venues back to Staffing Manager 74.",
                "updated": result.rowcount
            })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.get("/count-sm-6170")
async def get_sm_6170_count():
    engine = _engine()
    try:
        sql = text("SELECT COUNT(*) as count FROM venue WHERE staffing_manager_id = 6170")
        with engine.connect() as connection:
            result = connection.execute(sql).fetchone()
            return JSONResponse({"count": result[0]})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.post("/update-sm-6170")
async def update_sm_6170():
    engine = _engine()
    try:
        check_sql = text("SELECT COUNT(*) as count FROM venue WHERE staffing_manager_id = 6170")
        update_sql = text("UPDATE venue SET staffing_manager_id = 1803 WHERE staffing_manager_id = 6170")

        with engine.begin() as connection:
            count_result = connection.execute(check_sql).fetchone()
            count_before = count_result[0]

            if count_before == 0:
                return JSONResponse({"message": "No venues found with Staffing Manager ID 6170.", "updated": 0})

            result = connection.execute(update_sql)
            return JSONResponse({
                "message": f"Successfully updated {result.rowcount} venues to Staffing Manager 1803.",
                "updated": result.rowcount
            })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.post("/undo-sm-6170")
async def undo_sm_6170():
    engine = _engine()
    try:
        update_sql = text("UPDATE venue SET staffing_manager_id = 6170 WHERE staffing_manager_id = 1803")

        with engine.begin() as connection:
            result = connection.execute(update_sql)
            return JSONResponse({
                "message": f"Successfully reverted {result.rowcount} venues back to Staffing Manager 6170.",
                "updated": result.rowcount
            })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.get("/count-sm-6170-excel")
async def get_sm_6170_excel_count():
    engine = _engine()
    try:
        df = pd.read_excel("Venue Staffing Manager Report - May 6.xlsx")
        target_venues = df[df['staffing_manager_id'] == 6170]['venue_id'].dropna().astype(int).tolist()
        
        if not target_venues:
            return JSONResponse({"count": 0})
            
        placeholders = ','.join([str(v) for v in target_venues])
        sql = text(f"SELECT COUNT(*) as count FROM venue WHERE staffing_manager_id = 6170 AND venue_id IN ({placeholders})")
        
        with engine.connect() as connection:
            result = connection.execute(sql).fetchone()
            return JSONResponse({"count": result[0]})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.post("/update-sm-6170-excel")
async def update_sm_6170_excel(request: Request):
    engine = _engine()
    try:
        form_data = await request.form()
        new_id_str = form_data.get('new_staffing_manager_id')
        if not new_id_str:
            return JSONResponse({"error": "New Staffing Manager ID is required."}, status_code=400)
            
        new_id = int(new_id_str)
        
        df = pd.read_excel("Venue Staffing Manager Report - May 6.xlsx")
        target_venues = df[df['staffing_manager_id'] == 6170]['venue_id'].dropna().astype(int).tolist()
        
        if not target_venues:
            return JSONResponse({"message": "No target venues found in the spreadsheet.", "updated": 0})
            
        placeholders = ','.join([str(v) for v in target_venues])
        
        check_sql = text(f"SELECT COUNT(*) as count FROM venue WHERE staffing_manager_id = 6170 AND venue_id IN ({placeholders})")
        update_sql = text(f"UPDATE venue SET staffing_manager_id = :new_id WHERE staffing_manager_id = 6170 AND venue_id IN ({placeholders})")
        
        with engine.begin() as connection:
            count_result = connection.execute(check_sql).fetchone()
            count_before = count_result[0]
            
            if count_before == 0:
                return JSONResponse({"message": "No matching venues currently have Staffing Manager ID 6170.", "updated": 0})
                
            result = connection.execute(update_sql, {"new_id": new_id})
            return JSONResponse({
                "message": f"Successfully updated {result.rowcount} venues to Staffing Manager {new_id}.",
                "updated": result.rowcount,
                "new_id": new_id
            })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.post("/undo-sm-6170-excel")
async def undo_sm_6170_excel(request: Request):
    engine = _engine()
    try:
        form_data = await request.form()
        revert_from_id_str = form_data.get('revert_from_id')
        if not revert_from_id_str:
            return JSONResponse({"error": "Revert From ID is required."}, status_code=400)
            
        revert_from_id = int(revert_from_id_str)
        
        df = pd.read_excel("Venue Staffing Manager Report - May 6.xlsx")
        target_venues = df[df['staffing_manager_id'] == 6170]['venue_id'].dropna().astype(int).tolist()
        
        if not target_venues:
            return JSONResponse({"message": "No target venues found in the spreadsheet.", "updated": 0})
            
        placeholders = ','.join([str(v) for v in target_venues])
        
        update_sql = text(f"UPDATE venue SET staffing_manager_id = 6170 WHERE staffing_manager_id = :revert_from_id AND venue_id IN ({placeholders})")
        
        with engine.begin() as connection:
            result = connection.execute(update_sql, {"revert_from_id": revert_from_id})
            return JSONResponse({
                "message": f"Successfully reverted {result.rowcount} venues back to Staffing Manager 6170.",
                "updated": result.rowcount
            })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()
