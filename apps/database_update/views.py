from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, Request, Form, UploadFile, File
import pandas as pd
import io
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

@router.post("/update-phones")
async def update_phone_numbers(file: UploadFile = File(...)):
    engine = _engine()
    try:
        content = await file.read()
        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(content))
        elif file.filename.endswith('.xlsx'):
            df = pd.read_excel(io.BytesIO(content))
        else:
            return JSONResponse({"error": "Unsupported file format. Please upload .csv or .xlsx"}, status_code=400)
            
        df.columns = df.columns.astype(str).str.lower().str.strip()
        if 'employee_id' not in df.columns or 'mobile' not in df.columns:
            return JSONResponse({"error": "Spreadsheet must contain 'employee_id' and 'mobile' columns."}, status_code=400)
            
        df = df.dropna(subset=['employee_id', 'mobile'])
        
        updated_count = 0
        update_sql = text("UPDATE employee SET mobile = :mobile WHERE employee_id = :employee_id")
        
        with engine.begin() as connection:
            for _, row in df.iterrows():
                try:
                    emp_id = int(float(row['employee_id']))
                    mobile_val = str(row['mobile']).strip()
                    if mobile_val.endswith('.0'):
                        mobile_val = mobile_val[:-2]
                        
                    res = connection.execute(update_sql, {"mobile": mobile_val, "employee_id": emp_id})
                    updated_count += res.rowcount
                except (ValueError, TypeError):
                    continue
                    
        return JSONResponse({
            "message": f"Successfully updated {updated_count} employee phone numbers.",
            "updated": updated_count
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()

@router.post("/update-sales-reps")
async def update_sales_reps(file: UploadFile = File(...)):
    engine = _engine()
    try:
        content = await file.read()
        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(content))
        elif file.filename.endswith('.xlsx'):
            df = pd.read_excel(io.BytesIO(content))
        else:
            return JSONResponse({"error": "Unsupported file format. Please upload .csv or .xlsx"}, status_code=400)
            
        df.columns = df.columns.astype(str).str.lower().str.strip()
        if 'venue_id' not in df.columns or 'sales_rep_id' not in df.columns:
            return JSONResponse({"error": "Spreadsheet must contain 'venue_id' and 'sales_rep_id' columns."}, status_code=400)
            
        df = df.dropna(subset=['venue_id', 'sales_rep_id'])
        
        updated_count = 0
        update_sql = text("UPDATE venue SET sales_rep_id = :sales_rep_id WHERE venue_id = :venue_id")
        
        with engine.begin() as connection:
            for _, row in df.iterrows():
                try:
                    v_id = int(float(row['venue_id']))
                    # allow for Null / None / NaN handling if needed? The user said "update the sales_rep_id field".
                    # Let's assume sales_rep_id is an integer foreign key or similar. 
                    sr_val = row['sales_rep_id']
                    
                    if pd.isna(sr_val):
                        # skip if sales_rep_id is nan since we dropna above
                        continue
                        
                    sr_id = int(float(sr_val)) 
                    res = connection.execute(update_sql, {"sales_rep_id": sr_id, "venue_id": v_id})
                    updated_count += res.rowcount
                except (ValueError, TypeError):
                    continue
                    
        return JSONResponse({
            "message": f"Successfully updated {updated_count} venue sales reps.",
            "updated": updated_count
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        engine.dispose()
