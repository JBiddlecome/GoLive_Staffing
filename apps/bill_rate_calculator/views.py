import os
from fastapi import APIRouter, HTTPException, Request
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
async def bill_rate_calculator_page(request: Request):
    return templates.TemplateResponse(
        "apps/bill_rate_calculator.html", 
        {"request": request}
    )

@router.get("/api/msps")
async def get_msps():
    """Return all MSP names and rates for the filter dropdown."""
    engine = _engine()
    try:
        with engine.begin() as conn:
            sql = text("SELECT id, name, rate FROM msp WHERE id NOT IN (4, 8, 9, 12, 13, 21) ORDER BY name")
            rows = conn.execute(sql).mappings().fetchall()
            return [{"id": r["id"], "name": r["name"], "rate": float(r["rate"])} for r in rows]
    finally:
        engine.dispose()

@router.get("/api/wc_codes")
async def get_wc_codes():
    """Return all WC codes and rates for the filter dropdown."""
    engine = _engine()
    try:
        with engine.begin() as conn:
            sql = text("SELECT wc_id, rate FROM wc_code")
            rows = conn.execute(sql).mappings().fetchall()
            
            # Map of ID to assigned text based on the requirements
            wc_text_map = {
                44: "Bakery / Food Production",
                93: "Winery / Beverage Production",
                95: "Manufacturing",
                41: "Childcare / Assistance Services",
                96: "Food Distribution / Food Services",
                22: "Community / Housing Development",
                23: "Corporate / Management Services",
                40: "Senior Living / Post-Acute Care",
                82: "Hospital / Medical Center",
                51: "Retirement Community / Residential Care",
                27: "Hospital / Healthcare System",
                57: "Nonprofit / Camp / Charity Services",
                29: "Social Club / Recreation",
                87: "Hotel / Hospitality",
                30: "Country Club / Golf Club",
                47: "Private Club / Membership Organization",
                33: "Religious / Care Facility",
                19: "Events / Catering / Hospitality",
                86: "Commercial Kitchen / Food Facility",
                88: "Cafe / Quick Service",
                53: "Catering Company",
                89: "Bakery / Cafe",
                90: "Hospitality Group / Restaurant Group",
                20: "School / Education",
                84: "Performing Arts / Entertainment",
                81: "Catering / Events"
            }
            
            results = []
            for r in rows:
                wc_id = r["wc_id"]
                if wc_id in wc_text_map:
                    results.append({
                        "id": wc_id,
                        "name": wc_text_map[wc_id],
                        "rate": float(r["rate"]) if r["rate"] is not None else 0.0
                    })
            
            # Sort by name alphabetically
            results.sort(key=lambda x: x["name"])
            return results
    finally:
        engine.dispose()
