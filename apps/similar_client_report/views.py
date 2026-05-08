from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from .database import (
    INDUSTRY_CATEGORIES,
    geocode_location,
    get_msps_async,
    get_similar_clients_async,
)

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("", response_class=HTMLResponse)
async def similar_client_report_page(request: Request):
    return templates.TemplateResponse(
        "apps/similar_client_report.html", {"request": request}
    )


@router.get("/api/options")
async def get_options():
    msps = await get_msps_async()
    return JSONResponse({"msps": msps, "industries": INDUSTRY_CATEGORIES})


@router.post("/api/search")
async def search_similar_clients(request: Request):
    body = await request.json()
    client_name = (body.get("client_name") or "").strip()
    industry = (body.get("industry") or "").strip()
    location = (body.get("location") or "").strip()
    msp_id = body.get("msp_id") or None

    try:
        weight_industry = float(body.get("weight_industry") if body.get("weight_industry") is not None else 10000.0)
        weight_msp = float(body.get("weight_msp") if body.get("weight_msp") is not None else 1000.0)
        weight_shifts = float(body.get("weight_shifts") if body.get("weight_shifts") is not None else 1.0)
        weight_proximity = float(body.get("weight_proximity") if body.get("weight_proximity") is not None else 1.0)
    except (ValueError, TypeError):
        weight_industry = 10000.0
        weight_msp = 1000.0
        weight_shifts = 1.0
        weight_proximity = 1.0

    lat: float | None = None
    lon: float | None = None
    geocode_error: str | None = None

    if location:
        lat, lon = await run_in_threadpool(geocode_location, location)
        if lat is None:
            geocode_error = (
                f"Could not geocode '{location}'. Miles from location will not be shown."
            )

    if not industry:
        return JSONResponse({"error": "Industry is required."}, status_code=422)

    results = await get_similar_clients_async(
        industry,
        msp_id,
        client_name,
        lat,
        lon,
        weight_industry=weight_industry,
        weight_msp=weight_msp,
        weight_shifts=weight_shifts,
        weight_proximity=weight_proximity,
    )

    return JSONResponse(
        {
            "results": results,
            "geocode_error": geocode_error,
            "input_lat": lat,
            "input_lon": lon,
        }
    )
