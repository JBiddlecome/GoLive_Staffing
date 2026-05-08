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

    results = await get_similar_clients_async(industry, msp_id, client_name, lat, lon)

    return JSONResponse(
        {
            "results": results,
            "geocode_error": geocode_error,
            "input_lat": lat,
            "input_lon": lon,
        }
    )
