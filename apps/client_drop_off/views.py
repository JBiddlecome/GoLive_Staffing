from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from apps.client_drop_off.app import (
    PayrollDataError,
    calculate_drop_offs,
    load_payroll_data,
)

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("", response_class=HTMLResponse)
async def client_drop_off(request: Request) -> HTMLResponse:
    try:
        dataframe = load_payroll_data()
        records, lookback_start, recent_cutoff, max_date = calculate_drop_offs(
            dataframe
        )
    except PayrollDataError as exc:
        return templates.TemplateResponse(
            "apps/client_drop_off.html",
            {"request": request, "error": str(exc)},
            status_code=400,
        )

    staffing_managers = sorted(
        {
            row.get("Staffing Manager", "")
            for row in records
            if row.get("Staffing Manager")
        }
    )

    context = {
        "request": request,
        "records": records,
        "lookback_start": lookback_start,
        "recent_cutoff": recent_cutoff,
        "max_date": max_date,
        "total_clients": len(records),
        "staffing_managers": staffing_managers,
    }
    return templates.TemplateResponse("apps/client_drop_off.html", context)
