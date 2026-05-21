from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from apps.new_employee_position_approver.scheduler import (
    approve_positions_for_review,
    dismiss_review,
    load_state,
    refresh_pending_records_against_allowed_catalog,
    scan_for_new_employees,
    get_automation_enabled,
    set_automation_enabled,
    _today,
)

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("", response_class=HTMLResponse)
async def page(request: Request) -> HTMLResponse:
    refresh_pending_records_against_allowed_catalog()
    state = load_state()
    today_str = _today()
    records = [r for r in state.get("records", []) if r.get("review_date") == today_str]
    return templates.TemplateResponse(
        "apps/new_employee_position_approver.html",
        {
            "request": request,
            "records": records,
            "last_scan_at": state.get("last_scan_at"),
            "last_scan_message": state.get("last_scan_message"),
            "auto_approve_enabled": get_automation_enabled(),
        },
    )


@router.post("/scan")
async def scan_now() -> JSONResponse:
    result = await scan_for_new_employees()
    return JSONResponse({"status": "success", **result})


@router.post("/approve")
async def approve(
    review_id: str = Form(...),
    selected_position_ids: list[int] = Form(default=[]),
) -> JSONResponse:
    result = approve_positions_for_review(review_id, selected_position_ids)
    status_code = 200 if result.get("status") == "success" else 400
    return JSONResponse(result, status_code=status_code)


@router.post("/dismiss")
async def dismiss(
    review_id: str = Form(...),
    reason: str = Form(""),
) -> RedirectResponse:
    dismiss_review(review_id, reason)
    return RedirectResponse("/new-employee-position-approver", status_code=303)


@router.post("/toggle-automation")
async def toggle_automation(enabled: str = Form(...)) -> JSONResponse:
    is_enabled = (enabled.lower() == "true")
    set_automation_enabled(is_enabled)
    return JSONResponse({
        "status": "success",
        "auto_approve": is_enabled,
        "message": f"Auto-approval has been {'enabled' if is_enabled else 'disabled'}."
    })
