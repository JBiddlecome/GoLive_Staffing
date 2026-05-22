from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from apps.admin_dashboard.tracker import get_admin_activity

router = APIRouter()
templates = Jinja2Templates(directory="templates")

@router.get("", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    user = request.session.get("user")
    # if not user:
    #     return RedirectResponse(url="/auth/login?next=/admin-dashboard", status_code=303)
        
    activity = get_admin_activity()
    return templates.TemplateResponse("apps/admin_dashboard.html", {
        "request": request,
        "user": user,
        "activity": activity
    })

@router.get("/data")
async def admin_dashboard_data(request: Request):
    user = request.session.get("user")
    # if not user:
    #     return JSONResponse(status_code=401, content={"error": "Unauthorized"})

    activity = get_admin_activity()
    # Sort by total_seconds descending
    activity_sorted = sorted(activity, key=lambda x: x["total_seconds"], reverse=True)
    return JSONResponse(content={"status": "success", "data": activity_sorted})

@router.get("/history-data")
async def admin_history_data(request: Request, start_date: str, end_date: str):
    from apps.admin_dashboard.tracker import get_admin_history
    history = get_admin_history(start_date, end_date)
@router.get("/staffing-activity-data")
async def staffing_activity_data(request: Request, start_date: str, end_date: str):
    from apps.admin_dashboard.tracker import get_staffing_activity
    activity = get_staffing_activity(start_date, end_date)
    return JSONResponse(content={"status": "success", "data": activity})
