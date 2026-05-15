from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from apps.api_usage_tracker import get_api_usage_summary

router = APIRouter()
templates = Jinja2Templates(directory="templates")

@router.get("", response_class=HTMLResponse)
async def admin_portal(request: Request):
    user = request.session.get("user")
    api_usage = get_api_usage_summary()
    return templates.TemplateResponse("apps/admin_portal.html", {"request": request, "user": user, "api_usage": api_usage})
