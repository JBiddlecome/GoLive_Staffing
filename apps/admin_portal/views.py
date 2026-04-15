from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="templates")

@router.get("", response_class=HTMLResponse)
async def admin_portal(request: Request):
    user = request.session.get("user")
    return templates.TemplateResponse("apps/admin_portal.html", {"request": request, "user": user})
