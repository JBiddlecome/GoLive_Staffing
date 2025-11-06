from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Sub-routes
from apps.clickboarding_check.views import router as clickboarding_router
from apps.employee_list_filter.views import router as employee_list_router
from apps.employee_phone_county_audit.views import router as employee_audit_router
from apps.health_benefits.views import router as health_benefits_router
from apps.text_blast_filter.views import router as text_blast_router
from apps.ucla_hours_tool.views import router as ucla_hours_router

app = FastAPI(title="GoLive Staffing — Tools")

# Static + templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Landing
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Mount tool routers
app.include_router(clickboarding_router, prefix="/clickboarding-check", tags=["Clickboarding Check"])
app.include_router(health_benefits_router, prefix="/health-benefits", tags=["Health Benefits"])
app.include_router(text_blast_router, prefix="/text-blast-filter", tags=["Text Blast Filter"])
app.include_router(ucla_hours_router, prefix="/ucla-hours-tool", tags=["UCLA Hours Tool"])
app.include_router(employee_list_router, prefix="/employee-list-filter", tags=["Employee List Filter"])
app.include_router(
    employee_audit_router,
    prefix="/employee-phone-county-audit",
    tags=["Employee Phone & County Audit"],
)

# Simple health check for Render
@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
