from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Sub-routes
from apps.clickboarding_check.views import router as clickboarding_router
from apps.concierge.views import router as concierge_router
from apps.employee_list_filter.views import router as employee_list_router
from apps.employee_phone_county_audit.views import router as employee_audit_router
from apps.health_benefits.views import router as health_benefits_router
from apps.interview_questions.views import router as interview_questions_router
from apps.recruiting_metrics.views import router as recruiting_metrics_router
from apps.resume_analyzer.views import router as resume_analyzer_router
from apps.sales_staffing_metrics.views import router as sales_staffing_router
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


@app.head("/")
async def index_head() -> Response:
    """Render expects HEAD requests on the root path to succeed."""
    return Response(status_code=200)

# Mount tool routers
app.include_router(clickboarding_router, prefix="/clickboarding-check", tags=["Clickboarding Check"])
app.include_router(health_benefits_router, prefix="/health-benefits", tags=["Health Benefits"])
app.include_router(sales_staffing_router, prefix="/sales-staffing-metrics", tags=["Sales & Staffing Metrics"])
app.include_router(recruiting_metrics_router, prefix="/recruiting-metrics", tags=["Recruiting Metrics"])
app.include_router(resume_analyzer_router, prefix="/resume-analyzer", tags=["Resume Analyzer"])
app.include_router(concierge_router, prefix="/concierge", tags=["Concierge"])
app.include_router(text_blast_router, prefix="/text-blast-filter", tags=["Text Blast Filter"])
app.include_router(ucla_hours_router, prefix="/ucla-hours-tool", tags=["UCLA Hours Tool"])
app.include_router(employee_list_router, prefix="/employee-list-filter", tags=["Employee List Filter"])
app.include_router(
    employee_audit_router,
    prefix="/employee-phone-county-audit",
    tags=["Employee Phone & County Audit"],
)
app.include_router(
    interview_questions_router, prefix="/interview-questions", tags=["Interview Questions"]
)

# Simple health check for Render
@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.head("/healthz")
async def healthz_head() -> Response:
    """Render sends HEAD requests to the health check endpoint."""
    return Response(status_code=200)
