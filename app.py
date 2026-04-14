import os
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

from fastapi import FastAPI, Request, UploadFile, File, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
# Sub-routes
from apps.clickboarding_check.views import router as clickboarding_router
from apps.client_drop_off.views import router as client_drop_off_router
from apps.concierge.views import router as concierge_router
from apps.concierge_tracker_2.views import router as concierge_tracker_2_router
from apps.employee_list_filter.views import router as employee_list_router
from apps.employee_phone_county_audit.views import router as employee_audit_router
from apps.health_benefits.views import router as health_benefits_router
from apps.employee_access.views import router as employee_access_router
from apps.interview_questions.views import router as interview_questions_router
from apps.payroll_recruiting_reports.views import router as payroll_recruiting_reports_router
from apps.recruiting_metrics.views import router as recruiting_metrics_router
from apps.resume_analyzer.views import router as resume_analyzer_router
from apps.sales_payroll_analyzer.views import router as sales_payroll_analyzer_router
from apps.sales_staffing_metrics.views import router as sales_staffing_router
from apps.text_blast_filter.views import router as text_blast_router
from apps.ucla_hours_tool.views import router as ucla_hours_router
from apps.reports import router as reports_router
from apps.reportable import router as reportable_router
from apps.unconfirmed_requests import router as unconfirmed_requests_router
from apps.open_shifts_map import router as open_shifts_map_router
from apps.sms_paraphraser.views import router as sms_paraphraser_router
from apps.client_drop_off_v2.views import router as client_drop_off_v2_router
from apps.staffing_coverage_monitor.views import router as staffing_coverage_monitor_router
from apps.meal_penalty_dashboard.views import router as meal_penalty_router
from apps.staffing_dashboard.views import router as staffing_dashboard_router
from apps.client_cancellation_rates.views import router as client_cancellation_rates_router
from apps.database_update.views import router as database_update_router
from apps.payroll_notes.views import router as payroll_notes_router
from apps.payroll_reports.views import router as payroll_reports_router
from apps.scheduled_vs_worked.views import router as scheduled_vs_worked_router
from apps.sales_rate_intelligence.views import router as sales_rate_intelligence_router
from apps.staffing_employee_dashboard.views import router as staffing_employee_dashboard_router
from apps.daily_report_assessment.views import router as daily_report_assessment_router
from apps.msp_dashboard.views import router as msp_dashboard_router
from apps.credit_card_clients.views import router as credit_card_clients_router
from apps.admin_dashboard.views import router as admin_dashboard_router
from apps.client_communication_summary.views import router as client_communication_summary_router
from apps.employee_available_shift_history.views import router as employee_available_shift_history_router
from apps.auth.views import router as auth_router, get_current_user
from apps.contacts_data import add_contact, load_contacts, remove_contact

from contextlib import asynccontextmanager
import asyncio
from apps.msp_dashboard.scheduler import msp_monitoring_loop
from apps.credit_card_clients.scheduler import cc_clients_monitoring_loop
from apps.admin_dashboard.tracker import admin_tracking_loop

@asynccontextmanager
async def lifespan(app: FastAPI):
    monitor_task = asyncio.create_task(msp_monitoring_loop())
    cc_monitor_task = asyncio.create_task(cc_clients_monitoring_loop())
    admin_task = asyncio.create_task(admin_tracking_loop())
    yield
    monitor_task.cancel()
    cc_monitor_task.cancel()
    admin_task.cancel()

app = FastAPI(title="GoLive Staffing — Tools", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
from starlette.middleware.base import BaseHTTPMiddleware

class RequireLoginMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        allowed_paths = {"/auth/login", "/auth/logout", "/healthz", "/client-communication-summary"}
        if request.url.path in allowed_paths or request.url.path.startswith("/static"):
            return await call_next(request)
            
        if not request.session.get("user"):
            return RedirectResponse(url=f"/auth/login?next={request.url.path}", status_code=303)
            
        return await call_next(request)

app.add_middleware(RequireLoginMiddleware)
app.add_middleware(SessionMiddleware, secret_key="golive-super-secret-key")

# Static + templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Landing
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/external-ai-tools", response_class=HTMLResponse)
async def external_ai_tools(request: Request):
    return templates.TemplateResponse("external_ai_tools.html", {"request": request})


@app.get("/work-in-progress", response_class=HTMLResponse)
async def work_in_progress(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/auth/login?next=/work-in-progress", status_code=303)
    return templates.TemplateResponse("work_in_progress.html", {"request": request, "user": user})


@app.get("/contacts", response_class=HTMLResponse)
async def contacts(request: Request):
    contacts_data = load_contacts()
    return templates.TemplateResponse(
        "contacts.html", {"request": request, "departments": contacts_data["departments"]}
    )


@app.get("/contacts/update", response_class=HTMLResponse)
async def contacts_update(request: Request):
    contacts_data = load_contacts()
    department_options = list(
        {
            "Staffing",
            "HR & Office Management",
            "Recruiting",
            "Billing & Payroll",
            "Sales",
            "Tech Support",
            "Owner & Operators",
        }
    )
    department_options.sort()

    return templates.TemplateResponse(
        "contacts_update.html",
        {
            "request": request,
            "departments": contacts_data["departments"],
            "department_options": department_options,
        },
    )


@app.post("/contacts/update/add")
async def add_contact_entry(
    request: Request,
    department: str = Form(...),
    name: str = Form(...),
    title: str = Form(""),
    email: str = Form(""),
    phone: str = Form(""),
    extension: str = Form(""),
    staff_type: str = Form(...),
):
    add_contact(
        department=department,
        name=name,
        title=title,
        email=email,
        phone=phone,
        extension=extension,
        staff_type=staff_type,
    )
    return RedirectResponse("/contacts/update", status_code=303)


@app.post("/contacts/update/remove")
async def remove_contact_entry(contact_id: str = Form(...)):
    remove_contact(contact_id)
    return RedirectResponse("/contacts/update", status_code=303)


@app.get("/pro-account-form", response_class=HTMLResponse)
async def pro_account_form(request: Request):
    return templates.TemplateResponse("pro_account_form.html", {"request": request})


@app.head("/")
async def index_head() -> Response:
    """Render expects HEAD requests on the root path to succeed."""
    return Response(status_code=200)

# Mount tool routers
app.include_router(auth_router, prefix="/auth", tags=["Auth"])
app.include_router(clickboarding_router, prefix="/clickboarding-check", tags=["Clickboarding Check"])
app.include_router(employee_access_router, prefix="/employee-access", tags=["Employee Access"])
app.include_router(health_benefits_router, prefix="/health-benefits", tags=["Health Benefits"])
app.include_router(sales_staffing_router, prefix="/sales-staffing-metrics", tags=["Sales & Staffing Metrics"])
app.include_router(recruiting_metrics_router, prefix="/recruiting-metrics", tags=["Recruiting Metrics"])
app.include_router(resume_analyzer_router, prefix="/resume-analyzer", tags=["Resume Analyzer"])
app.include_router(
    sales_payroll_analyzer_router,
    prefix="/sales-payroll-analyzer",
    tags=["Payroll Rate Analyzer"],
)
app.include_router(concierge_router, prefix="/concierge", tags=["Concierge"])
app.include_router(concierge_tracker_2_router, prefix="/concierge-tracker-2", tags=["Concierge Tracker 2"])
app.include_router(text_blast_router, prefix="/text-blast-filter", tags=["Text Blast Filter"])
app.include_router(ucla_hours_router, prefix="/ucla-hours-tool", tags=["UCLA Hours Tool"])
app.include_router(client_drop_off_router, prefix="/client-drop-off", tags=["Client Drop Off"])
app.include_router(employee_list_router, prefix="/employee-list-filter", tags=["Employee List Filter"])
app.include_router(
    employee_audit_router,
    prefix="/employee-phone-county-audit",
    tags=["Employee Phone & County Audit"],
)
app.include_router(
    interview_questions_router, prefix="/interview-questions", tags=["Interview Questions"]
)
app.include_router(reports_router, prefix="/reports", tags=["Reports"])
app.include_router(reportable_router, prefix="/reportable", tags=["Reportable"])
app.include_router(
    payroll_recruiting_reports_router,
    prefix="/payroll-recruiting-reports",
    tags=["Payroll & Recruiting Reports"],
)
app.include_router(unconfirmed_requests_router, prefix="/unconfirmed-requests", tags=["Unconfirmed Requests"])
app.include_router(open_shifts_map_router, prefix="/open-shifts-map", tags=["Open Shifts Map"])
app.include_router(sms_paraphraser_router, prefix="/sms-paraphraser", tags=["SMS Paraphraser"])
app.include_router(client_drop_off_v2_router, prefix="/client-drop-off-v2", tags=["Client Drop Off v2"])
app.include_router(
    staffing_coverage_monitor_router,
    prefix="/staffing-coverage-monitor",
    tags=["Staffing Coverage Monitor"],
)
app.include_router(
    meal_penalty_router,
    prefix="/meal-penalty-dashboard",
    tags=["Meal Penalty Dashboard"],
)
app.include_router(
    staffing_dashboard_router,
    prefix="/staffing-dashboard",
    tags=["Staffing Manager Dashboard"],
)
app.include_router(
    client_cancellation_rates_router,
    prefix="/client-cancellation-rates",
    tags=["Client Cancellation Rates"],
)
app.include_router(
    database_update_router,
    prefix="/database-update",
    tags=["Database Update"],
)
app.include_router(
    payroll_notes_router,
    prefix="/payroll-notes",
    tags=["Payroll Notes"],
)
app.include_router(
    payroll_reports_router,
    prefix="/payroll-reports",
    tags=["Payroll Reports"],
)
app.include_router(
    scheduled_vs_worked_router,
    prefix="/scheduled-vs-worked-hours",
    tags=["Scheduled vs Worked Hours"],
)
app.include_router(
    sales_rate_intelligence_router,
    prefix="/sales-rate-intelligence",
    tags=["Sales Rate Intelligence Dashboard"],
)
app.include_router(
    staffing_employee_dashboard_router,
    prefix="/staffing-employee-dashboard",
    tags=["Staffing Employee Dashboard"],
)
app.include_router(
    daily_report_assessment_router,
    prefix="/daily-report-assessment",
    tags=["Daily Report Assessment"],
)
app.include_router(
    msp_dashboard_router,
    prefix="/msp-dashboard",
    tags=["MSP Dashboard"],
)
app.include_router(
    credit_card_clients_router,
    prefix="/credit-card-clients",
    tags=["Credit Card Clients"],
)
app.include_router(
    admin_dashboard_router,
    prefix="/admin-dashboard",
    tags=["Admin Dashboard"],
)
app.include_router(
    client_communication_summary_router,
    prefix="/client-communication-summary",
    tags=["Client Communication Summary"],
)
app.include_router(
    employee_available_shift_history_router,
    prefix="/employee-available-shift-history",
    tags=["Employee Available Shift History"],
)

# Redirect /sms_paraphraser to /sms-paraphraser for backward compatibility
@app.get("/sms_paraphraser", include_in_schema=False)
async def sms_paraphraser_redirect():
    return RedirectResponse(url="/sms-paraphraser", status_code=308)

# Simple health check for Render
@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.head("/healthz")
async def healthz_head() -> Response:
    """Render sends HEAD requests to the health check endpoint."""
    return Response(status_code=200)
