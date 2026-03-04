from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Sub-routes
from apps.clickboarding_check.views import router as clickboarding_router
from apps.client_drop_off.views import router as client_drop_off_router
from apps.concierge.views import router as concierge_router
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
from apps.client_drop_off_v2.views import router as client_drop_off_v2_router
from apps.contacts_data import add_contact, load_contacts, remove_contact

app = FastAPI(title="GoLive Staffing — Tools")

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
    return templates.TemplateResponse("work_in_progress.html", {"request": request})


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
app.include_router(client_drop_off_v2_router, prefix="/client-drop-off-v2", tags=["Client Drop Off v2"])

# Simple health check for Render
@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.head("/healthz")
async def healthz_head() -> Response:
    """Render sends HEAD requests to the health check endpoint."""
    return Response(status_code=200)
