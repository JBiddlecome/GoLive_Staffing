from fastapi import APIRouter, Request, UploadFile, File, Form
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="templates")
router = APIRouter()

@router.get("")
async def page(request: Request):
    return templates.TemplateResponse("apps/ucla_hours_tool.html", {"request": request})

@router.post("/upload")
async def upload(
    request: Request,
    employee_list: UploadFile = File(...),
    payroll: UploadFile = File(...),
    start_date: str = Form(""),
    end_date: str = Form(""),
):
    # TODO: implement real logic
    _ = await employee_list.read()
    _ = await payroll.read()
    msg = f"Received Employee List: {employee_list.filename}, Payroll: {payroll.filename}"
    if start_date or end_date:
        msg += f" | Date Range: {start_date} → {end_date}"
    return templates.TemplateResponse("apps/ucla_hours_tool.html", {"request": request, "message": msg})
