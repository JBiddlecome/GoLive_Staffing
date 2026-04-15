from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from apps.position_requests.scheduler import load_records, save_records

router = APIRouter()
templates = Jinja2Templates(directory="templates")

ALLOWED_RECRUITERS = ["Mercedes", "Piyush"]

@router.get("", response_class=HTMLResponse)
async def position_requests_page(request: Request):
    records = load_records()
    user = request.session.get("user")
    
    # Sort by newest first (do not sort by completed status so items don't jump to bottom)
    records.sort(key=lambda r: r.get("received_date", ""), reverse=True)
    
    context = {
        "request": request,
        "user": user,
        "records": records,
        "recruiters": ALLOWED_RECRUITERS
    }
    return templates.TemplateResponse("apps/position_requests.html", context)

@router.post("/update")
async def update_position_request(
    request: Request,
    message_id: str = Form(...),
    recruiter: str = Form(""),
    completed: str | None = Form(None)
):
    records = load_records()
    record = next((r for r in records if r.get("message_id") == message_id), None)
    
    if not record:
        return JSONResponse({"status": "error", "message": "Record not found"}, status_code=404)
        
    record["recruiter"] = recruiter if recruiter in ALLOWED_RECRUITERS else ""
    record["completed"] = (completed is not None and completed != "")
    
    save_records(records)
    
    return JSONResponse({"status": "success", "record": record})
