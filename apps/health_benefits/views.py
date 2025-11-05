from fastapi import APIRouter, Request, UploadFile, File
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="templates")
router = APIRouter()

@router.get("")
async def page(request: Request):
    return templates.TemplateResponse("apps/health_benefits.html", {"request": request})

@router.post("/upload")
async def upload(request: Request, file: UploadFile = File(...)):
    # TODO: process Excel here later
    _ = await file.read()  # read to avoid unused var
    return templates.TemplateResponse(
        "apps/health_benefits.html",
        {"request": request, "message": f"Received: {file.filename} (processing coming soon)"},
    )
