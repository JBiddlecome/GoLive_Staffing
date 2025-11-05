from fastapi import APIRouter, Request, UploadFile, File
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="templates")
router = APIRouter()

@router.get("")
async def page(request: Request):
    return templates.TemplateResponse("apps/text_blast_filter.html", {"request": request})

@router.post("/upload")
async def upload(request: Request, file: UploadFile = File(...)):
    # TODO: implement cleaning, dedupe, opt-out removal, etc.
    _ = await file.read()
    return templates.TemplateResponse(
        "apps/text_blast_filter.html",
        {"request": request, "message": f"Received: {file.filename} (processing coming soon)"},
    )
