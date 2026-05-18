from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from .extractor import ai_extract_order
from .knowledge_base import build_client_kb

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# Fallback dummy client knowledge base if DB connection fails locally
CLIENT_KB = {
    63: {
        "name": "ACME Catering",
        "available_positions": [
            {"name": "Server", "typical_grooming": "Standard Black Bistro", "tools": ["Wine Key", "Crumber"]},
            {"name": "Bartender", "typical_grooming": "Standard Black Bistro", "tools": ["Wine Key", "Shaker"]},
            {"name": "Cook 2", "typical_grooming": "Chef Coat", "tools": ["Knives"]},
            {"name": "Cook G", "typical_grooming": "Chef Coat", "tools": ["Knives", "Thermometer"]}
        ],
        "typical_positions_used": ["Server", "Cook G"],
        "typical_venues": [
            {"name": "Convention Center", "address": "123 Fake St"},
            {"name": "Downtown Hotel", "address": "456 Main St"}
        ],
        "certifications": ["Food Handler"]
    }
}

@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user = request.session.get("user")
    return templates.TemplateResponse("apps/orders/index.html", {"request": request, "user": user})

@router.post("/extract")
async def extract_order(request: Request):
    data = await request.json()
    text = data.get('text', '')
    
    # In the future, this will call the LLM API (e.g. Gemini/OpenAI with STAND_ALONE key)
    # For now, return a mocked structure to show the user the categories.
    
    # Fuzzy matching logic placeholder (Currently hardcoded to ACME Catering - 63)
    client_context = build_client_kb(63) or CLIENT_KB.get(63)
    
    extracted_data = await ai_extract_order(text, client_context)
    
    if not extracted_data.get('basic_information'):
        return JSONResponse({"status": "error", "message": "Failed to extract order data."}, status_code=500)
    
    return JSONResponse({"status": "success", "data": extracted_data})
