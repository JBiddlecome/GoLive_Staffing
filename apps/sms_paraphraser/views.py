from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from openai import OpenAI, OpenAIError
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

router = APIRouter()
templates = Jinja2Templates(directory="templates")
logger = logging.getLogger(__name__)

# --- Models ---

class ParaphraseRequest(BaseModel):
    event_id: int
    sections: List[str]

# --- Database Helper ---

def _engine():
    """Create a SQLAlchemy engine for the database."""
    # Use standard project environment variables as seen in reportable/views.py
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("REPORTABLE_DB_HOST") or os.getenv("DB_HOST")
    port = os.getenv("REPORTABLE_DB_PORT") or os.getenv("DB_PORT", "3306")
    database = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")

    if not all([user, password, host, database]):
        # Fallback to a single URL if individual vars are missing
        db_url = os.getenv("DATABASE_URL")
        if db_url:
            return create_engine(db_url)
        
        # Diagnostic logging for missing vars
        missing = []
        if not user: missing.append("DB_USER")
        if not password: missing.append("DB_PASSWORD")
        if not host: missing.append("DB_HOST")
        if not database: missing.append("DB_NAME")
        
        raise RuntimeError(f"Database environment variables are not fully configured. Missing: {', '.join(missing)}")

    url = URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=database,
    )
    return create_engine(url)

def strip_html(html: str) -> str:
    """Simple regex to strip HTML tags."""
    if not html:
        return ""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', html)

# --- OpenAI Helper ---

def _get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured.")
    return OpenAI(api_key=api_key)

# --- Routes ---

@router.get("", response_class=HTMLResponse)
async def sms_paraphraser_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("apps/sms_paraphraser.html", {"request": request})

@router.post("/paraphrase")
async def paraphrase_event(payload: ParaphraseRequest) -> JSONResponse:
    event_id = payload.event_id
    selected_fields = payload.sections

    engine = _engine()
    try:
        sql = text(
            """
            SELECT
                e.title,
                e.venue_details,
                e.parking_note,
                e.directions,
                e.check_in,
                v.name AS venue_name
            FROM event e
            JOIN venue v ON e.venue_id = v.venue_id
            WHERE e.event_id = :event_id
            """
        )
        with engine.begin() as connection:
            result = connection.execute(sql, {"event_id": event_id}).mappings().first()
            if not result:
                raise HTTPException(status_code=404, detail=f"Event {event_id} not found.")
            
            event_data = dict(result)

    except Exception as exc:
        logger.exception("Database query failed")
        raise HTTPException(status_code=500, detail=f"Database error: {str(exc)}")

    # Prepare data for AI
    venue_name = event_data.get("venue_name") or event_data.get("title") or "[Venue]"
    
    db_mapping = {
        "Venue Details": "venue_details",
        "Parking Note": "parking_note",
        "Directions": "directions",
        "Check-In": "check_in"
    }

    ai_info = f"Venue Name: {venue_name}\n"
    for label in selected_fields:
        db_field = db_mapping.get(label)
        if db_field:
            raw_text = event_data.get(db_field, "")
            clean_text = strip_html(raw_text)
            ai_info += f"{label}: {clean_text}\n"

    # AI Prompt (Matching extension exactly)
    system_prompt = "You are an expert at writing concise SMS notifications for staffing events."
    user_prompt = (
        "You are a helpful assistant that paraphrases event details into a concise SMS message. "
        "EXTREMELY IMPORTANT: You must format the SMS EXACTLY like this template, replacing the bracketed items with paraphrased information from the provided data: "
        "\"This is your first shift at [Venue]. Please note the event details: "
        "- Venue: [Paraphrase from 'Venue Details'] "
        "- Parking: [Paraphrase from 'Parking Note'] "
        "- Directions: [Paraphrase from 'Directions'] "
        "- Check-In: [Paraphrase from 'Check-In']\" "
        "If a section is missing from the information below, omit that bullet point entirely from the SMS. "
        f"Information to paraphrase:\n{ai_info}\n"
        "Output ONLY the final SMS message. No intro, no outro, no markdown formatting (like backticks or bolding)."
    )

    try:
        client = _get_openai_client()
        response = client.chat.completions.create(
            model="gpt-4o-mini", # Requested model
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7
        )
        sms_text = response.choices[0].message.content.strip()
        return JSONResponse({"text": sms_text})

    except OpenAIError as exc:
        logger.exception("OpenAI API request failed")
        raise HTTPException(status_code=502, detail="AI service error")
    except Exception as exc:
        logger.exception("Unexpected error")
        raise HTTPException(status_code=500, detail="Internal server error")
