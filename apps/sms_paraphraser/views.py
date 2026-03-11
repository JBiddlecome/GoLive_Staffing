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
    api_key = os.getenv("RESUME_ANALYZER_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Neither RESUME_ANALYZER_OPENAI_API_KEY nor OPENAI_API_KEY is configured.")
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
                e.date,
                e.venue_details,
                e.parking_note,
                e.directions,
                e.check_in,
                v.name AS venue_name,
                COALESCE(e.address1, v.address1) AS address1,
                COALESCE(e.address2, v.address2) AS address2,
                COALESCE(e.city, v.city) AS city,
                COALESCE(e.state, v.state) AS state,
                COALESCE(e.zip, v.zip) AS zip,
                e.venue_id,
                (SELECT MIN(start) FROM shift WHERE event_id = e.event_id AND deleted_at IS NULL) AS shift_start
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

            # Fetch document descriptions from both event and venue
            doc_sql = text(
                """
                SELECT description FROM event_document 
                WHERE event_id = :event_id AND description IS NOT NULL AND description != ''
                UNION
                SELECT description FROM venue_document 
                WHERE venue_id = :venue_id AND description IS NOT NULL AND description != ''
                """
            )
            doc_results = connection.execute(doc_sql, {
                "event_id": event_id,
                "venue_id": event_data.get("venue_id")
            }).mappings().all()
            
            # Use a list to preserve order but unique-ify if necessary (UNION already does for duplicates across rows)
            event_data["document_descriptions"] = [row["description"] for row in doc_results]

    except Exception as exc:
        logger.exception("Database query failed")
        raise HTTPException(status_code=500, detail=f"Database error: {str(exc)}")

    # Prepare data for AI
    venue_name = event_data.get("venue_name") or event_data.get("title") or "[Venue]"
    
    # Format Location
    addr = event_data.get("address1", "")
    if event_data.get("address2"):
        addr += f", {event_data['address2']}"
    addr += f", {event_data.get('city', '')}, {event_data.get('state', '')} {event_data.get('zip', '')}"
    formatted_location = addr.strip(", ")

    # Format Time
    formatted_time = ""
    if event_data.get("shift_start"):
        formatted_time = event_data["shift_start"].strftime("%I:%M %p").lstrip("0")

    # Format Date
    formatted_date = ""
    if event_data.get("date"):
        formatted_date = event_data["date"].strftime("%A, %B %d, %Y")

    db_mapping = {
        "Event Name": "title",
        "Date": "formatted_date",
        "Time": "formatted_time",
        "Location": "formatted_location",
        "Venue Details": "venue_details",
        "Parking Note": "parking_note",
        "Directions": "directions",
        "Check-In": "check_in"
    }

    # Add formatted values to event_data for mapping
    event_data["formatted_date"] = formatted_date
    event_data["formatted_time"] = formatted_time
    event_data["formatted_location"] = formatted_location

    ai_info = f"Venue Name: {venue_name}\n"
    bullet_points = []
    
    # Map selection labels to their display names in the final SMS template
    prompt_display_names = {
        "Event Name": "Event",
        "Date": "Date",
        "Time": "Time",
        "Location": "Location",
        "Venue Details": "Venue",
        "Parking Note": "Parking",
        "Directions": "Directions",
        "Check-In": "Check-In"
    }

    for label in selected_fields:
        db_field = db_mapping.get(label)
        if db_field:
            raw_text = event_data.get(db_field, "")
            clean_text = strip_html(str(raw_text))
            ai_info += f"{label}: {clean_text}\n"
            
            # Build dynamic template line
            display_name = prompt_display_names.get(label, label)
            bullet_points.append(f"- {display_name}: [Paraphrase from '{label}']")

    template_structure = "\n".join(bullet_points)

    # AI Prompt (Matching extension logic but expanded)
    system_prompt = "You are an expert at writing concise SMS notifications for staffing events."
    user_prompt = (
        "You are a helpful assistant that paraphrases event details into a concise SMS message. "
        "EXTREMELY IMPORTANT: You must format the SMS EXACTLY like this template, replacing the bracketed items with paraphrased information from the provided data: "
        f"\"This is your first shift at {venue_name}. Please note the event details:\n{template_structure}\"\n"
        "Rules: "
        "1. Include ONLY the bullet points shown in the template above. "
        "2. Do NOT add any sections that are not explicitly in the template. "
        "3. Ensure the output is concise and suitable for SMS. "
        f"\nInformation to paraphrase:\n{ai_info}\n"
        "Output ONLY the final SMS message. No intro, no outro, no markdown formatting."
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

        # Append document reminders if any
        doc_descriptions = event_data.get("document_descriptions", [])
        if doc_descriptions:
            doc_messages = [f"Review the document, {desc}, on the event." for desc in doc_descriptions]
            sms_text += "\n\n" + "\n".join(doc_messages)

        # Append signature
        signature = "\n\nAlways read through all shift details in GoLive before your shift.\n-Culinary Staffing"
        if not sms_text.endswith("-Culinary Staffing"):
            sms_text += signature
            
        return JSONResponse({"text": sms_text})

    except OpenAIError as exc:
        logger.exception("OpenAI API request failed")
        raise HTTPException(status_code=502, detail="AI service error")
    except Exception as exc:
        logger.exception("Unexpected error")
        raise HTTPException(status_code=500, detail="Internal server error")
