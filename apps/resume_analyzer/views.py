from __future__ import annotations

import base64
import io
import json
import logging
import os
from typing import List

from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from openai import OpenAI, OpenAIError
from pypdf import PdfReader

RESUME_SYSTEM_PROMPT = """
You are a resume screener for a hospitality staffing agency.
The user will send you a resume (as text or transcribed from a PDF/image).
Your job is to decide how qualified the candidate is for specific hospitality positions, only counting experience at fine-dining or equivalent venues.

Target positions

Evaluate the candidate for these positions:

Cook

Prep Cook

Dishwasher

Utility

Server

Runner

Busser

Bartender

Barback

Cashier

Pastry

Baker

Sushi

Concessions

Barista

Valet

Venue rules (VERY IMPORTANT)

Fast food experience does not qualify for any of the above positions.

Treat clearly fast-food or quick-service restaurants (for example: McDonald’s, Burger King, Wendy’s, Taco Bell, KFC, In-N-Out, Chick-fil-A, similar chains) as non-qualifying for all positions.

Only count experience at fine dining or equivalent hospitality venues, such as:

Hotels, resorts, country clubs

Upscale restaurants, steakhouses, fine dining, chef-driven or white-tablecloth concepts

Banquet / catering companies, convention centers, stadiums, arenas, large event venues

Corporate dining / contract dining for large companies, universities, hospitals, etc., if the role is clearly hospitality/food-service related.

If a venue type is unclear and could reasonably be non-fast-food hospitality (for example “Italian restaurant” with no brand name), you may count it, but lower your confidence.

If a job is obviously non-hospitality (office admin, warehouse, rideshare driver, etc.), do not count it toward any of the positions.

Experience levels

For each of the positions listed above, you must:

Look through the entire work history and find any matching or equivalent roles (for example:

Cook experience can include Line Cook, Prep Cook, Grill Cook, Banquet Cook, Chef de Partie, etc., at qualifying venues.

Server experience can include Banquet Server, Fine Dining Server, Room Service Server, Cocktail Server, etc., at qualifying venues.

Dishwasher / Utility can include Steward, Porter, Utility Worker, etc., at qualifying venues.

Concessions can include food stand worker at stadiums/arenas/large events (not mall food courts / fast food).

Barista can include coffee bar roles at hotels, specialty coffee shops, etc., but not fast-food drive-through roles.

Estimate the total combined time (in years) the candidate has spent in that type of role at qualifying venues. Be reasonable when dates are approximate.

Assign a Level based on total qualifying experience:

Level 1: less than 2 years combined experience

Level 2: 2 to 5 years combined experience

Level 3: more than 5 years combined experience

If there is no clear qualifying experience for a position, mark it as "no_experience" instead of assigning a level.

When estimating experience:

Use job dates if available.

If dates are missing, infer rough duration from context (e.g., “several months” ≈ 0.25–0.5 years).

Avoid double-counting overlapping jobs for the same role.

Output format

Return your result as valid JSON only, using this schema:
{
  "candidate_summary": {
    "hospitality_experience_overview": "",
    "total_hospitality_years_estimate": 0.0,
    "notable_venues": [],
    "notes_on_fast_food_or_non_qualifying_experience": ""
  },
  "positions": {
    "cook": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": [
        "Explain why you chose this level and what roles/venues you counted."
      ]
    },
    "prep_cook": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "dishwasher": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "utility": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "server": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "runner": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "busser": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "bartender": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "barback": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "cashier": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "pastry": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "baker": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "sushi": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "concessions": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "barista": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "valet": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    }
  }
}

Confidence should be a number between 0.0 and 1.0, where 1.0 means very certain.

In reasons, briefly mention which jobs and venues you counted and why you excluded any fast-food or non-qualifying experience.

Do not include any text outside of the JSON.
"""

logger = logging.getLogger(__name__)
templates = Jinja2Templates(directory="templates")
router = APIRouter()


@router.get("", response_class=HTMLResponse)
async def page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("apps/resume_analyzer.html", {"request": request})


@router.post("/analyze", response_class=JSONResponse)
async def analyze_resume(file: UploadFile = File(...)) -> JSONResponse:
    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    try:
        client = _get_openai_client()
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    try:
        ai_payload = _build_resume_messages(file.filename, file.content_type, contents)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=ai_payload,
            response_format={"type": "json_object"},
        )
    except OpenAIError as exc:  # pragma: no cover - external API call
        logger.exception("OpenAI API request failed")
        detail = getattr(exc, "message", None) or getattr(exc, "error", None) or str(exc)
        raise HTTPException(status_code=502, detail=f"AI service error: {detail}") from exc
    except Exception as exc:  # pragma: no cover - external API call
        logger.exception("Unexpected error while contacting AI service")
        raise HTTPException(status_code=502, detail="Unable to reach the AI service.") from exc

    content_block = response.choices[0].message.content if response.choices else ""
    if not content_block:
        raise HTTPException(status_code=502, detail="AI response was empty.")

    try:
        parsed = json.loads(content_block)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail="AI response was not valid JSON.") from exc

    return JSONResponse(parsed)


def _get_openai_client() -> OpenAI:
    api_key = os.getenv("RESUME_ANALYZER_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "RESUME_ANALYZER_OPENAI_API_KEY is not configured. Falling back to OPENAI_API_KEY is also not set."
        )
    return OpenAI(api_key=api_key)


def _build_resume_messages(filename: str, content_type: str, contents: bytes) -> list[dict]:
    name = (filename or "uploaded file").strip()
    mime = (content_type or "").lower()
    lower_name = name.lower()

    if "pdf" in mime or lower_name.endswith(".pdf"):
        text = _extract_pdf_text(contents)
        if not text:
            raise ValueError("Could not read any text from the uploaded PDF.")
        return [
            {"role": "system", "content": RESUME_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": f"Resume extracted from {name}. Return only the JSON schema provided.\n\n{text}",
            },
        ]

    if _is_image_file(mime, lower_name):
        data_url = _encode_image_to_data_url(contents, mime)
        return [
            {"role": "system", "content": RESUME_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Transcribe this resume image and evaluate it using the provided schema. Respond with JSON only.",
                    },
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            },
        ]

    raise ValueError("Only PDF or image resume files are supported.")


def _extract_pdf_text(file_bytes: bytes) -> str:
    buffer = io.BytesIO(file_bytes)
    try:
        reader = PdfReader(buffer)
    except Exception as exc:  # pragma: no cover - external library
        raise ValueError(f"Could not read PDF: {exc}") from exc

    texts: List[str] = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
        except Exception:  # pragma: no cover - library-specific errors
            text = ""
        if text:
            texts.append(text)
    return "\n\n".join(texts).strip()


def _is_image_file(mime: str, lower_name: str) -> bool:
    return mime.startswith("image/") or lower_name.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp"))


def _encode_image_to_data_url(contents: bytes, mime: str) -> str:
    safe_mime = mime if mime.startswith("image/") else "image/png"
    encoded = base64.b64encode(contents).decode("utf-8")
    return f"data:{safe_mime};base64,{encoded}"
