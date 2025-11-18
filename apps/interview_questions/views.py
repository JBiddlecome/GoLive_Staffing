from __future__ import annotations

import json
import os
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from openai import OpenAI
from pydantic import BaseModel

SYSTEM_PROMPT = """
You are a hiring evaluator for our staffing agency.

Read the full interview transcript (questions and answers) and evaluate the candidate.
Return ONLY valid JSON with this exact structure:

{
  "overall_recommendation": "Hire | No Hire | Borderline",
  "scores": {
    "communication": 1-5,
    "technical": 1-5,
    "professionalism": 1-5
  },
  "strengths": [ "string" ],
  "concerns": [ "string" ],
  "notes_for_hiring_manager": "string"
}
"""

router = APIRouter()
templates = Jinja2Templates(directory="templates")


class EvaluateInterviewRequest(BaseModel):
    transcript: str


def _get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured in the environment.")
    return OpenAI(api_key=api_key)


@router.get("", response_class=HTMLResponse)
async def interview_questions_page(request: Request) -> HTMLResponse:
    context = {"request": request}
    return templates.TemplateResponse("apps/interview_questions.html", context)


@router.post("/evaluate")
async def evaluate_interview(payload: EvaluateInterviewRequest) -> JSONResponse:
    transcript = payload.transcript.strip()
    if not transcript:
        raise HTTPException(status_code=400, detail="Transcript is required.")

    try:
        client = _get_openai_client()
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    try:
        response = client.responses.create(
            model="gpt-4.1-mini",
            input=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": transcript},
            ],
            response_format={"type": "json_object"},
        )
    except Exception as exc:  # pragma: no cover - external API call
        raise HTTPException(status_code=502, detail="Unable to reach the AI service.") from exc

    content_block = response.output[0].content[0].text if response.output else ""
    if not content_block:
        raise HTTPException(status_code=502, detail="AI response was empty.")

    try:
        parsed: Dict[str, Any] = json.loads(content_block)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail="AI response was not valid JSON.") from exc

    return JSONResponse(parsed)
