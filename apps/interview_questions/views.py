from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from openai import OpenAI, OpenAIError
from pydantic import BaseModel

SYSTEM_PROMPT = """
You are a hiring evaluator for our staffing agency.

Read the full interview transcript (questions and answers) and evaluate the candidate's responses to each of these five questions:
1) "When your manager gives you feedback or asks you to work on something, how do you usually handle it? Can you give me an example?"
2) "Has there been a time your manager asked you to do something that wasn’t really in your usual job? How did you handle it?"
3) "Tell me about a time during a busy shift when you felt really overwhelmed. What did you do to stay on track?"
4) "Have you ever felt stressed or frustrated at work? How did you make sure it didn’t affect your work or your team?"
5) "Has a coworker ever made your job harder because of their attitude or behavior? How did you deal with it professionally?"

For each question, classify the candidate's answer as:
- Green Flag: Positive behaviors (proactive, collaborative, professional) with clear examples.
- Yellow Flag: Mixed signals; attempts the right behavior but with inconsistency or limited follow-through.
- Red Flag: Unprofessional, resistant, or harmful behaviors that conflict with team expectations.

Use the transcript only—do not rely on the example prompts themselves as the answers. If an answer is missing or unclear, rate it conservatively.

Return ONLY valid JSON with this exact structure:
{
  "overall_recommendation": {
    "flag": "Green Flag | Yellow Flag | Red Flag",
    "confidence": 0-100
  },
  "question_evaluations": [
    {
      "question_number": 1,
      "question": "string",
      "flag": "Green Flag | Yellow Flag | Red Flag",
      "confidence": 0-100,
      "rationale": "string"
    }
  ],
  "strengths": ["string"],
  "concerns": ["string"],
  "notes_for_hiring_manager": "string"
}

Always include all five questions in order within "question_evaluations" with their corresponding question text, flags, confidences, and rationales. Base the overall recommendation on the pattern of flags across all answers and your confidence in the transcript.
"""

router = APIRouter()
templates = Jinja2Templates(directory="templates")
logger = logging.getLogger(__name__)


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
        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": transcript},
            ],
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
        parsed: Dict[str, Any] = json.loads(content_block)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail="AI response was not valid JSON.") from exc

    return JSONResponse(parsed)
