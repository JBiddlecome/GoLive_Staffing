from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
import re

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from openai import OpenAI, OpenAIError
from pydantic import BaseModel

SYSTEM_PROMPT = """
You are a hiring evaluator for our staffing agency.

You will read a full interview transcript containing questions and answers. Evaluate ONLY the candidate's responses to the questions that were actually asked and answered in the transcript. The ten possible questions are:

1) "When your manager gives you feedback or asks you to work on something, how do you usually handle it? Can you give me an example?"
2) "Has there been a time your manager asked you to do something that wasn’t really in your usual job? How did you handle it?"
3) "Tell me about a time during a busy shift when you felt really overwhelmed. What did you do to stay on track?"
4) "Have you ever felt stressed or frustrated at work? How did you make sure it didn’t affect your work or your team?"
5) "Has a coworker ever made your job harder because of their attitude or behavior? How did you deal with it professionally?"
6) "How do you quickly assess and manage a team of temporary staff who may have never worked together before?"
7) "Walk me through how you prepare and assign roles to staff before an event starts."
8) "How would you handle multiple staff call-outs right before or during an event."
9) "How do you ensure agency or temporary staff follow client standards, even if they’re unfamiliar with the environment?"
10) "If a staff member is underperforming during an event, how do you address it in the moment?"

For each question that IS present and answered in the transcript, classify the candidate’s response as:
- Green Flag: Positive behaviors (proactive, collaborative, professional). Clear examples are preferred, but absence of an example should not deduct from consideration of a green flag.
- Yellow Flag: Mixed signals; attempts the right behavior but with inconsistency or limited follow-through.
- Red Flag: Unprofessional, resistant, or harmful behaviors that conflict with team expectations.

Use ONLY the interview transcript when evaluating; do not rely on the question list as if they were sample answers. If an answer is missing, incomplete, or unclear, rate conservatively. 
For questions that require an example do not deduct points if they answer the question but do not provide an example. If their answer to the main question is green flag worthy, then give them a green flag.
Do not mention if they failed to provide an example for their response.

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

Important rules:
• Include ONLY the questions that were actually asked and answered in the transcript.  
• Do NOT include or score any of the ten questions that do not appear in the transcript.  
• The “question_evaluations” array should list the evaluated questions IN NUMERICAL ORDER based on their question number.  
• Base the overall recommendation on the pattern of flags across the evaluated questions and your confidence in the transcript.
"""

router = APIRouter()
templates = Jinja2Templates(directory="templates")
logger = logging.getLogger(__name__)
DATA_FILE = Path("data/interview_question_responses.json")
DATA_FILE.parent.mkdir(parents=True, exist_ok=True)


class EvaluateInterviewRequest(BaseModel):
    transcript: str


def _get_openai_client() -> OpenAI:
    api_key = os.getenv("INTERVIEW_QUESTIONS") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("INTERVIEW_QUESTIONS or OPENAI_API_KEY is not configured in the environment.")
    return OpenAI(api_key=api_key)


@router.get("", response_class=HTMLResponse)
async def interview_questions_page(request: Request) -> HTMLResponse:
    context = {"request": request}
    return templates.TemplateResponse("apps/interview_questions.html", context)


@router.get("/responses", response_class=HTMLResponse)
async def interview_question_responses(request: Request) -> HTMLResponse:
    entries = [_format_entry_for_display(entry) for entry in _load_saved_entries()]
    context = {"request": request, "entries": entries}
    return templates.TemplateResponse("apps/interview_question_responses.html", context)


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
            model=os.getenv("OPENAI_MODEL_AUTOMATION", "gpt-4.1-nano"),
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
        parsed = _harmonize_overall_flag(parsed)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail="AI response was not valid JSON.") from exc

    try:
        _persist_transcript(transcript, parsed)
    except Exception:  # pragma: no cover - file I/O safety net
        logger.exception("Unable to persist interview transcript.")

    return JSONResponse(parsed)


def _harmonize_overall_flag(response: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure the overall flag reflects the majority of question flags.

    The AI sometimes returns an overall recommendation that conflicts with the
    per-question flags. This function adjusts the overall flag to match the
    majority vote across all question evaluations while preserving any provided
    confidence value.
    """

    question_evaluations: List[Dict[str, Any]] = response.get("question_evaluations") or []
    if not question_evaluations:
        return response

    counts = {"Green Flag": 0, "Yellow Flag": 0, "Red Flag": 0}
    for entry in question_evaluations:
        flag = entry.get("flag")
        if flag in counts:
            counts[flag] += 1

    top_count = max(counts.values())
    majority_flags = [flag for flag, count in counts.items() if count == top_count]
    if len(majority_flags) != 1:
        return response

    majority_flag = majority_flags[0]

    overall = response.get("overall_recommendation")
    if isinstance(overall, dict):
        overall["flag"] = majority_flag
    else:
        response["overall_recommendation"] = {"flag": majority_flag, "confidence": None}

    return response


def _persist_transcript(transcript: str, evaluation: Dict[str, Any]) -> None:
    entry = {
        "transcript": transcript,
        "evaluation": evaluation,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "interviewer_name": _extract_interviewer_name(transcript),
    }

    entries = _load_saved_entries()
    entries.insert(0, entry)  # newest first
    DATA_FILE.write_text(json.dumps(entries, indent=2))


def _load_saved_entries() -> List[Dict[str, Any]]:
    if not DATA_FILE.exists():
        return []
    try:
        entries = json.loads(DATA_FILE.read_text())
        if isinstance(entries, list):
            return entries
    except json.JSONDecodeError:
        logger.exception("Saved interview responses file is not valid JSON.")
    except Exception:
        logger.exception("Unexpected error reading interview responses file.")
    return []


def _format_entry_for_display(entry: Dict[str, Any]) -> Dict[str, Any]:
    raw_timestamp = entry.get("saved_at")
    display_timestamp = ""
    if isinstance(raw_timestamp, str):
        try:
            parsed = datetime.fromisoformat(raw_timestamp)
            display_timestamp = parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %I:%M %p %Z")
        except ValueError:
            display_timestamp = raw_timestamp

    return {
        "transcript": entry.get("transcript", ""),
        "evaluation_pretty": json.dumps(entry.get("evaluation", {}), indent=2),
        "saved_at": display_timestamp,
        "interviewer_name": entry.get("interviewer_name"),
    }


def _extract_interviewer_name(transcript: str) -> str | None:
    lines = [line.strip() for line in transcript.splitlines()]
    for idx, line in enumerate(lines):
        if _looks_like_time(line):
            for previous in reversed(lines[:idx]):
                if not previous or _looks_like_time(previous) or _looks_like_phone(previous):
                    continue
                return previous
    return None


def _looks_like_time(line: str) -> bool:
    return bool(re.search(r"\b\d{1,2}:\d{2}\s?(AM|PM)\b", line, flags=re.IGNORECASE))


def _looks_like_phone(line: str) -> bool:
    return bool(re.fullmatch(r"[\d\-()+\s]{7,}", line))
