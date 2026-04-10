from __future__ import annotations

import os
import json
import logging
from typing import Optional
from base64 import b64encode

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import httpx

router = APIRouter()
templates = Jinja2Templates(directory="templates")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default prompt template — {client_name} is substituted at runtime
# ---------------------------------------------------------------------------
DEFAULT_PROMPT = (
    "Using the email communication data provided below for {client_name}, "
    "provide a summary of the communication. "
    "Note any issues the client has communicated as well as positive feedback. "
    "Provide details of any repeated issues with staffing, employees, or "
    "invoices and payments. Let the user know why revenue may be dropping off "
    "and suggest ways to improve business with the client."
)


def _anthropic_api_key() -> str:
    key = os.getenv("CLOSECONNECTKEY", "")
    if not key:
        raise RuntimeError("CLOSECONNECTKEY environment variable is not set")
    return key


def _close_api_key() -> str:
    key = os.getenv("CLOSE_API_KEY", "")
    if not key:
        raise RuntimeError("CLOSE_API_KEY environment variable is not set")
    return key


# ---------------------------------------------------------------------------
# Close.com helpers
# ---------------------------------------------------------------------------
def _close_auth_header(api_key: str) -> dict:
    """Close uses HTTP Basic Auth with the API key as the username."""
    token = b64encode(f"{api_key}:".encode()).decode()
    return {"Authorization": f"Basic {token}"}


async def _search_lead(client: httpx.AsyncClient, close_key: str, client_name: str) -> list[dict]:
    """Search Close.com for leads matching the given client name."""
    headers = _close_auth_header(close_key)
    resp = await client.get(
        "https://api.close.com/api/v1/lead/",
        headers=headers,
        params={"query": f'name:"{client_name}"', "_limit": 5},
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


async def _get_email_activity(
    client: httpx.AsyncClient,
    close_key: str,
    lead_id: str,
) -> list[dict]:
    """Fetch email activity for a given lead from Close.com."""
    headers = _close_auth_header(close_key)
    all_emails: list[dict] = []
    has_more = True
    skip = 0
    limit = 100

    while has_more:
        resp = await client.get(
            "https://api.close.com/api/v1/activity/email/",
            headers=headers,
            params={
                "lead_id": lead_id,
                "_skip": skip,
                "_limit": limit,
                "_fields": "id,date_created,subject,body_text,sender,to,direction,status",
                "_order_by": "-date_created",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        emails = data.get("data", [])
        all_emails.extend(emails)
        has_more = data.get("has_more", False)
        skip += limit
        # Safety cap: keep the most recent 200 emails
        if len(all_emails) >= 200:
            break

    return all_emails


# Rough token budget: ~100k tokens ≈ 400k characters, leaves plenty of
# headroom under the 200k-token context window for the system prompt,
# user prompt, and the 8k-token response.
MAX_CONTEXT_CHARS = 400_000
MAX_BODY_CHARS = 800


def _format_emails_for_prompt(emails: list[dict], client_name: str) -> str:
    """Format Close.com email data into a readable block for the LLM prompt."""
    if not emails:
        return f"No email communications found for {client_name} in Close.com."

    header = f"=== EMAIL COMMUNICATIONS FOR {client_name.upper()} ({len(emails)} total emails in Close) ===\n"
    lines: list[str] = [header]
    char_count = len(header)
    included = 0

    for i, email in enumerate(emails, 1):
        direction = email.get("direction", "unknown")
        subject = email.get("subject", "(no subject)")
        date = email.get("date_created", "unknown date")
        sender = email.get("sender", "unknown")
        to_addrs = email.get("to", [])
        body = email.get("body_text", "").strip()

        # Truncate individual email bodies
        if len(body) > MAX_BODY_CHARS:
            body = body[:MAX_BODY_CHARS] + "\n... [truncated]"

        block_lines = [
            f"--- Email {i} ---",
            f"Date: {date}",
            f"Direction: {direction}",
            f"From: {sender}",
        ]
        if to_addrs:
            to_str = ", ".join(to_addrs) if isinstance(to_addrs, list) else to_addrs
            block_lines.append(f"To: {to_str}")
        block_lines.append(f"Subject: {subject}")
        block_lines.append(f"Body:\n{body}\n")

        block = "\n".join(block_lines)

        # Enforce total character budget
        if char_count + len(block) > MAX_CONTEXT_CHARS:
            lines.append(
                f"\n... [{len(emails) - included} older emails omitted to stay within analysis limits]"
            )
            break

        lines.append(block)
        char_count += len(block)
        included += 1

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Page route
# ---------------------------------------------------------------------------
@router.get("", response_class=HTMLResponse)
async def client_communication_summary_page(request: Request):
    return templates.TemplateResponse(
        "apps/client_communication_summary.html",
        {"request": request, "default_prompt": DEFAULT_PROMPT},
    )


# ---------------------------------------------------------------------------
# API: analyse client communication via Close API + Claude
# ---------------------------------------------------------------------------
@router.post("/analyze")
async def analyze_client_communication(
    client_name: str = Form(...),
    custom_prompt: Optional[str] = Form(None),
):
    """
    1. Search Close.com for the client lead
    2. Pull all email activity for that lead
    3. Send the emails + prompt to Claude for analysis
    """

    try:
        anthropic_key = _anthropic_api_key()
        close_key = _close_api_key()
    except RuntimeError as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)

    # ------------------------------------------------------------------
    # Step 1 & 2: Pull email data from Close.com
    # ------------------------------------------------------------------
    try:
        async with httpx.AsyncClient(timeout=30.0) as http:
            leads = await _search_lead(http, close_key, client_name)

            if not leads:
                return JSONResponse({
                    "error": f"No lead found in Close.com matching \"{client_name}\". "
                             "Please check the client name and try again."
                }, status_code=404)

            # Use the first matching lead
            lead = leads[0]
            lead_id = lead["id"]
            lead_name = lead.get("display_name", client_name)

            emails = await _get_email_activity(http, close_key, lead_id)

    except httpx.HTTPStatusError as exc:
        logger.error("Close API error: %s %s", exc.response.status_code, exc.response.text)
        return JSONResponse({
            "error": f"Close.com API error ({exc.response.status_code}). Check your CLOSE_API_KEY.",
            "details": exc.response.text[:500],
        }, status_code=502)
    except Exception as exc:
        logger.exception("Error fetching data from Close.com")
        return JSONResponse({"error": f"Failed to connect to Close.com: {str(exc)}"}, status_code=500)

    # ------------------------------------------------------------------
    # Step 3: Send to Claude for analysis
    # ------------------------------------------------------------------
    email_context = _format_emails_for_prompt(emails, lead_name)

    prompt_template = custom_prompt.strip() if custom_prompt and custom_prompt.strip() else DEFAULT_PROMPT
    user_prompt = prompt_template.replace("{client_name}", lead_name)

    full_prompt = f"{user_prompt}\n\n{email_context}"

    payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 8192,
        "messages": [
            {"role": "user", "content": full_prompt}
        ],
    }

    headers = {
        "x-api-key": anthropic_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=120.0) as http:
            resp = await http.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=payload,
            )

        if resp.status_code != 200:
            error_body = resp.text
            logger.error("Anthropic API error %s: %s", resp.status_code, error_body)
            return JSONResponse(
                {"error": f"Claude API returned {resp.status_code}", "details": error_body},
                status_code=502,
            )

        data = resp.json()

        # Extract the text blocks from Claude's response
        content_blocks = data.get("content", [])
        analysis_parts: list[str] = []
        for block in content_blocks:
            if block.get("type") == "text":
                analysis_parts.append(block["text"])

        analysis = "\n\n".join(analysis_parts) if analysis_parts else "No analysis content was returned."

        return JSONResponse({
            "analysis": analysis,
            "client_name": lead_name,
            "email_count": len(emails),
            "model": data.get("model", "unknown"),
            "usage": data.get("usage", {}),
        })

    except httpx.TimeoutException:
        return JSONResponse(
            {"error": "The request to Claude timed out. Try a more specific prompt or a client with fewer emails."},
            status_code=504,
        )
    except Exception as exc:
        logger.exception("Unexpected error calling Claude API")
        return JSONResponse({"error": str(exc)}, status_code=500)
