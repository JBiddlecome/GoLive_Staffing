from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Dict, List

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from fastapi.templating import Jinja2Templates

from apps.client_drop_off.app import (
    PayrollDataError,
    calculate_drop_offs,
    load_payroll_data,
)

router = APIRouter()
templates = Jinja2Templates(directory="templates")
NOTES_FILE = Path("data/client_drop_off_notes.json")
CONTACTED_FILE = Path("data/client_drop_off_contacted.json")
NOTES_FILE.parent.mkdir(parents=True, exist_ok=True)


class ClientNotePayload(BaseModel):
    client: str
    text: str


class ClientContactedPayload(BaseModel):
    client: str
    ignore_until: str


class ClientContactedDeletePayload(BaseModel):
    client: str


@router.get("", response_class=HTMLResponse)
async def client_drop_off(request: Request) -> HTMLResponse:
    try:
        dataframe = load_payroll_data()
        records, lookback_start, recent_cutoff, max_date = calculate_drop_offs(
            dataframe
        )
    except PayrollDataError as exc:
        return templates.TemplateResponse(
            "apps/client_drop_off.html",
            {"request": request, "error": str(exc)},
            status_code=400,
        )

    staffing_managers = sorted(
        {
            row.get("Staffing Manager", "")
            for row in records
            if row.get("Staffing Manager")
        }
    )

    context = {
        "request": request,
        "records": records,
        "lookback_start": lookback_start,
        "recent_cutoff": recent_cutoff,
        "max_date": max_date,
        "total_clients": len(records),
        "staffing_managers": staffing_managers,
    }
    return templates.TemplateResponse("apps/client_drop_off.html", context)


@router.get("/notes", response_class=JSONResponse, name="client_drop_off_notes")
async def client_drop_off_notes() -> JSONResponse:
    return JSONResponse({"notes": _load_notes_map()})


@router.post("/notes", response_class=JSONResponse, name="client_drop_off_notes_save")
async def client_drop_off_notes_save(payload: ClientNotePayload) -> JSONResponse:
    client_name = payload.client.strip()
    note_text = payload.text.strip()
    if not client_name or not note_text:
        return JSONResponse(
            {"error": "Client and note text are required."}, status_code=400
        )

    notes_map = _load_notes_map()
    client_key = _normalize_client_key(client_name)
    notes = notes_map.get(client_key, [])
    notes.insert(0, {"text": note_text, "date": date.today().isoformat()})
    notes_map[client_key] = notes
    _save_notes_map(notes_map)

    return JSONResponse({"client_key": client_key, "notes": notes})


@router.get("/contacted", response_class=JSONResponse, name="client_drop_off_contacted")
async def client_drop_off_contacted() -> JSONResponse:
    return JSONResponse({"contacted": _load_contacted_map()})


@router.post(
    "/contacted", response_class=JSONResponse, name="client_drop_off_contacted_save"
)
async def client_drop_off_contacted_save(
    payload: ClientContactedPayload,
) -> JSONResponse:
    client_name = payload.client.strip()
    ignore_until = payload.ignore_until.strip()
    if not client_name or not ignore_until:
        return JSONResponse(
            {"error": "Client and ignore-until date are required."}, status_code=400
        )

    try:
        parsed_ignore_until = date.fromisoformat(ignore_until)
    except ValueError:
        return JSONResponse(
            {"error": "Ignore-until date must be in ISO format (YYYY-MM-DD)."},
            status_code=400,
        )

    if parsed_ignore_until <= date.today():
        return JSONResponse(
            {"error": "Ignore-until date must be in the future."}, status_code=400
        )

    contacted_map = _load_contacted_map()
    client_key = _normalize_client_key(client_name)
    contacted_map[client_key] = parsed_ignore_until.isoformat()
    _save_contacted_map(contacted_map)

    return JSONResponse({"contacted": contacted_map})


@router.delete(
    "/contacted", response_class=JSONResponse, name="client_drop_off_contacted_delete"
)
async def client_drop_off_contacted_delete(
    payload: ClientContactedDeletePayload,
) -> JSONResponse:
    client_name = payload.client.strip()
    if not client_name:
        return JSONResponse({"error": "Client is required."}, status_code=400)

    contacted_map = _load_contacted_map()
    client_key = _normalize_client_key(client_name)
    if client_key in contacted_map:
        contacted_map.pop(client_key, None)
        _save_contacted_map(contacted_map)

    return JSONResponse({"contacted": contacted_map})


def _normalize_client_key(name: str) -> str:
    return name.strip().lower()


def _load_notes_map() -> Dict[str, List[Dict[str, str]]]:
    if not NOTES_FILE.exists():
        return {}

    try:
        data = json.loads(NOTES_FILE.read_text())
    except json.JSONDecodeError:
        return {}

    if not isinstance(data, dict):
        return {}

    normalized: Dict[str, List[Dict[str, str]]] = {}
    for key, value in data.items():
        if not isinstance(key, str) or not isinstance(value, list):
            continue
        cleaned_notes = []
        for entry in value:
            if not isinstance(entry, dict):
                continue
            text = str(entry.get("text", "")).strip()
            entry_date = str(entry.get("date", "")).strip()
            if text and entry_date:
                cleaned_notes.append({"text": text, "date": entry_date})
        if cleaned_notes:
            normalized[key] = cleaned_notes
    return normalized


def _save_notes_map(notes_map: Dict[str, List[Dict[str, str]]]) -> None:
    NOTES_FILE.write_text(json.dumps(notes_map, indent=2))


def _load_contacted_map() -> Dict[str, str]:
    if not CONTACTED_FILE.exists():
        return {}

    try:
        data = json.loads(CONTACTED_FILE.read_text())
    except json.JSONDecodeError:
        return {}

    if not isinstance(data, dict):
        return {}

    cleaned: Dict[str, str] = {}
    today = date.today()
    for key, value in data.items():
        if not isinstance(key, str) or not isinstance(value, str):
            continue
        try:
            ignore_date = date.fromisoformat(value)
        except ValueError:
            continue
        if ignore_date <= today:
            continue
        cleaned[key] = ignore_date.isoformat()

    if cleaned != data:
        _save_contacted_map(cleaned)

    return cleaned


def _save_contacted_map(contacted_map: Dict[str, str]) -> None:
    CONTACTED_FILE.write_text(json.dumps(contacted_map, indent=2))
