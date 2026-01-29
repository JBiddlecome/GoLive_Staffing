from __future__ import annotations

import json
import sqlite3
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
DATA_DIR = Path("data")
DB_FILE = DATA_DIR / "client_drop_off.db"
LEGACY_NOTES_FILE = DATA_DIR / "client_drop_off_notes.json"
LEGACY_CONTACTED_FILE = DATA_DIR / "client_drop_off_contacted.json"
DATA_DIR.mkdir(parents=True, exist_ok=True)


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
    return JSONResponse({"notes": _load_notes_map()}, headers={"Cache-Control": "no-store"})


@router.post("/notes", response_class=JSONResponse, name="client_drop_off_notes_save")
async def client_drop_off_notes_save(payload: ClientNotePayload) -> JSONResponse:
    client_name = payload.client.strip()
    note_text = payload.text.strip()
    if not client_name or not note_text:
        return JSONResponse(
            {"error": "Client and note text are required."}, status_code=400
        )

    client_key = _normalize_client_key(client_name)
    note_date = date.today().isoformat()
    _init_db()
    with _get_connection() as conn:
        _maybe_migrate_legacy_data(conn)
        conn.execute(
            """
            INSERT INTO client_drop_off_notes
                (client_key, client_name, note_text, created_date)
            VALUES (?, ?, ?, ?)
            """,
            (client_key, client_name, note_text, note_date),
        )
        conn.commit()

    notes = _load_notes_for_client(client_key)

    return JSONResponse({"client_key": client_key, "notes": notes})


@router.get("/contacted", response_class=JSONResponse, name="client_drop_off_contacted")
async def client_drop_off_contacted() -> JSONResponse:
    return JSONResponse(
        {"contacted": _load_contacted_map()}, headers={"Cache-Control": "no-store"}
    )


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

    client_key = _normalize_client_key(client_name)
    ignore_until = parsed_ignore_until.isoformat()
    _init_db()
    with _get_connection() as conn:
        _maybe_migrate_legacy_data(conn)
        conn.execute(
            """
            INSERT INTO client_drop_off_contacted
                (client_key, client_name, ignore_until)
            VALUES (?, ?, ?)
            ON CONFLICT(client_key) DO UPDATE SET
                client_name = excluded.client_name,
                ignore_until = excluded.ignore_until
            """,
            (client_key, client_name, ignore_until),
        )
        conn.commit()

    return JSONResponse({"contacted": _load_contacted_map()})


@router.delete(
    "/contacted", response_class=JSONResponse, name="client_drop_off_contacted_delete"
)
async def client_drop_off_contacted_delete(
    payload: ClientContactedDeletePayload,
) -> JSONResponse:
    client_name = payload.client.strip()
    if not client_name:
        return JSONResponse({"error": "Client is required."}, status_code=400)

    client_key = _normalize_client_key(client_name)
    _init_db()
    with _get_connection() as conn:
        _maybe_migrate_legacy_data(conn)
        conn.execute(
            "DELETE FROM client_drop_off_contacted WHERE client_key = ?",
            (client_key,),
        )
        conn.commit()

    return JSONResponse({"contacted": _load_contacted_map()})


def _normalize_client_key(name: str) -> str:
    return name.strip().lower()


def _load_notes_map() -> Dict[str, List[Dict[str, str]]]:
    _init_db()
    with _get_connection() as conn:
        _maybe_migrate_legacy_data(conn)
        rows = conn.execute(
            """
            SELECT client_key, note_text, created_date
            FROM client_drop_off_notes
            ORDER BY client_key, created_date DESC, id DESC
            """
        ).fetchall()

    normalized: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        client_key = row["client_key"]
        normalized.setdefault(client_key, []).append(
            {"text": row["note_text"], "date": row["created_date"]}
        )
    return normalized


def _load_notes_for_client(client_key: str) -> List[Dict[str, str]]:
    _init_db()
    with _get_connection() as conn:
        _maybe_migrate_legacy_data(conn)
        rows = conn.execute(
            """
            SELECT note_text, created_date
            FROM client_drop_off_notes
            WHERE client_key = ?
            ORDER BY created_date DESC, id DESC
            """,
            (client_key,),
        ).fetchall()
    return [{"text": row["note_text"], "date": row["created_date"]} for row in rows]


def _save_notes_map(notes_map: Dict[str, List[Dict[str, str]]]) -> None:
    _init_db()
    with _get_connection() as conn:
        _maybe_migrate_legacy_data(conn)
        conn.execute("DELETE FROM client_drop_off_notes")
        for client_key, notes in notes_map.items():
            for note in notes:
                conn.execute(
                    """
                    INSERT INTO client_drop_off_notes
                        (client_key, client_name, note_text, created_date)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        client_key,
                        client_key,
                        note.get("text", ""),
                        note.get("date", ""),
                    ),
                )
        conn.commit()


def _load_contacted_map() -> Dict[str, str]:
    _init_db()
    today = date.today().isoformat()
    with _get_connection() as conn:
        _maybe_migrate_legacy_data(conn)
        conn.execute(
            "DELETE FROM client_drop_off_contacted WHERE ignore_until <= ?",
            (today,),
        )
        rows = conn.execute(
            """
            SELECT client_key, ignore_until
            FROM client_drop_off_contacted
            WHERE ignore_until > ?
            """,
            (today,),
        ).fetchall()
        conn.commit()

    cleaned: Dict[str, str] = {}
    for row in rows:
        cleaned[row["client_key"]] = row["ignore_until"]
    return cleaned


def _save_contacted_map(contacted_map: Dict[str, str]) -> None:
    _init_db()
    with _get_connection() as conn:
        _maybe_migrate_legacy_data(conn)
        conn.execute("DELETE FROM client_drop_off_contacted")
        for client_key, ignore_until in contacted_map.items():
            conn.execute(
                """
                INSERT INTO client_drop_off_contacted
                    (client_key, client_name, ignore_until)
                VALUES (?, ?, ?)
                """,
                (client_key, client_key, ignore_until),
            )
        conn.commit()


def _get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db() -> None:
    with _get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS client_drop_off_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_key TEXT NOT NULL,
                client_name TEXT NOT NULL,
                note_text TEXT NOT NULL,
                created_date TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS client_drop_off_contacted (
                client_key TEXT PRIMARY KEY,
                client_name TEXT NOT NULL,
                ignore_until TEXT NOT NULL
            )
            """
        )
        conn.commit()


def _maybe_migrate_legacy_data(conn: sqlite3.Connection) -> None:
    notes_exists = conn.execute(
        "SELECT 1 FROM client_drop_off_notes LIMIT 1"
    ).fetchone()
    contacted_exists = conn.execute(
        "SELECT 1 FROM client_drop_off_contacted LIMIT 1"
    ).fetchone()
    if notes_exists or contacted_exists:
        return

    legacy_notes = _load_legacy_notes_map()
    for client_key, notes in legacy_notes.items():
        for note in notes:
            conn.execute(
                """
                INSERT INTO client_drop_off_notes
                    (client_key, client_name, note_text, created_date)
                VALUES (?, ?, ?, ?)
                """,
                (client_key, client_key, note["text"], note["date"]),
            )

    legacy_contacted = _load_legacy_contacted_map()
    for client_key, ignore_until in legacy_contacted.items():
        conn.execute(
            """
            INSERT INTO client_drop_off_contacted
                (client_key, client_name, ignore_until)
            VALUES (?, ?, ?)
            """,
            (client_key, client_key, ignore_until),
        )
    conn.commit()


def _load_legacy_notes_map() -> Dict[str, List[Dict[str, str]]]:
    if not LEGACY_NOTES_FILE.exists():
        return {}

    try:
        data = json.loads(LEGACY_NOTES_FILE.read_text())
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


def _load_legacy_contacted_map() -> Dict[str, str]:
    if not LEGACY_CONTACTED_FILE.exists():
        return {}

    try:
        data = json.loads(LEGACY_CONTACTED_FILE.read_text())
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
    return cleaned
