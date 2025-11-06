from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from uuid import uuid4

import numpy as np
import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.templating import Jinja2Templates

import re
from datetime import datetime
from difflib import SequenceMatcher


templates = Jinja2Templates(directory="templates")
router = APIRouter()

UPLOAD_DIR = Path("tmp/clickboarding_check")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_CONFIDENCE = 90
DEFAULT_DAYS_BACK = 60


class ClickboardingProcessingError(HTTPException):
    """Domain-specific HTTP exception for validation issues."""


def _load_clickboarding(data: bytes) -> pd.DataFrame:
    try:
        df = pd.read_csv(BytesIO(data))
    except Exception as exc:  # pragma: no cover - pandas specific error
        raise ClickboardingProcessingError(status_code=400, detail="Clickboarding List must be a valid CSV file.") from exc

    if df.empty:
        raise ClickboardingProcessingError(status_code=400, detail="Clickboarding List does not contain any data.")

    return df


def _load_employee_list(data: bytes) -> pd.DataFrame:
    try:
        df = pd.read_excel(BytesIO(data))
    except Exception as exc:  # pragma: no cover - pandas specific error
        raise ClickboardingProcessingError(status_code=400, detail="Employee List must be a valid Excel workbook.") from exc

    if df.empty:
        raise ClickboardingProcessingError(status_code=400, detail="Employee List does not contain any data.")

    return df


def _normalize_clickboarding_name(value: object) -> Tuple[str | None, str | None, str | None]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None, None, None

    text = str(value).strip()
    if not text:
        return None, None, None

    text = re.sub(r"\s+", " ", text)
    text = text.replace(".", "")

    if "," in text:
        last, rest = text.split(",", 1)
        last = last.strip().lower()
        rest = rest.strip()
        first = rest.split()[0].lower() if rest else ""
        return f"{last}, {first}", last, first

    parts = text.split()
    if len(parts) >= 2:
        first = parts[0].lower()
        last = parts[-1].lower()
        return f"{last}, {first}", last, first

    return text.lower(), None, None


def _make_employee_key(first: object, last: object) -> str:
    first_part = "" if first is None else str(first).strip()
    last_part = "" if last is None else str(last).strip()
    cleaned = f"{last_part}, {first_part}".replace(".", "").lower()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _seq_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a or "", b or "").ratio() * 100.0


def _stringify_row(row: Dict[str, object], date_columns: Iterable[str]) -> Dict[str, str]:
    formatted: Dict[str, str] = {}

    for key, value in row.items():
        if key in date_columns and isinstance(value, (pd.Timestamp, datetime)):
            if pd.isna(value):
                formatted[key] = ""
            else:
                formatted[key] = value.strftime("%Y-%m-%d")
            continue

        if isinstance(value, float) and pd.isna(value):
            formatted[key] = ""
            continue

        if value is None:
            formatted[key] = ""
            continue

        formatted[key] = str(value)

    return formatted


def _process(cb: pd.DataFrame, emp: pd.DataFrame, confidence: int, days_back: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    start_col = None
    rehire_col = None
    for column in emp.columns:
        lowered = str(column).strip().lower()
        if lowered == "start date":
            start_col = column
        elif lowered == "rehire date":
            rehire_col = column

    if start_col is None and rehire_col is None:
        raise ClickboardingProcessingError(
            status_code=400,
            detail="Employee List must include a 'Start Date' and/or 'Rehire Date' column.",
        )

    emp = emp.copy()
    if start_col:
        emp["_start_dt"] = pd.to_datetime(emp[start_col], errors="coerce")
    else:
        emp["_start_dt"] = pd.NaT
    if rehire_col:
        emp["_rehire_dt"] = pd.to_datetime(emp[rehire_col], errors="coerce")
    else:
        emp["_rehire_dt"] = pd.NaT

    today = pd.Timestamp(datetime.now().date())
    cutoff = today - pd.Timedelta(days=int(days_back))
    recent_mask = (
        (emp["_start_dt"].notna() & (emp["_start_dt"] >= cutoff))
        | (emp["_rehire_dt"].notna() & (emp["_rehire_dt"] >= cutoff))
    )
    recent = emp.loc[recent_mask].copy()

    if recent.empty:
        raise ClickboardingProcessingError(
            status_code=400,
            detail="No employees found with Start Date or Rehire Date within the selected look-back window.",
        )

    if "First Name" not in recent.columns or "Last Name" not in recent.columns:
        raise ClickboardingProcessingError(
            status_code=400,
            detail="Employee List must include 'First Name' and 'Last Name' columns.",
        )

    recent["_name_key"] = recent.apply(
        lambda row: _make_employee_key(row.get("First Name", ""), row.get("Last Name", "")),
        axis=1,
    )
    recent["_last"] = recent["_name_key"].apply(
        lambda s: s.split(",")[0] if isinstance(s, str) and "," in s else ""
    )

    name_col = None
    for column in cb.columns:
        if str(column).strip().lower() == "name":
            name_col = column
            break
    if name_col is None:
        name_col = cb.columns[0]

    review_rows: List[Dict[str, object]] = []
    match_rows: List[Dict[str, object]] = []

    for _, row in cb.iterrows():
        raw_name = row.get(name_col, None)
        key, last, _first = _normalize_clickboarding_name(raw_name)
        if key is None:
            continue

        if last:
            candidates = recent[recent["_last"] == last]
        else:
            candidates = recent

        if candidates.empty:
            review_rows.append(
                {
                    "Clickboarding Name": raw_name,
                    "Best Match (Employee List)": "",
                    "Confidence %": 0,
                    "Reason": "No candidates with matching last name in recent employees",
                }
            )
            continue

        best_score = -1.0
        best_emp = None
        for _, emp_row in candidates.iterrows():
            score = _seq_similarity(key, emp_row["_name_key"])
            if score > best_score:
                best_score = score
                best_emp = emp_row

        if best_emp is None:
            review_rows.append(
                {
                    "Clickboarding Name": raw_name,
                    "Best Match (Employee List)": "",
                    "Confidence %": 0,
                    "Reason": "Unable to determine a best match",
                }
            )
            continue

        match_entry = {
            "Clickboarding Name": raw_name,
            "Matched Employee": f"{best_emp.get('Last Name', '')}, {best_emp.get('First Name', '')}",
            "Confidence %": round(best_score, 1),
            "Employee ID": best_emp.get("Employee ID", ""),
        }
        if start_col:
            match_entry["Start Date"] = best_emp.get(start_col, "")
        if rehire_col:
            match_entry["Rehire Date"] = best_emp.get(rehire_col, "")

        match_rows.append(match_entry)

        if best_score < confidence:
            review_rows.append(
                {
                    "Clickboarding Name": raw_name,
                    "Best Match (Employee List)": match_entry["Matched Employee"],
                    "Confidence %": round(best_score, 1),
                    "Reason": f"Best available match < {confidence}%",
                }
            )

    review_df = pd.DataFrame(review_rows)
    if not review_df.empty:
        review_df = review_df.sort_values(by=["Confidence %"], ascending=True, na_position="last").reset_index(drop=True)
    else:
        review_df = pd.DataFrame(columns=["Clickboarding Name", "Best Match (Employee List)", "Confidence %", "Reason"])

    match_df = pd.DataFrame(match_rows)
    if not match_df.empty:
        match_df = match_df.sort_values(by=["Confidence %"], ascending=False, na_position="last").reset_index(drop=True)
    else:
        match_df = pd.DataFrame(
            columns=["Clickboarding Name", "Matched Employee", "Confidence %", "Employee ID", "Start Date", "Rehire Date"]
        )

    return review_df, match_df


def _prepare_table(df: pd.DataFrame, date_columns: Iterable[str]) -> Dict[str, List[Dict[str, str]]]:
    if df.empty:
        return {"headers": list(df.columns), "rows": []}

    cleaned = df.replace({pd.NA: "", np.nan: ""})
    records = cleaned.to_dict(orient="records")
    rows = [_stringify_row(record, date_columns) for record in records]
    return {"headers": list(df.columns), "rows": rows}


def _base_context() -> Dict[str, object]:
    return {
        "clickboarding_error": None,
        "clickboarding_review": None,
        "clickboarding_matches": None,
        "clickboarding_download_token": None,
        "clickboarding_uploaded": {
            "clickboarding_filename": None,
            "employee_filename": None,
        },
        "clickboarding_settings": {
            "confidence": DEFAULT_CONFIDENCE,
            "days_back": DEFAULT_DAYS_BACK,
        },
    }


@router.get("")
async def page(request: Request):
    context = _base_context()
    context.update({"request": request})
    return templates.TemplateResponse("apps/clickboarding_check.html", context)


@router.post("/process")
async def process(
    request: Request,
    clickboarding_file: UploadFile = File(...),
    employee_file: UploadFile = File(...),
    confidence: int = Form(DEFAULT_CONFIDENCE),
    days_back: int = Form(DEFAULT_DAYS_BACK),
):
    context = _base_context()
    context.update({"request": request})
    context["clickboarding_settings"]["confidence"] = confidence
    context["clickboarding_settings"]["days_back"] = days_back

    if not clickboarding_file or not employee_file:
        context["clickboarding_error"] = "Please upload both the Clickboarding List CSV and Employee List Excel files."
        return templates.TemplateResponse("apps/clickboarding_check.html", context, status_code=400)

    cb_bytes = await clickboarding_file.read()
    emp_bytes = await employee_file.read()

    if not cb_bytes or not emp_bytes:
        context["clickboarding_error"] = "One or both uploaded files were empty."
        return templates.TemplateResponse("apps/clickboarding_check.html", context, status_code=400)

    context["clickboarding_uploaded"]["clickboarding_filename"] = clickboarding_file.filename
    context["clickboarding_uploaded"]["employee_filename"] = employee_file.filename

    try:
        cb_df = _load_clickboarding(cb_bytes)
        emp_df = _load_employee_list(emp_bytes)
        review_df, match_df = _process(cb_df, emp_df, int(confidence), int(days_back))
    except ClickboardingProcessingError as exc:
        context["clickboarding_error"] = exc.detail
        return templates.TemplateResponse(
            "apps/clickboarding_check.html",
            context,
            status_code=exc.status_code,
        )

    review_table = _prepare_table(review_df, date_columns=[])
    match_table = _prepare_table(match_df, date_columns=["Start Date", "Rehire Date"])

    context["clickboarding_review"] = review_table
    context["clickboarding_matches"] = match_table

    if review_df.empty:
        context["clickboarding_download_token"] = None
    else:
        token = uuid4().hex
        output_path = UPLOAD_DIR / f"review_{token}.csv"
        output_path.write_text(review_df.to_csv(index=False), encoding="utf-8")
        context["clickboarding_download_token"] = token

    return templates.TemplateResponse("apps/clickboarding_check.html", context)


@router.get("/download/{token}")
async def download_review(token: str):
    path = UPLOAD_DIR / f"review_{token}.csv"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Report not found.")

    contents = path.read_bytes()
    buffer = BytesIO(contents)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=clickboarding_to_review_under_threshold.csv"},
    )
