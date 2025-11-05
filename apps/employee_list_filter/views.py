from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Dict, List
from uuid import uuid4

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.templating import Jinja2Templates


templates = Jinja2Templates(directory="templates")
router = APIRouter()

REQUIRED_COLUMNS: List[str] = [
    "Employee ID",
    "Status",
    "City",
    "State",
    "Start Date",
    "Rehire Date",
    "Positions",
    "County of Residence",
    "Mobile",
]

UPLOAD_DIR = Path("tmp/employee_list_uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def _load_dataframe(path: Path) -> pd.DataFrame:
    try:
        dataframe = pd.read_excel(path)
    except ValueError as exc:  # pragma: no cover - pandas specific error
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if dataframe.empty:
        raise HTTPException(status_code=400, detail="Uploaded file does not contain any data.")

    dataframe = dataframe.dropna(how="all").copy()

    missing = [column for column in REQUIRED_COLUMNS if column not in dataframe.columns]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Uploaded file is missing required columns: {', '.join(missing)}.",
        )

    return dataframe


def _prepare_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    prepared = dataframe.copy()

    prepared["Start Date"] = pd.to_datetime(prepared["Start Date"], errors="coerce")
    prepared["Rehire Date"] = pd.to_datetime(prepared["Rehire Date"], errors="coerce")
    prepared["Start Date"] = prepared["Rehire Date"].combine_first(prepared["Start Date"])

    prepared["_positions_list"] = (
        prepared["Positions"]
        .fillna("")
        .astype(str)
        .str.split(",")
        .apply(lambda values: [value.strip() for value in values if value and value.strip()])
    )

    return prepared


def _collect_filter_options(df: pd.DataFrame) -> Dict[str, List[str]]:
    def _options_for(column: str) -> List[str]:
        values = df[column].dropna().astype(str).str.strip()
        return sorted({value for value in values if value})

    start_dates = (
        df["Start Date"].dropna().dt.normalize().drop_duplicates().sort_values().dt.strftime("%Y-%m-%d").tolist()
    )

    positions: List[str] = sorted({position for values in df["_positions_list"] for position in values})

    return {
        "statuses": _options_for("Status"),
        "cities": _options_for("City"),
        "states": _options_for("State"),
        "start_dates": start_dates,
        "positions": positions,
        "counties": _options_for("County of Residence"),
    }


def _apply_filters(
    df: pd.DataFrame,
    status: str,
    city: str,
    state: str,
    start_date: str,
    position: str,
    county: str,
) -> pd.DataFrame:
    filtered = df.copy()

    if status and status.lower() != "all":
        filtered = filtered[filtered["Status"].astype(str).str.strip() == status]

    if city and city.lower() != "all":
        filtered = filtered[filtered["City"].astype(str).str.strip() == city]

    if state and state.lower() != "all":
        filtered = filtered[filtered["State"].astype(str).str.strip() == state]

    if start_date and start_date.lower() != "all":
        try:
            target_date = pd.to_datetime(start_date, errors="raise").normalize()
            filtered = filtered[filtered["Start Date"].dt.normalize() == target_date]
        except (TypeError, ValueError):
            pass

    if position and position.lower() != "all":
        filtered = filtered[filtered["_positions_list"].apply(lambda items: position in items)]

    if county and county.lower() != "all":
        filtered = filtered[filtered["County of Residence"].astype(str).str.strip() == county]

    return filtered


def _normalize_mobile(df: pd.DataFrame) -> pd.DataFrame:
    mobile = df["Mobile"].fillna("").astype(str)
    digits = mobile.str.replace(r"\D", "", regex=True)

    valid = digits.str.len() > 0
    valid &= ~digits.str.startswith("0")
    valid &= ~digits.str.startswith("1")

    cleaned = df.loc[valid].copy()
    cleaned.loc[:, "Mobile"] = digits.loc[valid]
    return cleaned


def _remove_invalid_employee_ids(df: pd.DataFrame) -> pd.DataFrame:
    employee_ids = df["Employee ID"].fillna("").astype(str).str.strip()
    valid = employee_ids != ""
    valid &= ~employee_ids.str.contains("deleted", case=False, na=False)
    return df.loc[valid].copy()


@router.get("")
async def page(request: Request):
    return templates.TemplateResponse(
        "apps/employee_list_filter.html",
        {
            "request": request,
            "options": None,
            "selected": {
                "status": "All",
                "city": "All",
                "state": "All",
                "start_date": "All",
                "position": "All",
                "county": "All",
            },
            "file_token": None,
            "uploaded_filename": None,
            "error": None,
        },
    )


@router.post("/upload")
async def upload(request: Request, file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".xlsx"):
        return templates.TemplateResponse(
            "apps/employee_list_filter.html",
            {
                "request": request,
                "options": None,
                "selected": {
                    "status": "All",
                    "city": "All",
                    "state": "All",
                    "start_date": "All",
                    "position": "All",
                    "county": "All",
                },
                "file_token": None,
                "uploaded_filename": None,
                "error": "Please upload an .xlsx file.",
            },
            status_code=400,
        )

    file_contents = await file.read()
    if not file_contents:
        return templates.TemplateResponse(
            "apps/employee_list_filter.html",
            {
                "request": request,
                "options": None,
                "selected": {
                    "status": "All",
                    "city": "All",
                    "state": "All",
                    "start_date": "All",
                    "position": "All",
                    "county": "All",
                },
                "file_token": None,
                "uploaded_filename": None,
                "error": "The uploaded file was empty.",
            },
            status_code=400,
        )

    file_suffix = Path(file.filename).suffix or ".xlsx"
    file_token = f"{uuid4().hex}{file_suffix}"
    saved_path = UPLOAD_DIR / file_token
    saved_path.write_bytes(file_contents)

    try:
        dataframe = _prepare_dataframe(_load_dataframe(saved_path))
    except HTTPException as exc:
        saved_path.unlink(missing_ok=True)
        return templates.TemplateResponse(
            "apps/employee_list_filter.html",
            {
                "request": request,
                "options": None,
                "selected": {
                    "status": "All",
                    "city": "All",
                    "state": "All",
                    "start_date": "All",
                    "position": "All",
                    "county": "All",
                },
                "file_token": None,
                "uploaded_filename": None,
                "error": exc.detail,
            },
            status_code=exc.status_code,
        )

    options = _collect_filter_options(dataframe)

    return templates.TemplateResponse(
        "apps/employee_list_filter.html",
        {
            "request": request,
            "options": options,
            "selected": {
                "status": "All",
                "city": "All",
                "state": "All",
                "start_date": "All",
                "position": "All",
                "county": "All",
            },
            "file_token": file_token,
            "uploaded_filename": file.filename,
            "error": None,
        },
    )


@router.post("/process")
async def process(
    file_token: str = Form(...),
    status: str = Form("All"),
    city: str = Form("All"),
    state: str = Form("All"),
    start_date: str = Form("All"),
    position: str = Form("All"),
    county: str = Form("All"),
):
    saved_path = (UPLOAD_DIR / Path(file_token).name).resolve()

    if not str(saved_path).startswith(str(UPLOAD_DIR.resolve())) or not saved_path.exists():
        raise HTTPException(status_code=400, detail="The uploaded file could not be found. Please upload it again.")

    dataframe = _prepare_dataframe(_load_dataframe(saved_path))

    filtered = _apply_filters(
        dataframe,
        status.strip(),
        city.strip(),
        state.strip(),
        start_date.strip(),
        position.strip(),
        county.strip(),
    )

    cleaned = _normalize_mobile(filtered)
    cleaned = _remove_invalid_employee_ids(cleaned)

    output = cleaned.drop(columns=["_positions_list"], errors="ignore").copy()
    output_buffer = BytesIO()
    output.to_excel(output_buffer, index=False)
    output_buffer.seek(0)

    filename = f"employee_list_{uuid4().hex}.xlsx"
    return StreamingResponse(
        output_buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
