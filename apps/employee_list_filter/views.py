from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Dict, List, Sequence
from uuid import uuid4

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from apps.context import default_employee_filter_context, default_text_blast_context


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
    prepared["Start Date"] = prepared["Start Date"].dt.normalize()

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

    normalized_dates = df["Start Date"].dropna()
    start_date_min = normalized_dates.min()
    start_date_max = normalized_dates.max()

    positions: List[str] = sorted({position for values in df["_positions_list"] for position in values})

    return {
        "statuses": _options_for("Status"),
        "cities": _options_for("City"),
        "states": _options_for("State"),
        "positions": positions,
        "counties": _options_for("County of Residence"),
        "start_date_min": start_date_min.strftime("%Y-%m-%d") if start_date_min is not None else "",
        "start_date_max": start_date_max.strftime("%Y-%m-%d") if start_date_max is not None else "",
    }


def _apply_filters(
    df: pd.DataFrame,
    statuses: Sequence[str],
    cities: Sequence[str],
    state: str,
    start_date_start: str,
    start_date_end: str,
    positions: Sequence[str],
    counties: Sequence[str],
) -> pd.DataFrame:
    filtered = df.copy()

    normalized_statuses = {value.strip() for value in statuses if value and value.strip()}
    if normalized_statuses:
        filtered = filtered[filtered["Status"].astype(str).str.strip().isin(normalized_statuses)]

    normalized_cities = {value.strip() for value in cities if value and value.strip()}
    if normalized_cities:
        filtered = filtered[filtered["City"].astype(str).str.strip().isin(normalized_cities)]

    if state and state.lower() != "all":
        filtered = filtered[filtered["State"].astype(str).str.strip() == state]

    parsed_start = pd.to_datetime(start_date_start, errors="coerce") if start_date_start else None
    parsed_end = pd.to_datetime(start_date_end, errors="coerce") if start_date_end else None

    if parsed_start is not None:
        parsed_start = parsed_start.normalize()
        filtered = filtered[filtered["Start Date"] >= parsed_start]

    if parsed_end is not None:
        parsed_end = parsed_end.normalize()
        filtered = filtered[filtered["Start Date"] <= parsed_end]

    normalized_positions = {value.strip() for value in positions if value and value.strip()}
    if normalized_positions:
        filtered = filtered[filtered["_positions_list"].apply(lambda items: any(pos in items for pos in normalized_positions))]

    normalized_counties = {value.strip() for value in counties if value and value.strip()}
    if normalized_counties:
        filtered = filtered[filtered["County of Residence"].astype(str).str.strip().isin(normalized_counties)]

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
async def page():
    return RedirectResponse(url="/text-blast-filter", status_code=303)


@router.post("/upload")
async def upload(request: Request, file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".xlsx"):
        context: Dict[str, object] = {"request": request}
        context.update(default_text_blast_context())
        context.update(default_employee_filter_context())
        context.update({"employee_error": "Please upload an .xlsx file."})

        return templates.TemplateResponse(
            "apps/text_blast_filter.html",
            context,
            status_code=400,
        )

    file_contents = await file.read()
    if not file_contents:
        context = {"request": request}
        context.update(default_text_blast_context())
        context.update(default_employee_filter_context())
        context.update({"employee_error": "The uploaded file was empty."})

        return templates.TemplateResponse(
            "apps/text_blast_filter.html",
            context,
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
        context = {"request": request}
        context.update(default_text_blast_context())
        context.update(default_employee_filter_context())
        context.update({"employee_error": exc.detail})

        return templates.TemplateResponse(
            "apps/text_blast_filter.html",
            context,
            status_code=exc.status_code,
        )

    options = _collect_filter_options(dataframe)

    context = {"request": request}
    context.update(default_text_blast_context())
    context.update(default_employee_filter_context())
    context.update(
        {
            "employee_options": options,
            "employee_file_token": file_token,
            "employee_uploaded_filename": file.filename,
        }
    )

    return templates.TemplateResponse("apps/text_blast_filter.html", context)


@router.post("/process")
async def process(
    file_token: str = Form(...),
    statuses: List[str] = Form([]),
    cities: List[str] = Form([]),
    state: str = Form("All"),
    start_date_start: str = Form(""),
    start_date_end: str = Form(""),
    positions: List[str] = Form([]),
    counties: List[str] = Form([]),
):
    saved_path = (UPLOAD_DIR / Path(file_token).name).resolve()

    if not str(saved_path).startswith(str(UPLOAD_DIR.resolve())) or not saved_path.exists():
        raise HTTPException(status_code=400, detail="The uploaded file could not be found. Please upload it again.")

    dataframe = _prepare_dataframe(_load_dataframe(saved_path))

    filtered = _apply_filters(
        dataframe,
        statuses,
        cities,
        state.strip(),
        start_date_start.strip(),
        start_date_end.strip(),
        positions,
        counties,
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
