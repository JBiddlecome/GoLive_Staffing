from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Dict, List, Tuple
from uuid import uuid4

import pandas as pd
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

DATE_COL_CANDIDATES = {
    "start_date": ["Start Date", "StartDate", "Start_Date", "start_date", "startdate"],
    "rehire_date": ["Rehire Date", "RehireDate", "Rehire_Date", "rehire_date", "rehiredate"],
}
COUNTY_COL_CANDIDATES = [
    "County of Residence",
    "County",
    "County_of_Residence",
    "county_of_residence",
    "county",
]
POSITIONS_COL_CANDIDATES = ["Positions", "Position(s)", "positions"]
TARGET_POSITIONS = ["Cook 2", "Server 2", "Dishwasher"]

UPLOAD_DIR = Path("tmp/recruiting_metrics_uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

templates = Jinja2Templates(directory="templates")
router = APIRouter()


@router.get("", response_class=HTMLResponse)
async def page(request: Request) -> HTMLResponse:
    context = {"request": request}
    return templates.TemplateResponse("apps/recruiting_metrics.html", context)


@router.post("/upload", response_class=HTMLResponse)
async def upload_file(request: Request, file: UploadFile = File(...)) -> HTMLResponse:
    file_bytes = await file.read()
    if not file_bytes:
        return _error_response(request, "The uploaded file was empty.")

    try:
        dataframe = _load_dataframe(file_bytes, file.filename)
    except ValueError as exc:
        return _error_response(request, str(exc))

    if dataframe.empty:
        return _error_response(request, "The uploaded file does not contain any rows.")

    try:
        resolved = _resolve_columns(dataframe)
    except ValueError as exc:
        return _error_response(request, str(exc))

    sundays = _pick_week_ending_sundays(dataframe, start_year=2025, resolved=resolved)
    if not sundays:
        return _error_response(
            request,
            "No Sunday week-ending dates found (starting in 2025). Check your Start Date / Rehire Date columns.",
        )

    token = uuid4().hex + Path(file.filename).suffix.lower()
    file_path = UPLOAD_DIR / token
    file_path.write_bytes(file_bytes)
    _write_metadata(file_path, {"filename": file.filename})

    selected_sunday = sundays[-1]
    metrics, details = _build_metrics(dataframe, selected_sunday, resolved)

    context = _build_context(
        request=request,
        filename=file.filename,
        token=token,
        sundays=sundays,
        selected_sunday=selected_sunday,
        metrics=metrics,
        details=details,
    )
    return templates.TemplateResponse("apps/recruiting_metrics.html", context)


@router.post("/select-week", response_class=HTMLResponse)
async def select_week(
    request: Request,
    token: str = Form(...),
    week_ending: str = Form(...),
) -> HTMLResponse:
    file_path = UPLOAD_DIR / token
    if not file_path.exists():
        return _error_response(request, "The uploaded file could not be found. Please upload it again.")

    metadata = _read_metadata(file_path)
    file_bytes = file_path.read_bytes()

    try:
        dataframe = _load_dataframe(file_bytes, file_path.name)
    except ValueError as exc:
        return _error_response(request, str(exc))

    if dataframe.empty:
        return _error_response(request, "The uploaded file does not contain any rows.")

    try:
        resolved = _resolve_columns(dataframe)
    except ValueError as exc:
        return _error_response(request, str(exc))

    sundays = _pick_week_ending_sundays(dataframe, start_year=2025, resolved=resolved)
    if not sundays:
        return _error_response(
            request,
            "No Sunday week-ending dates found (starting in 2025). Check your Start Date / Rehire Date columns.",
        )

    selected_sunday = pd.to_datetime(week_ending, errors="coerce")
    if pd.isna(selected_sunday):
        selected_sunday = sundays[-1]
    else:
        selected_sunday = selected_sunday.normalize()
        if selected_sunday not in sundays:
            # fall back to closest matching week ending
            selected_sunday = max(
                (s for s in sundays if s <= selected_sunday),
                default=sundays[-1],
            )

    metrics, details = _build_metrics(dataframe, selected_sunday, resolved)

    context = _build_context(
        request=request,
        filename=metadata.get("filename", "Uploaded file"),
        token=token,
        sundays=sundays,
        selected_sunday=selected_sunday,
        metrics=metrics,
        details=details,
    )
    return templates.TemplateResponse("apps/recruiting_metrics.html", context)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _error_response(request: Request, message: str) -> HTMLResponse:
    context = {"request": request, "rm_error": message}
    return templates.TemplateResponse("apps/recruiting_metrics.html", context, status_code=400)


def _load_dataframe(file_bytes: bytes, filename: str) -> pd.DataFrame:
    buffer = io.BytesIO(file_bytes)
    try:
        if filename.lower().endswith(".csv"):
            df = pd.read_csv(buffer)
        else:
            df = pd.read_excel(buffer)
    except Exception as exc:  # pragma: no cover - pandas specific errors
        raise ValueError(f"Could not read file: {exc}") from exc

    return df


def _resolve_columns(df: pd.DataFrame) -> Dict[str, str]:
    start_col = _resolve_column(df, DATE_COL_CANDIDATES["start_date"])
    rehire_col = _resolve_column(df, DATE_COL_CANDIDATES["rehire_date"])
    county_col = _resolve_column(df, COUNTY_COL_CANDIDATES)
    positions_col = _resolve_column(df, POSITIONS_COL_CANDIDATES)

    if not start_col and not rehire_col:
        raise ValueError("Missing required date columns. Include at least Start Date or Rehire Date.")

    return {
        "start_col": start_col,
        "rehire_col": rehire_col,
        "county_col": county_col,
        "positions_col": positions_col,
    }


def _resolve_column(df: pd.DataFrame, candidates: List[str]) -> str:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate

    lower_map = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    return ""


def _pick_week_ending_sundays(
    df: pd.DataFrame, *, start_year: int, resolved: Dict[str, str]
) -> List[pd.Timestamp]:
    date_series: List[pd.Series] = []
    for key in ("start_col", "rehire_col"):
        column = resolved.get(key)
        if column:
            date_series.append(pd.to_datetime(df[column], errors="coerce"))

    if not date_series:
        return []

    all_dates = pd.concat(date_series, ignore_index=True).dropna()
    if all_dates.empty:
        return []

    sundays = all_dates.map(_compute_week_ending_sunday).dropna().unique()
    if len(sundays) == 0:
        return []

    sunday_series = pd.Series(list(sundays))
    sunday_series = sunday_series.dt.normalize()
    sunday_series = sunday_series[sunday_series.dt.year >= start_year]
    sunday_series = pd.to_datetime(pd.Series(pd.unique(sunday_series))).sort_values()
    return list(sunday_series)


def _compute_week_ending_sunday(any_date: pd.Timestamp) -> pd.Timestamp:
    if pd.isna(any_date):
        return pd.NaT
    weekday = any_date.weekday()
    delta = 6 - weekday
    return (any_date + pd.Timedelta(days=delta)).normalize()


def _week_bounds_from_sunday(week_ending: pd.Timestamp) -> Tuple[pd.Timestamp, pd.Timestamp]:
    start = week_ending - pd.Timedelta(days=6)
    return start.normalize(), week_ending.normalize()


def _build_week_series(center_sunday: pd.Timestamp) -> List[pd.Timestamp]:
    offsets = [-14, -7, 0, 7, 14]
    return [center_sunday + pd.Timedelta(days=offset) for offset in offsets]


def _count_new_hires(
    df: pd.DataFrame,
    start_col: str,
    rehire_col: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> int:
    starts = pd.to_datetime(df[start_col], errors="coerce") if start_col else pd.Series(pd.NaT, index=df.index)
    rehires = pd.to_datetime(df[rehire_col], errors="coerce") if rehire_col else pd.Series(pd.NaT, index=df.index)
    mask = (starts.between(start, end, inclusive="both")) | (rehires.between(start, end, inclusive="both"))
    return int(mask.sum())


def _count_rehires(
    df: pd.DataFrame,
    rehire_col: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> int:
    if not rehire_col:
        return 0
    rehires = pd.to_datetime(df[rehire_col], errors="coerce")
    mask = rehires.between(start, end, inclusive="both")
    return int(mask.sum())


def _hires_by_county(
    df: pd.DataFrame,
    county_col: str,
    start_col: str,
    rehire_col: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    starts = pd.to_datetime(df[start_col], errors="coerce") if start_col else pd.Series(pd.NaT, index=df.index)
    rehires = pd.to_datetime(df[rehire_col], errors="coerce") if rehire_col else pd.Series(pd.NaT, index=df.index)
    mask = (starts.between(start, end, inclusive="both")) | (rehires.between(start, end, inclusive="both"))
    subset = df.loc[mask].copy()

    if not county_col:
        subset["County"] = "Unknown"
        county_col = "County"

    subset[county_col] = subset[county_col].fillna("Unknown").astype(str).str.strip()
    counts = subset.groupby(county_col).size().reset_index(name="count")
    counts = counts.sort_values("count", ascending=False).reset_index(drop=True)
    return counts.rename(columns={county_col: "County of Residence"})


def _position_counts_by_county(
    df: pd.DataFrame,
    county_col: str,
    positions_col: str,
    start_col: str,
    rehire_col: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    starts = pd.to_datetime(df[start_col], errors="coerce") if start_col else pd.Series(pd.NaT, index=df.index)
    rehires = pd.to_datetime(df[rehire_col], errors="coerce") if rehire_col else pd.Series(pd.NaT, index=df.index)
    week_mask = (starts.between(start, end, inclusive="both")) | (rehires.between(start, end, inclusive="both"))
    subset = df.loc[week_mask].copy()

    if not county_col:
        subset["County"] = "Unknown"
        county_col = "County"

    subset[county_col] = subset[county_col].fillna("Unknown").astype(str).str.strip()

    if not positions_col:
        base = subset[[county_col]].copy()
        for position in TARGET_POSITIONS:
            base[position] = 0
        base = base.groupby(county_col)[TARGET_POSITIONS].sum().reset_index()
        return base.rename(columns={county_col: "County of Residence"})

    tokens = subset[[county_col, positions_col]].copy()
    tokens[positions_col] = tokens[positions_col].fillna("").astype(str)
    tokens["__tokens"] = tokens[positions_col].str.split(",")

    rows: List[Tuple[str, str]] = []
    for _, row in tokens.iterrows():
        county = str(row[county_col]).strip() if pd.notna(row[county_col]) else "Unknown"
        for token in row["__tokens"] or []:
            cleaned = str(token).strip()
            if not cleaned:
                continue
            matches = [t for t in TARGET_POSITIONS if t.lower() in cleaned.lower()]
            rows.extend((county, match) for match in matches)

    if not rows:
        base = subset[[county_col]].copy()
        for position in TARGET_POSITIONS:
            base[position] = 0
        base = base.groupby(county_col)[TARGET_POSITIONS].sum().reset_index()
        return base.rename(columns={county_col: "County of Residence"})

    matches_df = pd.DataFrame(rows, columns=[county_col, "Position"])
    pivot = (
        matches_df.groupby([county_col, "Position"]).size().unstack(fill_value=0)
    )
    pivot = pivot.reindex(columns=TARGET_POSITIONS, fill_value=0).reset_index()
    pivot = pivot.rename(columns={county_col: "County of Residence"})

    for position in TARGET_POSITIONS:
        if position in pivot.columns:
            pivot[position] = pivot[position].astype(int)

    return pivot


def _build_metrics(
    df: pd.DataFrame,
    selected_sunday: pd.Timestamp,
    resolved: Dict[str, str],
) -> Tuple[Dict[str, object], Dict[str, object]]:
    start_col = resolved.get("start_col", "")
    rehire_col = resolved.get("rehire_col", "")
    county_col = resolved.get("county_col", "")
    positions_col = resolved.get("positions_col", "")

    current_start, current_end = _week_bounds_from_sunday(selected_sunday)
    prior_year_sunday = selected_sunday - relativedelta(years=1)
    prior_start, prior_end = _week_bounds_from_sunday(prior_year_sunday)

    current_weeks = _build_week_series(selected_sunday)
    prior_weeks = _build_week_series(prior_year_sunday)

    def _trend_payload(weeks: List[pd.Timestamp], counter) -> List[Dict[str, object]]:
        payload: List[Dict[str, object]] = []
        for week in weeks:
            week_start, week_end = _week_bounds_from_sunday(week)
            count_value = counter(df, start_col, rehire_col, week_start, week_end)
            payload.append({
                "weekEnding": week.strftime("%Y-%m-%d"),
                "count": int(count_value),
            })
        return payload

    onboarded_current = _trend_payload(current_weeks, _count_new_hires)
    onboarded_prior = _trend_payload(prior_weeks, _count_new_hires)

    def _rehire_counter(df_obj, start_c, rehire_c, week_start, week_end):
        return _count_rehires(df_obj, rehire_c, week_start, week_end)

    rehire_current = _trend_payload(current_weeks, _rehire_counter)
    rehire_prior = _trend_payload(prior_weeks, _rehire_counter)

    county_df = _hires_by_county(df, county_col, start_col, rehire_col, current_start, current_end)
    county_records = [
        {
            "county": str(row["County of Residence"]),
            "count": int(row["count"]),
        }
        for _, row in county_df.iterrows()
    ]

    positions_df = _position_counts_by_county(
        df,
        county_col,
        positions_col,
        start_col,
        rehire_col,
        current_start,
        current_end,
    ).sort_values("County of Residence").reset_index(drop=True)

    position_records = [
        {
            "county": str(row["County of Residence"]),
            **{position: int(row.get(position, 0)) for position in TARGET_POSITIONS},
        }
        for _, row in positions_df.iterrows()
    ]

    metrics = {
        "weekRange": {
            "start": current_start.strftime("%Y-%m-%d"),
            "end": current_end.strftime("%Y-%m-%d"),
        },
        "priorWeekRange": {
            "start": prior_start.strftime("%Y-%m-%d"),
            "end": prior_end.strftime("%Y-%m-%d"),
        },
        "onboarded": {
            "current": onboarded_current,
            "prior": onboarded_prior,
        },
        "rehires": {
            "current": rehire_current,
            "prior": rehire_prior,
        },
        "hiresByCounty": county_records,
        "positions": {
            "counties": [record["county"] for record in position_records],
            "positions": TARGET_POSITIONS,
            "matrix": [
                [record.get(position, 0) for position in TARGET_POSITIONS]
                for record in position_records
            ],
        },
        "positionTable": position_records,
    }

    has_hires = any(record["count"] > 0 for record in county_records)
    has_positions = any(
        sum(record.get(position, 0) for position in TARGET_POSITIONS) > 0
        for record in position_records
    )

    details = {
        "current_start": current_start,
        "current_end": current_end,
        "prior_start": prior_start,
        "prior_end": prior_end,
        "has_hires_by_county": has_hires,
        "has_positions": has_positions,
    }
    return metrics, details


def _build_context(
    *,
    request: Request,
    filename: str,
    token: str,
    sundays: List[pd.Timestamp],
    selected_sunday: pd.Timestamp,
    metrics: Dict[str, object],
    details: Dict[str, object],
) -> Dict[str, object]:
    weeks = [
        {
            "value": sunday.strftime("%Y-%m-%d"),
            "label": sunday.strftime("%B %d, %Y"),
        }
        for sunday in sundays
    ]

    metrics_json = json.dumps(metrics)

    context: Dict[str, object] = {
        "request": request,
        "rm_token": token,
        "rm_weeks": weeks,
        "rm_selected_week": selected_sunday.strftime("%Y-%m-%d"),
        "rm_selected_week_label": selected_sunday.strftime("%B %d, %Y"),
        "rm_uploaded_filename": filename,
        "rm_metrics_json": metrics_json,
        "rm_selected_week_range": _format_range(details["current_start"], details["current_end"]),
        "rm_prior_week_range": _format_range(details["prior_start"], details["prior_end"]),
        "rm_has_hires_by_county": details["has_hires_by_county"],
        "rm_has_positions": details["has_positions"],
        "rm_position_table": metrics["positionTable"],
    }

    return context


def _format_range(start: pd.Timestamp, end: pd.Timestamp) -> str:
    return f"{start.strftime('%B %d, %Y')} – {end.strftime('%B %d, %Y')}"


def _write_metadata(file_path: Path, metadata: Dict[str, str]) -> None:
    meta_path = file_path.with_suffix(file_path.suffix + ".meta.json")
    meta_path.write_text(json.dumps(metadata), encoding="utf-8")


def _read_metadata(file_path: Path) -> Dict[str, str]:
    meta_path = file_path.with_suffix(file_path.suffix + ".meta.json")
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:  # pragma: no cover - defensive
            return {}
    return {}
