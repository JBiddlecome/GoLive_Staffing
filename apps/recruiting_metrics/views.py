from __future__ import annotations

import base64
import io
import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from openai import OpenAI, OpenAIError
from pypdf import PdfReader

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
CONCIERGE_COL_CANDIDATES = [
    "Concierge Date",
    "ConciergeDate",
    "Concierge_Date",
    "concierge_date",
    "conciergedate",
]
TARGET_POSITIONS = ["Cook 2", "Server 2", "Dishwasher"]

DATA_FILE = Path("Employee List Data.xlsx")

templates = Jinja2Templates(directory="templates")
router = APIRouter()
logger = logging.getLogger(__name__)

RESUME_SYSTEM_PROMPT = """
You are a resume screener for a hospitality staffing agency.
The user will send you a resume (as text or transcribed from a PDF/image).
Your job is to decide how qualified the candidate is for specific hospitality positions, only counting experience at fine-dining or equivalent venues.

Target positions

Evaluate the candidate for these positions:

Cook

Prep Cook

Dishwasher

Utility

Server

Runner

Busser

Bartender

Barback

Cashier

Pastry

Baker

Sushi

Concessions

Barista

Valet

Venue rules (VERY IMPORTANT)

Fast food experience does not qualify for any of the above positions.

Treat clearly fast-food or quick-service restaurants (for example: McDonald’s, Burger King, Wendy’s, Taco Bell, KFC, In-N-Out, Chick-fil-A, similar chains) as non-qualifying for all positions.

Only count experience at fine dining or equivalent hospitality venues, such as:

Hotels, resorts, country clubs

Upscale restaurants, steakhouses, fine dining, chef-driven or white-tablecloth concepts

Banquet / catering companies, convention centers, stadiums, arenas, large event venues

Corporate dining / contract dining for large companies, universities, hospitals, etc., if the role is clearly hospitality/food-service related.

If a venue type is unclear and could reasonably be non-fast-food hospitality (for example “Italian restaurant” with no brand name), you may count it, but lower your confidence.

If a job is obviously non-hospitality (office admin, warehouse, rideshare driver, etc.), do not count it toward any of the positions.

Experience levels

For each of the positions listed above, you must:

Look through the entire work history and find any matching or equivalent roles (for example:

Cook experience can include Line Cook, Prep Cook, Grill Cook, Banquet Cook, Chef de Partie, etc., at qualifying venues.

Server experience can include Banquet Server, Fine Dining Server, Room Service Server, Cocktail Server, etc., at qualifying venues.

Dishwasher / Utility can include Steward, Porter, Utility Worker, etc., at qualifying venues.

Concessions can include food stand worker at stadiums/arenas/large events (not mall food courts / fast food).

Barista can include coffee bar roles at hotels, specialty coffee shops, etc., but not fast-food drive-through roles.

Estimate the total combined time (in years) the candidate has spent in that type of role at qualifying venues. Be reasonable when dates are approximate.

Assign a Level based on total qualifying experience:

Level 1: less than 2 years combined experience

Level 2: 2 to 5 years combined experience

Level 3: more than 5 years combined experience

If there is no clear qualifying experience for a position, mark it as "no_experience" instead of assigning a level.

When estimating experience:

Use job dates if available.

If dates are missing, infer rough duration from context (e.g., “several months” ≈ 0.25–0.5 years).

Avoid double-counting overlapping jobs for the same role.

Output format

Return your result as valid JSON only, using this schema:
{
  "candidate_summary": {
    "hospitality_experience_overview": "",
    "total_hospitality_years_estimate": 0.0,
    "notable_venues": [],
    "notes_on_fast_food_or_non_qualifying_experience": ""
  },
  "positions": {
    "cook": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": [
        "Explain why you chose this level and what roles/venues you counted."
      ]
    },
    "prep_cook": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "dishwasher": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "utility": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "server": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "runner": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "busser": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "bartender": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "barback": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "cashier": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "pastry": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "baker": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "sushi": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "concessions": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "barista": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    },
    "valet": {
      "status": "no_experience | level_1 | level_2 | level_3",
      "estimated_years": 0.0,
      "confidence": 0.0,
      "reasons": []
    }
  }
}

Confidence should be a number between 0.0 and 1.0, where 1.0 means very certain.

In reasons, briefly mention which jobs and venues you counted and why you excluded any fast-food or non-qualifying experience.

Do not include any text outside of the JSON.
"""


@router.get("", response_class=HTMLResponse)
async def page(request: Request, week_ending: str | None = None) -> HTMLResponse:
    return await _render_page(request, week_ending)


@router.post("/select-week", response_class=HTMLResponse)
async def select_week(
    request: Request,
    week_ending: str = Form(...),
) -> HTMLResponse:
    return await _render_page(request, week_ending)


@router.post("/resume-analyzer", response_class=JSONResponse)
async def analyze_resume(file: UploadFile = File(...)) -> JSONResponse:
    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    try:
        client = _get_openai_client()
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    try:
        ai_payload = _build_resume_messages(file.filename, file.content_type, contents)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=ai_payload,
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
        parsed = json.loads(content_block)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail="AI response was not valid JSON.") from exc

    return JSONResponse(parsed)


def _get_openai_client() -> OpenAI:
    api_key = os.getenv("RESUME_ANALYZER_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "RESUME_ANALYZER_OPENAI_API_KEY is not configured. Falling back to OPENAI_API_KEY is also not set."
        )
    return OpenAI(api_key=api_key)


def _build_resume_messages(filename: str, content_type: str, contents: bytes) -> list[dict]:
    name = (filename or "uploaded file").strip()
    mime = (content_type or "").lower()
    lower_name = name.lower()

    if "pdf" in mime or lower_name.endswith(".pdf"):
        text = _extract_pdf_text(contents)
        if not text:
            raise ValueError("Could not read any text from the uploaded PDF.")
        return [
            {"role": "system", "content": RESUME_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": f"Resume extracted from {name}. Return only the JSON schema provided.\n\n{text}",
            },
        ]

    if _is_image_file(mime, lower_name):
        data_url = _encode_image_to_data_url(contents, mime)
        return [
            {"role": "system", "content": RESUME_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Transcribe this resume image and evaluate it using the provided schema. Respond with JSON only.",
                    },
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            },
        ]

    raise ValueError("Only PDF or image resume files are supported.")


def _extract_pdf_text(file_bytes: bytes) -> str:
    buffer = io.BytesIO(file_bytes)
    try:
        reader = PdfReader(buffer)
    except Exception as exc:  # pragma: no cover - external library
        raise ValueError(f"Could not read PDF: {exc}") from exc

    texts: List[str] = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
        except Exception:  # pragma: no cover - library-specific errors
            text = ""
        if text:
            texts.append(text)
    return "\n\n".join(texts).strip()


def _is_image_file(mime: str, lower_name: str) -> bool:
    return mime.startswith("image/") or lower_name.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp"))


def _encode_image_to_data_url(contents: bytes, mime: str) -> str:
    safe_mime = mime if mime.startswith("image/") else "image/png"
    encoded = base64.b64encode(contents).decode("utf-8")
    return f"data:{safe_mime};base64,{encoded}"


async def _render_page(request: Request, week_ending: str | None) -> HTMLResponse:
    try:
        dataframe = _load_default_dataframe()
    except ValueError as exc:
        return _error_response(request, str(exc))

    if dataframe.empty:
        return _error_response(request, "The data file does not contain any rows.")

    try:
        resolved = _resolve_columns(dataframe)
    except ValueError as exc:
        return _error_response(request, str(exc))

    sundays = _pick_week_ending_sundays(dataframe, start_year=2024, resolved=resolved)
    if not sundays:
        return _error_response(
            request,
            "No Sunday week-ending dates found. Check the Start Date and Rehire Date columns in the data file.",
        )

    selected_sunday = _resolve_selected_sunday(week_ending, sundays)

    metrics, details = _build_metrics(dataframe, selected_sunday, resolved)

    context = _build_context(
        request=request,
        filename=DATA_FILE.name,
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
    context = {
        "request": request,
        "rm_error": message,
        "rm_data_source": DATA_FILE.name,
    }
    return templates.TemplateResponse("apps/recruiting_metrics.html", context, status_code=400)


def _load_default_dataframe() -> pd.DataFrame:
    if not DATA_FILE.exists():
        raise ValueError(
            "The data file 'Employee List Data.xlsx' could not be found in the application directory."
        )

    try:
        file_bytes = DATA_FILE.read_bytes()
    except OSError as exc:  # pragma: no cover - filesystem errors
        raise ValueError(f"Could not read data file: {exc}") from exc

    return _load_dataframe(file_bytes, DATA_FILE.name)


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
    concierge_col = _resolve_column(df, CONCIERGE_COL_CANDIDATES)

    if not start_col and not rehire_col:
        raise ValueError("Missing required date columns. Include at least Start Date or Rehire Date.")

    return {
        "start_col": start_col,
        "rehire_col": rehire_col,
        "county_col": county_col,
        "positions_col": positions_col,
        "concierge_col": concierge_col,
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


def _resolve_selected_sunday(
    week_ending: str | None, sundays: List[pd.Timestamp]
) -> pd.Timestamp:
    selected_sunday = pd.to_datetime(week_ending, errors="coerce")
    if pd.isna(selected_sunday):
        return sundays[-1]

    selected_sunday = selected_sunday.normalize()
    if selected_sunday in sundays:
        return selected_sunday

    # Fall back to the closest matching week ending that is not after the requested date
    return max((s for s in sundays if s <= selected_sunday), default=sundays[-1])


def _compute_week_ending_sunday(any_date: pd.Timestamp) -> pd.Timestamp:
    if pd.isna(any_date):
        return pd.NaT
    weekday = any_date.weekday()
    delta = 6 - weekday
    return (any_date + pd.Timedelta(days=delta)).normalize()


def _week_bounds_from_sunday(week_ending: pd.Timestamp) -> Tuple[pd.Timestamp, pd.Timestamp]:
    start = week_ending - pd.Timedelta(days=6)
    return start.normalize(), week_ending.normalize()


def _build_week_series(latest_sunday: pd.Timestamp, weeks: int = 5) -> List[pd.Timestamp]:
    """Return a chronologically ordered list of recent week-ending Sundays."""

    if weeks <= 0:
        return []

    offsets = range(weeks - 1, -1, -1)
    return [latest_sunday - pd.Timedelta(days=7 * offset) for offset in offsets]


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


def _count_concierged(
    df: pd.DataFrame, concierge_col: str, start: pd.Timestamp, end: pd.Timestamp
) -> int:
    if not concierge_col:
        return 0

    concierge_dates = pd.to_datetime(df[concierge_col], errors="coerce")
    if concierge_dates.empty:
        return 0

    mask = concierge_dates.between(start, end, inclusive="both")
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
            normalized = " ".join(cleaned.split()).lower()
            matches = [t for t in TARGET_POSITIONS if normalized == t.lower()]
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
    concierge_col = resolved.get("concierge_col", "")

    current_start, current_end = _week_bounds_from_sunday(selected_sunday)
    prior_year_sunday = selected_sunday - relativedelta(years=1)
    prior_start, prior_end = _week_bounds_from_sunday(prior_year_sunday)

    current_weeks = _build_week_series(selected_sunday)
    prior_weeks = _build_week_series(prior_year_sunday)

    def _trend_payload(
        weeks: List[pd.Timestamp], counter, *, include_start: bool = True
    ) -> List[Dict[str, object]]:
        payload: List[Dict[str, object]] = []
        for week in weeks:
            week_start, week_end = _week_bounds_from_sunday(week)
            start_arg = start_col if include_start else ""
            count_value = counter(df, start_arg, rehire_col, week_start, week_end)
            payload.append(
                {
                    "weekEnding": week.strftime("%Y-%m-%d"),
                    "count": int(count_value),
                }
            )
        return payload

    onboarded_current = _trend_payload(current_weeks, _count_new_hires, include_start=True)
    onboarded_prior = _trend_payload(prior_weeks, _count_new_hires, include_start=True)

    def _rehire_counter(df_obj, _start_c, rehire_c, week_start, week_end):
        return _count_rehires(df_obj, rehire_c, week_start, week_end)

    rehire_current = _trend_payload(current_weeks, _rehire_counter, include_start=False)
    rehire_prior = _trend_payload(prior_weeks, _rehire_counter, include_start=False)

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
            "currentYear": onboarded_current,
            "priorYear": onboarded_prior,
        },
        "rehires": {
            "currentYear": rehire_current,
            "priorYear": rehire_prior,
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
        "columns": {
            "start": start_col or None,
            "rehire": rehire_col or None,
            "county": county_col or None,
            "positions": positions_col or None,
            "concierge": concierge_col or None,
        },
        "conciergeNumbers": {
            "onboardedAndHired": _count_new_hires(
                df, start_col, rehire_col, current_start, current_end
            ),
            "onboardedAndConcierged": _count_concierged(
                df, concierge_col, current_start, current_end
            ),
        },
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
        "rm_data_source": filename,
        "rm_concierge_numbers": metrics["conciergeNumbers"],
    }

    return context


def _format_range(start: pd.Timestamp, end: pd.Timestamp) -> str:
    return f"{start.strftime('%B %d, %Y')} – {end.strftime('%B %d, %Y')}"
