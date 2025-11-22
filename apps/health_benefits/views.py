from __future__ import annotations

from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="templates")
router = APIRouter()

BASE_DIR = Path(__file__).resolve().parents[2]
PAYROLL_SEARCH_PATHS = [BASE_DIR / "data" / "payroll.xlsx", BASE_DIR / "payroll.xlsx"]


def _resolve_payroll_source() -> Path:
    for candidate in PAYROLL_SEARCH_PATHS:
        if candidate.exists():
            return candidate
    return PAYROLL_SEARCH_PATHS[0]


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(column).strip() for column in df.columns]
    return df


def find_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    lookup = {column.strip().lower(): column for column in df.columns}
    for name in candidates:
        key = name.strip().lower()
        if key in lookup:
            return lookup[key]
    for column in df.columns:
        for name in candidates:
            if column.strip().lower() == name.strip().lower():
                return column
    return None


def coerce_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.normalize()


def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def _generate_preset_ranges(year_start: int = 2024, year_end: int = 2027) -> List[Dict[str, object]]:
    presets: List[Dict[str, object]] = []
    current = date(year_start, 1, 2)
    end_limit = date(year_end, 12, 1)

    while current <= end_limit:
        start = date(current.year, current.month, 2)
        next_month = start + relativedelta(months=+1)
        end = date(next_month.year, next_month.month, 1)
        label = f"{start.strftime('%m/%d/%Y')} → {end.strftime('%m/%d/%Y')}"
        presets.append(
            {
                "label": label,
                "value": label,
                "start": start,
                "end": end,
                "start_display": start.strftime("%B %d, %Y"),
                "end_display": end.strftime("%B %d, %Y"),
            }
        )
        current = next_month

    return presets


def _default_preset_label(presets: List[Dict[str, object]]) -> str:
    today = date.today()
    for preset in presets:
        start: date = preset["start"]  # type: ignore[assignment]
        end: date = preset["end"]  # type: ignore[assignment]
        if start <= today <= end:
            return preset["value"]  # type: ignore[return-value]
    return presets[-1]["value"] if presets else ""


def _format_window(start: date, end: date) -> Dict[str, str]:
    return {
        "start": start.strftime("%B %d, %Y"),
        "end": end.strftime("%B %d, %Y"),
        "start_iso": start.isoformat(),
        "end_iso": end.isoformat(),
    }


def _build_context(
    request: Request,
    *,
    selected_label: Optional[str] = None,
    error: Optional[str] = None,
    info: Optional[str] = None,
    message: Optional[str] = None,
    results: Optional[List[Dict[str, object]]] = None,
    analysis_complete: bool = False,
    employee_preview: Optional[List[Dict[str, object]]] = None,
    employee_window: Optional[Dict[str, str]] = None,
    payroll_window: Optional[Dict[str, str]] = None,
    employee_count: Optional[int] = None,
    payroll_rows_considered: Optional[int] = None,
    payroll_rows_matched: Optional[int] = None,
) -> Dict[str, object]:
    presets = _generate_preset_ranges()
    allowed_labels = {preset["value"] for preset in presets}
    default_label = _default_preset_label(presets)
    chosen_label = selected_label if selected_label in allowed_labels else default_label
    payroll_path = _resolve_payroll_source()

    context: Dict[str, object] = {
        "request": request,
        "presets": presets,
        "selected_preset": chosen_label,
        "payroll_path": str(payroll_path),
        "error": error,
        "info": info,
        "message": message,
        "results": results or [],
        "analysis_complete": analysis_complete,
        "employee_preview": employee_preview or [],
        "employee_window": employee_window,
        "payroll_window": payroll_window,
        "employee_count": employee_count,
        "payroll_rows_considered": payroll_rows_considered,
        "payroll_rows_matched": payroll_rows_matched,
    }
    return context


def _format_date_value(value: object) -> str:
    if value is None or pd.isna(value):  # type: ignore[arg-type]
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value)


@router.get("")
async def page(request: Request):
    context = _build_context(request)
    return templates.TemplateResponse("apps/health_benefits.html", context)


@router.post("/analyze")
async def analyze(
    request: Request,
    preset_label: str = Form(...),
    employee_file: UploadFile = File(...),
):
    presets = _generate_preset_ranges()
    preset_lookup = {preset["value"]: preset for preset in presets}
    selected = preset_lookup.get(preset_label)

    if selected is None:
        context = _build_context(request, error="Invalid preset selection supplied.")
        return templates.TemplateResponse("apps/health_benefits.html", context, status_code=400)

    if not employee_file.filename or not employee_file.filename.lower().endswith((".xlsx", ".xls")):
        context = _build_context(
            request,
            selected_label=selected["value"],
            error="Please upload an Excel file (.xlsx or .xls) for the Employee List.",
        )
        return templates.TemplateResponse("apps/health_benefits.html", context, status_code=400)

    file_bytes = await employee_file.read()
    if not file_bytes:
        context = _build_context(
            request,
            selected_label=selected["value"],
            error="The uploaded Employee List file was empty.",
        )
        return templates.TemplateResponse("apps/health_benefits.html", context, status_code=400)

    try:
        employee_df = pd.read_excel(BytesIO(file_bytes), dtype=str)
    except ValueError as exc:
        context = _build_context(
            request,
            selected_label=selected["value"],
            error=f"Unable to read the Employee List workbook: {exc}",
        )
        return templates.TemplateResponse("apps/health_benefits.html", context, status_code=400)

    employee_df = normalize_columns(employee_df)
    if employee_df.empty:
        context = _build_context(
            request,
            selected_label=selected["value"],
            error="The Employee List workbook did not contain any rows.",
        )
        return templates.TemplateResponse("apps/health_benefits.html", context, status_code=400)

    employee_id_col = find_col(employee_df, ["Employee ID", "Emp ID", "ID", "#Emp"])
    start_col = find_col(employee_df, ["Start Date", "Start", "StartDate"])
    rehire_col = find_col(employee_df, ["Rehire Date", "Rehire", "RehireDate"])
    first_name_col = find_col(
        employee_df,
        [
            "First Name",
            "First",
            "FirstName",
            "Given Name",
            "Employee First Name",
        ],
    )
    last_name_col = find_col(
        employee_df,
        [
            "Last Name",
            "Last",
            "LastName",
            "Surname",
            "Employee Last Name",
        ],
    )
    mobile_col = find_col(
        employee_df,
        [
            "Mobile",
            "Mobile Phone",
            "Mobile Phone Number",
            "Cell Phone",
            "Cell Phone Number",
            "Cell",
            "Phone",
            "Phone Number",
            "Primary Phone",
            "Primary Phone Number",
        ],
    )
    email_col = find_col(
        employee_df,
        [
            "Email",
            "Email Address",
            "EmailAddress",
            "Primary Email",
            "Primary Email Address",
            "Work Email",
            "Work Email Address",
            "Personal Email",
        ],
    )

    missing_cols = [
        name
        for name, column in {
            "Employee ID": employee_id_col,
            "Start Date": start_col,
            "Rehire Date": rehire_col,
            "First Name": first_name_col,
            "Last Name": last_name_col,
            "Mobile": mobile_col,
            "Email": email_col,
        }.items()
        if column is None
    ]

    if missing_cols:
        context = _build_context(
            request,
            selected_label=selected["value"],
            error=f"Employee List is missing required columns: {', '.join(missing_cols)}.",
        )
        return templates.TemplateResponse("apps/health_benefits.html", context, status_code=400)

    employee_df[start_col] = coerce_date(employee_df[start_col])
    employee_df[rehire_col] = coerce_date(employee_df[rehire_col])

    emp_start: date = selected["start"]  # type: ignore[assignment]
    emp_end: date = selected["end"]  # type: ignore[assignment]
    emp_start_ts = pd.Timestamp(emp_start)
    emp_end_ts = pd.Timestamp(emp_end)

    in_range_mask = (
        (
            employee_df[start_col].notna()
            & (employee_df[start_col] >= emp_start_ts)
            & (employee_df[start_col] <= emp_end_ts)
        )
        |
        (
            employee_df[rehire_col].notna()
            & (employee_df[rehire_col] >= emp_start_ts)
            & (employee_df[rehire_col] <= emp_end_ts)
        )
    )

    employees_in_range = employee_df.loc[in_range_mask].copy()
    employee_ids = (
        employees_in_range[employee_id_col]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda series: series != ""]
        .unique()
        .tolist()
    )

    employee_window = _format_window(emp_start, emp_end)

    employee_preview = [
        {
            "employee_id": str(record.get(employee_id_col, "")),
            "start_date": _format_date_value(record.get(start_col)),
            "rehire_date": _format_date_value(record.get(rehire_col)),
        }
        for record in employees_in_range[[employee_id_col, start_col, rehire_col]].head(50).to_dict("records")
    ]

    employee_details = (
        employees_in_range[
            [employee_id_col, first_name_col, last_name_col, mobile_col, email_col]
        ]
        .dropna(subset=[employee_id_col])
        .copy()
    )
    employee_details[employee_id_col] = employee_details[employee_id_col].astype(str).str.strip()
    for column in [first_name_col, last_name_col, mobile_col, email_col]:
        employee_details[column] = (
            employee_details[column].fillna("").astype(str).str.strip()
        )
    details_lookup = {
        row[employee_id_col]: {
            "first_name": row[first_name_col],
            "last_name": row[last_name_col],
            "mobile": row[mobile_col],
            "email": row[email_col],
        }
        for row in employee_details.drop_duplicates(employee_id_col, keep="first").to_dict("records")
    }

    if not employee_ids:
        context = _build_context(
            request,
            selected_label=selected["value"],
            info="No employees matched the selected preset window based on Start Date or Rehire Date.",
            employee_preview=employee_preview,
            employee_window=employee_window,
            employee_count=0,
        )
        return templates.TemplateResponse("apps/health_benefits.html", context)

    payroll_start = (emp_end + relativedelta(months=+1)).replace(day=1)
    payroll_end = payroll_start + relativedelta(months=+3) - timedelta(days=1)
    payroll_start_ts = pd.Timestamp(payroll_start)
    payroll_end_ts = pd.Timestamp(payroll_end)
    payroll_window = _format_window(payroll_start, payroll_end)

    payroll_source = _resolve_payroll_source()

    if not payroll_source.exists():
        context = _build_context(
            request,
            selected_label=selected["value"],
            error=(
                "Payroll workbook not found. Upload the payroll Excel file to the repository at "
                f"{PAYROLL_SEARCH_PATHS[0]} (preferred) or {PAYROLL_SEARCH_PATHS[1]} so it can be reused."
            ),
            employee_preview=employee_preview,
            employee_window=employee_window,
            employee_count=len(employee_ids),
        )
        return templates.TemplateResponse("apps/health_benefits.html", context, status_code=400)

    try:
        payroll_df = pd.read_excel(payroll_source, dtype=str)
    except ValueError as exc:
        context = _build_context(
            request,
            selected_label=selected["value"],
            error=f"Unable to read the payroll workbook: {exc}",
            employee_preview=employee_preview,
            employee_window=employee_window,
            employee_count=len(employee_ids),
        )
        return templates.TemplateResponse("apps/health_benefits.html", context, status_code=400)

    payroll_df = normalize_columns(payroll_df)
    if payroll_df.empty:
        context = _build_context(
            request,
            selected_label=selected["value"],
            error="The payroll workbook does not contain any rows.",
            employee_preview=employee_preview,
            employee_window=employee_window,
            employee_count=len(employee_ids),
        )
        return templates.TemplateResponse("apps/health_benefits.html", context, status_code=400)

    payroll_date_col = find_col(
        payroll_df,
        [
            "Date",
            "Check Date",
            "Pay Date",
            "Period Ending",
            "Period End Date",
            "Work Date",
            "Week End Date",
        ],
    )
    payroll_emp_col = find_col(payroll_df, ["#Emp", "Emp", "Employee ID", "Emp ID", "ID"])
    payroll_reg_col = find_col(payroll_df, ["Reg H (e)", "Reg H(e)", "Reg H", "Regular Hours", "Reg Hours"])
    payroll_ot_col = find_col(payroll_df, ["OT H (e)", "OT H(e)", "OT H", "Overtime Hours", "OT Hours"])
    payroll_dt_col = find_col(payroll_df, ["DT H (e)", "DT H(e)", "DT H", "Doubletime Hours", "DT Hours"])
    payroll_nw_col = find_col(
        payroll_df,
        [
            "Non-Worked Hours (e)",
            "Non Worked Hours (e)",
            "Non-Worked Hours",
            "Non Worked Hours",
            "NW Hours",
        ],
    )

    missing_payroll_cols = [
        name
        for name, column in {
            "Payroll Date": payroll_date_col,
            "#Emp": payroll_emp_col,
            "Reg H (e)": payroll_reg_col,
            "OT H (e)": payroll_ot_col,
            "DT H (e)": payroll_dt_col,
            "Non-Worked Hours (e)": payroll_nw_col,
        }.items()
        if column is None
    ]

    if missing_payroll_cols:
        context = _build_context(
            request,
            selected_label=selected["value"],
            error=f"Payroll workbook is missing required columns: {', '.join(missing_payroll_cols)}.",
            employee_preview=employee_preview,
            employee_window=employee_window,
            employee_count=len(employee_ids),
        )
        return templates.TemplateResponse("apps/health_benefits.html", context, status_code=400)

    payroll_df[payroll_date_col] = coerce_date(payroll_df[payroll_date_col])
    payroll_df[payroll_emp_col] = payroll_df[payroll_emp_col].astype(str).str.strip()

    date_mask = (
        payroll_df[payroll_date_col].notna()
        & (payroll_df[payroll_date_col] >= payroll_start_ts)
        & (payroll_df[payroll_date_col] <= payroll_end_ts)
    )
    payroll_within_window = payroll_df.loc[date_mask].copy()
    rows_considered = len(payroll_within_window)

    if payroll_within_window.empty:
        context = _build_context(
            request,
            selected_label=selected["value"],
            info=(
                "No payroll rows fall within the calculated payroll window. "
                "Adjust the preset window or update the payroll workbook."
            ),
            employee_preview=employee_preview,
            employee_window=employee_window,
            payroll_window=payroll_window,
            employee_count=len(employee_ids),
            payroll_rows_considered=0,
        )
        return templates.TemplateResponse("apps/health_benefits.html", context)

    payroll_matched = payroll_within_window[payroll_within_window[payroll_emp_col].isin(employee_ids)].copy()
    rows_matched = len(payroll_matched)

    if payroll_matched.empty:
        context = _build_context(
            request,
            selected_label=selected["value"],
            info="No payroll rows match the filtered employees within the payroll window.",
            employee_preview=employee_preview,
            employee_window=employee_window,
            payroll_window=payroll_window,
            employee_count=len(employee_ids),
            payroll_rows_considered=rows_considered,
            payroll_rows_matched=0,
        )
        return templates.TemplateResponse("apps/health_benefits.html", context)

    payroll_matched["_reg"] = safe_numeric(payroll_matched[payroll_reg_col])
    payroll_matched["_ot"] = safe_numeric(payroll_matched[payroll_ot_col])
    payroll_matched["_dt"] = safe_numeric(payroll_matched[payroll_dt_col])
    payroll_matched["_nw"] = safe_numeric(payroll_matched[payroll_nw_col])
    payroll_matched["Total Hours"] = payroll_matched[["_reg", "_ot", "_dt", "_nw"]].sum(axis=1)

    totals = (
        payroll_matched.groupby(payroll_emp_col, as_index=False)["Total Hours"].sum().sort_values("Total Hours", ascending=False)
    )

    qualified = totals[totals["Total Hours"] >= 360].copy()
    results = []
    for row in qualified.to_dict("records"):
        employee_id = row[payroll_emp_col]
        details = details_lookup.get(employee_id, {})
        results.append(
            {
                "employee_id": employee_id,
                "first_name": details.get("first_name", ""),
                "last_name": details.get("last_name", ""),
                "mobile": details.get("mobile", ""),
                "email": details.get("email", ""),
                "total_hours": float(row["Total Hours"]),
            }
        )

    context = _build_context(
        request,
        selected_label=selected["value"],
        message="Analysis complete.",
        results=results,
        analysis_complete=True,
        employee_preview=employee_preview,
        employee_window=employee_window,
        payroll_window=payroll_window,
        employee_count=len(employee_ids),
        payroll_rows_considered=rows_considered,
        payroll_rows_matched=rows_matched,
    )

    if not results:
        context["info"] = "No employees have Total Hours ≥ 360 in the selected payroll window."

    return templates.TemplateResponse("apps/health_benefits.html", context)
