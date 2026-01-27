from __future__ import annotations

from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import re
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="templates")

UPLOAD_DIR = Path("tmp/payroll_recruiting_reports")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_EMPLOYEE_START_DAYS = 90


def _default_dates() -> Tuple[str, str]:
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=DEFAULT_EMPLOYEE_START_DAYS)
    return start_date.isoformat(), end_date.isoformat()


def _read_excel(uploaded_file: UploadFile) -> pd.DataFrame:
    try:
        contents = uploaded_file.file.read()
    except Exception as exc:  # pragma: no cover - file handling safeguard
        raise HTTPException(status_code=400, detail=f"Unable to read the uploaded file: {exc}") from exc

    if not contents:
        raise HTTPException(status_code=400, detail="The uploaded file was empty.")

    try:
        dataframe = pd.read_excel(BytesIO(contents))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Couldn't read that Excel file: {exc}") from exc

    if dataframe.empty:
        raise HTTPException(status_code=400, detail="The uploaded file contains no rows.")

    dataframe = dataframe.dropna(how="all").copy()
    dataframe.columns = [str(column).strip() for column in dataframe.columns]
    return dataframe


def _pick_status_before_industry(df: pd.DataFrame) -> Tuple[Optional[str], str]:
    cols = list(df.columns)
    canon = [str(column).strip().lower() for column in cols]

    industry_idx = None
    for idx, name in enumerate(canon):
        if name == "industry" or name.startswith("industry"):
            industry_idx = idx
            break

    candidates: List[Tuple[int, int, str]] = []
    for idx, name in enumerate(canon):
        if name == "status":
            candidates.append((0, idx, cols[idx]))
        elif name.startswith("status"):
            candidates.append((1, idx, cols[idx]))
        elif re.search(r"\bstatus\b", name):
            candidates.append((2, idx, cols[idx]))

    if not candidates:
        return None, "No 'Status' column found."

    if industry_idx is not None:
        before = [candidate for candidate in candidates if candidate[1] < industry_idx]
        if before:
            before_sorted = sorted(before, key=lambda candidate: (candidate[1], -candidate[0]))
            prio, idx, name = before_sorted[-1]
            return name, f"Using Status column '{name}' at index {idx} (before Industry at {industry_idx})."

        candidates_sorted = sorted(candidates, key=lambda candidate: (candidate[0], candidate[1]))
        prio, idx, name = candidates_sorted[0]
        return name, f"No 'Status' before 'Industry'; using '{name}' at index {idx}."

    candidates_sorted = sorted(candidates, key=lambda candidate: (candidate[0], candidate[1]))
    prio, idx, name = candidates_sorted[0]
    return name, f"No 'Industry' column; using '{name}' at index {idx}."


def _prepare_table(df: pd.DataFrame, max_rows: int = 25) -> Dict[str, List[Dict[str, Any]]]:
    preview = df.head(max_rows).copy()
    preview = preview.replace({pd.NA: "", np.nan: ""}).infer_objects(copy=False)

    for column in preview.columns:
        if pd.api.types.is_datetime64_any_dtype(preview[column]):
            preview[column] = preview[column].dt.strftime("%Y-%m-%d")

    rows = preview.to_dict(orient="records")
    return {"headers": list(preview.columns), "rows": rows}


def _base_context() -> Dict[str, Any]:
    start_date, end_date = _default_dates()
    return {
        "payroll_error": None,
        "payroll_messages": [],
        "payroll_status_message": None,
        "payroll_metrics": None,
        "payroll_breakdown": None,
        "payroll_preview": None,
        "payroll_download_token": None,
        "payroll_filter_error": None,
        "employee_error": None,
        "employee_preview": None,
        "employee_count": None,
        "employee_download_token": None,
        "employee_start_date": start_date,
        "employee_end_date": end_date,
    }


@router.get("")
async def page(request: Request):
    context = _base_context()
    context["request"] = request
    return templates.TemplateResponse("apps/payroll_recruiting_reports.html", context)


@router.post("/payroll")
async def payroll_report(request: Request, payroll_file: UploadFile = File(...)):
    context = _base_context()
    context["request"] = request

    if not payroll_file:
        context["payroll_error"] = "Please upload a payroll Excel file."
        return templates.TemplateResponse("apps/payroll_recruiting_reports.html", context, status_code=400)

    if not payroll_file.filename.lower().endswith((".xlsx", ".xls")):
        context["payroll_error"] = "Please upload an Excel file (.xlsx or .xls)."
        return templates.TemplateResponse("apps/payroll_recruiting_reports.html", context, status_code=400)

    try:
        df = _read_excel(payroll_file)
    except HTTPException as exc:
        context["payroll_error"] = exc.detail
        return templates.TemplateResponse("apps/payroll_recruiting_reports.html", context, status_code=exc.status_code)

    messages: List[str] = []

    if "Total Bill" in df.columns:
        tb_numeric = pd.to_numeric(df["Total Bill"], errors="coerce").fillna(0)
    else:
        tb_numeric = pd.Series([0] * len(df), index=df.index)
        messages.append("Column 'Total Bill' not found; Shift Count will be 0.")

    status_col, status_message = _pick_status_before_industry(df)
    context["payroll_status_message"] = status_message

    if status_col is None:
        status_str = pd.Series(["" for _ in range(len(df))], index=df.index)
        messages.append("Column 'Status' not found; status-based counts will be 0.")
    else:
        status_str = df[status_col].astype(str).fillna("")

    def _contains(pattern: str) -> pd.Series:
        return status_str.str.contains(pattern, case=False, na=False, regex=True)

    no_show_mask = _contains(r"\bno[\s-]?show\b")
    sent_home_mask = _contains(r"\bsent\s*home\b")
    cancelled_mask = _contains(r"\bcancel+l?ed\b")
    worked_mask = _contains(r"\bworked\b")

    total_count = int((no_show_mask | sent_home_mask | cancelled_mask | worked_mask).sum())
    shift_count = int((tb_numeric > 0).sum())

    breakdown = [
        ("No Shows", int(no_show_mask.sum())),
        ("Sent Home (Total Bill > 0)", int((sent_home_mask & (tb_numeric > 0)).sum())),
        ("Sent Home (All)", int(sent_home_mask.sum())),
        ("Cancelled (Total Bill > 0)", int((cancelled_mask & (tb_numeric > 0)).sum())),
        ("Cancelled (All)", int(cancelled_mask.sum())),
        ("Worked", int(worked_mask.sum())),
    ]

    context["payroll_messages"] = messages
    context["payroll_metrics"] = {
        "total_count": total_count,
        "shift_count": shift_count,
        "total_bill_sum": None,
    }
    context["payroll_breakdown"] = breakdown

    if "Client Won Date" not in df.columns:
        context["payroll_filter_error"] = "Column 'Client Won Date' not found in the uploaded file."
        return templates.TemplateResponse("apps/payroll_recruiting_reports.html", context)

    df["Client Won Date"] = pd.to_datetime(df["Client Won Date"], errors="coerce")
    today = pd.Timestamp(datetime.now().date())
    cutoff = today - pd.Timedelta(days=185)
    mask = (df["Client Won Date"] >= cutoff) & (df["Client Won Date"] <= today)
    filtered = df.loc[mask].copy()

    if "Total Bill" in filtered.columns:
        filtered["Total Bill"] = pd.to_numeric(filtered["Total Bill"], errors="coerce")
        context["payroll_metrics"]["total_bill_sum"] = float(filtered["Total Bill"].sum(skipna=True))

    keep_cols = ["Client", "Total Bill", "Client Won Date", "Sales Executive"]
    existing = [col for col in keep_cols if col in filtered.columns]
    filtered_min = filtered[existing].copy() if existing else filtered.copy()

    context["payroll_preview"] = _prepare_table(filtered_min)

    token = datetime.now().strftime("%Y%m%d%H%M%S%f")
    output_path = UPLOAD_DIR / f"payroll_{token}.xlsx"
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        filtered_min.to_excel(writer, index=False, sheet_name="Filtered")
    context["payroll_download_token"] = token

    return templates.TemplateResponse("apps/payroll_recruiting_reports.html", context)


@router.post("/employee")
async def employee_report(
    request: Request,
    employee_file: UploadFile = File(...),
    start_date: str = Form(...),
    end_date: str = Form(...),
):
    context = _base_context()
    context["request"] = request
    context["employee_start_date"] = start_date
    context["employee_end_date"] = end_date

    if not employee_file:
        context["employee_error"] = "Please upload an employee Excel file."
        return templates.TemplateResponse("apps/payroll_recruiting_reports.html", context, status_code=400)

    if not employee_file.filename.lower().endswith((".xlsx", ".xls")):
        context["employee_error"] = "Please upload an Excel file (.xlsx or .xls)."
        return templates.TemplateResponse("apps/payroll_recruiting_reports.html", context, status_code=400)

    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        context["employee_error"] = "Please provide a valid start and end date."
        return templates.TemplateResponse("apps/payroll_recruiting_reports.html", context, status_code=400)

    if start_dt > end_dt:
        context["employee_error"] = "Start of date range must be on or before the end date."
        return templates.TemplateResponse("apps/payroll_recruiting_reports.html", context, status_code=400)

    try:
        df = _read_excel(employee_file)
    except HTTPException as exc:
        context["employee_error"] = exc.detail
        return templates.TemplateResponse("apps/payroll_recruiting_reports.html", context, status_code=exc.status_code)

    if "Start Date" in df.columns:
        df["Start Date"] = pd.to_datetime(df["Start Date"], errors="coerce")
    if "Rehire Date" in df.columns:
        df["Rehire Date"] = pd.to_datetime(df["Rehire Date"], errors="coerce")

    start_ts = pd.Timestamp(start_dt)
    end_ts = pd.Timestamp(end_dt)

    start_in = (
        df["Start Date"].between(start_ts, end_ts, inclusive="both")
        if "Start Date" in df.columns
        else pd.Series(False, index=df.index)
    )
    rehire_in = (
        df["Rehire Date"].between(start_ts, end_ts, inclusive="both")
        if "Rehire Date" in df.columns
        else pd.Series(False, index=df.index)
    )

    filtered = df.loc[start_in | rehire_in].copy()

    keep_cols = [
        "Employee ID",
        "First Name",
        "Last Name",
        "County of Residence",
        "Start Date",
        "Rehire Date",
    ]
    existing = [col for col in keep_cols if col in filtered.columns]
    filtered_min = filtered[existing].copy() if existing else filtered.copy()

    context["employee_count"] = len(filtered_min)
    context["employee_preview"] = _prepare_table(filtered_min)

    token = datetime.now().strftime("%Y%m%d%H%M%S%f")
    output_path = UPLOAD_DIR / f"employee_{token}.xlsx"
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        filtered_min.to_excel(writer, index=False, sheet_name="Filtered")
    context["employee_download_token"] = token

    return templates.TemplateResponse("apps/payroll_recruiting_reports.html", context)


@router.get("/download/payroll/{token}")
async def download_payroll(token: str):
    path = UPLOAD_DIR / f"payroll_{token}.xlsx"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Report not found.")

    buffer = BytesIO(path.read_bytes())
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=Payroll_Filtered.xlsx"},
    )


@router.get("/download/employee/{token}")
async def download_employee(token: str):
    path = UPLOAD_DIR / f"employee_{token}.xlsx"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Report not found.")

    buffer = BytesIO(path.read_bytes())
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=Employee_Filtered.xlsx"},
    )


@router.get("/redirect")
async def redirect_to_reports():
    return RedirectResponse(url="/work-in-progress", status_code=303)
