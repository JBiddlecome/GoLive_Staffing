from __future__ import annotations

import io
import re
from datetime import date, datetime, timedelta
from typing import Dict

import pandas as pd
from fastapi import APIRouter, Request, UploadFile, File
from fastapi.responses import StreamingResponse
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="templates")
router = APIRouter()


def _normalise_key(value: str) -> str:
    """Create a simplified key for column lookups."""

    return re.sub(r"[^a-z0-9]", "", value.lower())


def _lookup_column(columns: Dict[str, str], *candidates: str) -> str:
    for candidate in candidates:
        key = _normalise_key(candidate)
        if key in columns:
            return columns[key]
    raise ValueError(f"Could not find required column. Looked for one of: {', '.join(candidates)}")


def _clean_numeric(series: pd.Series) -> pd.Series:
    return (
        pd.to_numeric(series.astype(str).str.replace(r"[^0-9.\-]", "", regex=True), errors="coerce")
        .fillna(0)
    )


def _normalise_name(value: str) -> str:
    return re.sub(r"\s+", " ", str(value).strip()).upper()


async def _read_excel(upload: UploadFile) -> pd.DataFrame:
    try:
        payload = await upload.read()
        return pd.read_excel(io.BytesIO(payload))
    except Exception as exc:  # pragma: no cover - defensive
        raise ValueError(f"Unable to read Excel file '{upload.filename}'.") from exc


def _prepare_ucla_hours(payroll_df: pd.DataFrame, assignments_df: pd.DataFrame) -> tuple[pd.DataFrame, date]:
    payroll_columns = {_normalise_key(col): col for col in payroll_df.columns}
    assignments_columns = {_normalise_key(col): col for col in assignments_df.columns}

    client_col = _lookup_column(payroll_columns, "Client", "Client Name")
    reg_hours_col = _lookup_column(payroll_columns, "Reg H (e)", "Regular Hours")
    ot_hours_col = _lookup_column(payroll_columns, "OT H (e)", "Overtime Hours")
    dt_hours_col = _lookup_column(payroll_columns, "DT H (e)", "Doubletime Hours")
    first_name_col = _lookup_column(payroll_columns, "First Name")
    last_name_col = _lookup_column(payroll_columns, "Last Name")
    payroll_rate_col = _lookup_column(payroll_columns, "Pay Rate")

    assign_no_col = _lookup_column(assignments_columns, "Assign No", "Assignment #", "Assignment Number")
    assign_name_col = _lookup_column(assignments_columns, "Full Name", "Employee Name")
    assign_rate_col = _lookup_column(assignments_columns, "Pay Rate")

    # Filter payroll to UCLA clients only
    payroll_df = payroll_df[
        payroll_df[client_col].astype(str).str.contains("UCLA", case=False, na=False)
    ].copy()

    if payroll_df.empty:
        raise ValueError("No UCLA records found in the payroll spreadsheet.")

    for column in (reg_hours_col, ot_hours_col, dt_hours_col, payroll_rate_col):
        payroll_df[column] = _clean_numeric(payroll_df[column])

    payroll_df["Employee Name"] = (
        payroll_df[first_name_col].fillna("").astype(str).str.strip()
        + " "
        + payroll_df[last_name_col].fillna("").astype(str).str.strip()
    ).str.replace(r"\s+", " ", regex=True).str.strip()
    payroll_df["Employee Key"] = payroll_df["Employee Name"].map(_normalise_name)
    payroll_df = payroll_df[payroll_df["Employee Key"] != ""].copy()
    payroll_df["Total Hours"] = (
        payroll_df[reg_hours_col] + payroll_df[ot_hours_col] + payroll_df[dt_hours_col]
    )
    payroll_df["Pay Rate Clean"] = payroll_df[payroll_rate_col].round(2)

    grouped = (
        payroll_df.groupby(["Employee Key", "Pay Rate Clean"], as_index=False)
        .agg({"Total Hours": "sum", "Employee Name": "first"})
        .rename(columns={"Total Hours": "Hours"})
    )
    grouped["Hours"] = grouped["Hours"].round(2)

    assignments_df = assignments_df.copy()
    assignments_df["Employee Name"] = (
        assignments_df[assign_name_col]
        .astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )
    assignments_df["Employee Key"] = assignments_df["Employee Name"].map(_normalise_name)
    assignments_df["Pay Rate Clean"] = _clean_numeric(assignments_df[assign_rate_col]).round(2)
    assignments_df = assignments_df.dropna(subset=["Employee Key", "Pay Rate Clean"])
    assignments_df = assignments_df[assignments_df["Employee Key"] != ""].copy()
    assignments_df = assignments_df.drop_duplicates(subset=["Employee Key", "Pay Rate Clean"])

    merged = grouped.merge(
        assignments_df[["Employee Key", "Pay Rate Clean", assign_no_col]],
        on=["Employee Key", "Pay Rate Clean"],
        how="left",
    )

    missing_assignments = merged[merged[assign_no_col].isna()]
    if not missing_assignments.empty:
        details = []
        for name, rate in zip(
            missing_assignments["Employee Name"],
            missing_assignments["Pay Rate Clean"],
        ):
            if pd.notna(rate):
                details.append(f"{name} @ {rate:.2f}")
            else:
                details.append(str(name))
        raise ValueError("Missing assignment numbers for: " + ", ".join(details))

    merged = merged.rename(columns={assign_no_col: "Assignment #"})
    merged = merged.sort_values(["Assignment #", "Employee Name"]).reset_index(drop=True)

    today = datetime.now().date()
    most_recent_sunday = today - timedelta(days=(today.weekday() + 1) % 7)
    work_date_str = most_recent_sunday.strftime("%m/%d/%Y")
    id_prefix = most_recent_sunday.strftime("%Y%m%d")

    merged["Pay Rate"] = merged["Pay Rate Clean"].round(2)
    merged["Hours"] = merged["Hours"].round(2)
    merged["Work Date"] = work_date_str
    merged["Weekending Date"] = work_date_str
    merged["Unique Line ID"] = [f"{id_prefix}{i:04d}" for i in range(1, len(merged) + 1)]

    return (
        merged[
            [
                "Assignment #",
                "Employee Name",
                "Pay Rate",
                "Work Date",
                "Weekending Date",
                "Hours",
                "Unique Line ID",
            ]
        ],
        most_recent_sunday,
    )


@router.get("")
async def page(request: Request):
    return templates.TemplateResponse("apps/ucla_hours_tool.html", {"request": request})


@router.post("/upload")
async def upload(
    request: Request,
    employee_list: UploadFile = File(...),
    payroll: UploadFile = File(...),
):
    try:
        payroll_df = await _read_excel(payroll)
        assignments_df = await _read_excel(employee_list)
        output_df, sunday = _prepare_ucla_hours(payroll_df, assignments_df)
    except ValueError as exc:
        return templates.TemplateResponse(
            "apps/ucla_hours_tool.html",
            {"request": request, "error": str(exc)},
        )
    except Exception:  # pragma: no cover - defensive
        return templates.TemplateResponse(
            "apps/ucla_hours_tool.html",
            {
                "request": request,
                "error": "An unexpected error occurred while processing the workbooks.",
            },
            status_code=500,
        )

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        output_df.to_excel(writer, index=False, sheet_name="UCLA Hours")
    buffer.seek(0)

    filename = f"ucla_hours_{sunday.strftime('%Y%m%d')}.xlsx"
    headers = {"Content-Disposition": f"attachment; filename=\"{filename}\""}
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )
