from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

import pandas as pd
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from apps.sales_payroll_analyzer.app import PayrollDataError, load_payroll_data

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("", response_class=HTMLResponse)
async def payroll_rate_analyzer(
    request: Request,
    start_date: date | None = None,
    end_date: date | None = None,
    position: str | None = None,
    industry: str | None = None,
    county: List[str] | None = Query(None),
) -> HTMLResponse:
    try:
        dataframe = load_payroll_data()
    except PayrollDataError as exc:
        return templates.TemplateResponse(
            "apps/sales_payroll_analyzer.html",
            {"request": request, "error": str(exc)},
            status_code=400,
        )

    if dataframe.empty:
        return templates.TemplateResponse(
            "apps/sales_payroll_analyzer.html",
            {"request": request, "error": "The payroll workbook contains no rows."},
            status_code=400,
        )

    min_date = dataframe["Date"].min()
    max_date = dataframe["Date"].max()
    resolved_start = start_date or min_date
    resolved_end = end_date or max_date

    if resolved_start and resolved_end and resolved_start > resolved_end:
        return templates.TemplateResponse(
            "apps/sales_payroll_analyzer.html",
            {
                "request": request,
                "error": "Start date must be on or before end date.",
            },
            status_code=400,
        )

    positions = _sorted_unique(dataframe, "Position")
    industries = _sorted_unique(dataframe, "Industry")
    counties = _sorted_unique(dataframe, "County of Venue")

    filtered = _apply_filters(
        dataframe,
        resolved_start,
        resolved_end,
        position,
        industry,
        county or [],
    )

    metrics: Dict[str, Any] = {}
    rate_counts: List[Dict[str, Any]] = []
    preview_rows: List[Dict[str, Any]] = []

    if not filtered.empty:
        bill_series = filtered["Bill Rate"]
        pay_series = filtered["Pay Rate"]
        metrics = {
            "total_shifts": len(filtered),
            "bill_min": float(bill_series.min()),
            "bill_avg": float(bill_series.mean()),
            "bill_max": float(bill_series.max()),
            "pay_min": float(pay_series.min()),
            "pay_avg": float(pay_series.mean()),
            "pay_max": float(pay_series.max()),
        }

        rate_counts_df = (
            filtered.groupby("Bill Rate", dropna=False)
            .size()
            .reset_index(name="Shift Count")
            .sort_values("Bill Rate", ascending=True)
        )
        rate_counts = rate_counts_df.to_dict(orient="records")
        preview_rows = filtered.head(200).to_dict(orient="records")

    context = {
        "request": request,
        "min_date": min_date,
        "max_date": max_date,
        "selected_start": resolved_start,
        "selected_end": resolved_end,
        "positions": positions,
        "industries": industries,
        "counties": counties,
        "selected_position": position or "(All)",
        "selected_industry": industry or "(All)",
        "selected_counties": county or [],
        "filtered_rows": len(filtered),
        "metrics": metrics,
        "rate_counts": rate_counts,
        "preview_rows": preview_rows,
    }
    return templates.TemplateResponse("apps/sales_payroll_analyzer.html", context)


def _sorted_unique(dataframe: pd.DataFrame, column: str) -> List[str]:
    values = dataframe[column].dropna().astype(str).str.strip()
    return sorted({value for value in values if value})


def _apply_filters(
    dataframe: pd.DataFrame,
    start: date,
    end: date,
    position: str | None,
    industry: str | None,
    counties: List[str],
) -> pd.DataFrame:
    filtered = dataframe[(dataframe["Date"] >= start) & (dataframe["Date"] <= end)].copy()

    if position and position != "(All)":
        filtered = filtered[filtered["Position"] == position]

    if industry and industry != "(All)":
        filtered = filtered[filtered["Industry"] == industry]

    if counties:
        filtered = filtered[filtered["County of Venue"].isin(counties)]

    return filtered
