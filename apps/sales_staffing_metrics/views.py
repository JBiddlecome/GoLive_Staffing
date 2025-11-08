from __future__ import annotations

import io
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter, Form, Request, UploadFile, File
from fastapi.concurrency import run_in_threadpool
from fastapi.templating import Jinja2Templates
from openpyxl import load_workbook

templates = Jinja2Templates(directory="templates")
router = APIRouter()

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
WORKBOOK_PATH = DATA_DIR / "Sales and Staffing Charts.xlsx"
METRICS_EXPORT_PATH = DATA_DIR / "sales_staffing_metrics.csv"
DASHBOARD_DATA_PATH = DATA_DIR / "sales_staffing_dashboard.json"


def _normalize_week_ending(value: datetime) -> datetime:
    """Return the naive midnight datetime used for exports."""

    if value.tzinfo is not None:
        value = value.astimezone(tz=None)
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


def _load_metrics_export(path: Path = METRICS_EXPORT_PATH) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "week_ending",
                "total_revenue",
                "new_sales_revenue",
                "new_sales_pct",
                "shift_count",
                "open_shifts",
                "fill_rate",
            ]
        )

    df = pd.read_csv(path)
    if "week_ending" in df.columns:
        df["week_ending"] = pd.to_datetime(df["week_ending"], errors="coerce")
    return df


def _write_metrics_export(metrics: Dict[str, Any], path: Path = METRICS_EXPORT_PATH) -> None:
    metrics = metrics.copy()
    metrics["week_ending"] = _normalize_week_ending(metrics["week_ending"])

    path.parent.mkdir(parents=True, exist_ok=True)
    export_df = _load_metrics_export(path)
    if not export_df.empty:
        export_df = export_df[export_df["week_ending"] != metrics["week_ending"]]

    export_df = pd.concat([export_df, pd.DataFrame([metrics])], ignore_index=True)
    export_df = export_df.sort_values("week_ending")
    export_df.to_csv(path, index=False)


def _load_dashboard_data(path: Path = DASHBOARD_DATA_PATH) -> Dict[str, Any]:
    if not path.exists():
        return {}

    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:  # pragma: no cover - defensive
        return {}


def _write_dashboard_data(data: Dict[str, Any], path: Path = DASHBOARD_DATA_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    safe_data = data.copy()
    if "weekEnding" in safe_data and hasattr(safe_data["weekEnding"], "isoformat"):
        safe_data["weekEnding"] = safe_data["weekEnding"].isoformat()
    if "weekLabel" in safe_data and hasattr(safe_data["weekLabel"], "strftime"):
        safe_data["weekLabel"] = safe_data["weekLabel"].strftime("%B %d, %Y")
    with path.open("w", encoding="utf-8") as fh:
        json.dump(safe_data, fh, indent=2)


def _empty_chart_payload() -> Dict[str, Any]:
    dashboard_data = _load_dashboard_data()
    return {
        "weeks": [],
        "topClients": dashboard_data.get("topClients", []),
        "topClientsWeekEnding": dashboard_data.get("weekEnding"),
        "topClientsWeekLabel": dashboard_data.get("weekLabel"),
    }


def _to_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _next_sunday_on_or_after(value: datetime) -> datetime:
    days_ahead = (6 - value.weekday()) % 7
    return value + timedelta(days=days_ahead)


def _normalize_money(value: Any) -> float:
    if pd.isna(value):
        return 0.0
    if isinstance(value, str):
        value = value.replace(",", "").replace("$", "").strip()
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _ensure_headers(ws) -> Dict[str, int]:
    headers: Dict[str, int] = {}
    for idx, cell in enumerate(ws[1], start=1):
        if cell.value is None:
            continue
        headers[str(cell.value).strip()] = idx
    return headers


def _find_or_create_week_row(
    ws, headers: Dict[str, int], week_ending: datetime
) -> Tuple[int, Dict[str, int]]:
    week_column_name = None
    candidate_week_columns = (
        "Week Ending",
        "Week Ending (Shift Count)",
        "Week Ending (Fill Rate)",
    )

    for candidate in candidate_week_columns:
        if candidate in headers:
            week_column_name = candidate
            break

    if week_column_name is None and "2025 (Shift Count)" in headers:
        insert_position = headers["2025 (Shift Count)"]
        ws.insert_cols(insert_position)
        ws.cell(row=1, column=insert_position).value = "Week Ending"
        headers = _ensure_headers(ws)
        week_column_name = "Week Ending"

    if week_column_name is None:
        raise ValueError(
            f'Required column "Week Ending" not found in sheet "{ws.title}". '
            "Expected one of: Week Ending, Week Ending (Shift Count), Week Ending (Fill Rate), "
            "or 2025 (Shift Count)."
        )

    column = headers[week_column_name]
    for row in range(2, ws.max_row + 1):
        cell_value = ws.cell(row=row, column=column).value
        if cell_value is None:
            continue
        try:
            cell_date = pd.to_datetime(cell_value).date()
        except Exception:  # pragma: no cover - defensive
            continue
        if cell_date == week_ending.date():
            return row, headers

    row = ws.max_row + 1 if ws.max_row >= 1 else 2
    ws.cell(row=row, column=column).value = week_ending
    return row, headers


def _set_cell(ws, row: int, headers: Dict[str, int], column: str, value: Any) -> None:
    if column not in headers:
        raise ValueError(f'Required column "{column}" not found in sheet "{ws.title}"')
    ws.cell(row=row, column=headers[column]).value = value


def _clean_int(value: Any) -> int | None:
    if pd.isna(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _clean_float(value: Any) -> float | None:
    if pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _load_chart_data(path: Path = WORKBOOK_PATH) -> Dict[str, Any]:
    if not path.exists():
        return _empty_chart_payload()

    try:
        revenue_df = pd.read_excel(path, sheet_name="Revenue")
        shift_df = pd.read_excel(path, sheet_name="Shift Count")
    except Exception:  # pragma: no cover - defensive
        return _empty_chart_payload()

    if "Week Ending" not in revenue_df.columns:
        return _empty_chart_payload()

    revenue_df = revenue_df.copy()
    revenue_df["Week Ending"] = pd.to_datetime(revenue_df["Week Ending"], errors="coerce")
    revenue_df = revenue_df.dropna(subset=["Week Ending"]).sort_values("Week Ending")

    if revenue_df.empty:
        return _empty_chart_payload()

    shift_df = shift_df.copy()
    week_columns: List[str] = [
        col for col in (
            "Week Ending (Shift Count)",
            "Week Ending",
        )
        if col in shift_df.columns
    ]
    if not week_columns:
        return {"weeks": []}

    week_column = week_columns[0]
    shift_df[week_column] = pd.to_datetime(shift_df[week_column], errors="coerce")
    shift_df = shift_df.dropna(subset=[week_column])

    shift_records: Dict[Any, Dict[str, Any]] = {}
    for _, row in shift_df.iterrows():
        week = pd.to_datetime(row[week_column]).date()
        shift_records[week] = {
            "shiftCount2024": _clean_int(row.get("2024 (Shift Count)")),
            "shiftCount2025": _clean_int(row.get("2025 (Shift Count)")),
            "fillRate2024": _clean_float(row.get("2024 (Fill Rate)")),
            "fillRate2025": _clean_float(row.get("2025 (Fill Rate)")),
        }

    weeks: List[Dict[str, Any]] = []
    for _, row in revenue_df.iterrows():
        week_ts = pd.to_datetime(row["Week Ending"])
        week_date = week_ts.date()
        record = shift_records.get(
            week_date,
            {
                "shiftCount2024": None,
                "shiftCount2025": None,
                "fillRate2024": None,
                "fillRate2025": None,
            },
        )
        revenue_2025 = _clean_float(row.get("2025 Revenue"))
        revenue_goal_2025 = _clean_float(row.get("2025 Revenue Goal"))
        new_sales_revenue = _clean_float(row.get("New Sales Revenue"))
        new_sales_pct = _clean_float(row.get("New Sales % of Revenue"))
        weeks.append(
            {
                "weekEnding": week_ts.strftime("%Y-%m-%d"),
                "label": week_ts.strftime("%B %d, %Y"),
                **record,
                "revenue2025": revenue_2025,
                "revenueGoal2025": revenue_goal_2025,
                "newSalesRevenue": new_sales_revenue,
                "newSalesPct": new_sales_pct,
            }
        )

    dashboard_data = _load_dashboard_data()

    return {
        "weeks": weeks,
        "topClients": dashboard_data.get("topClients", []),
        "topClientsWeekEnding": dashboard_data.get("weekEnding"),
        "topClientsWeekLabel": dashboard_data.get("weekLabel"),
    }


def _calculate_top_clients(payroll_df: pd.DataFrame) -> List[Dict[str, Any]]:
    if "Client" not in payroll_df.columns or "Total Bill" not in payroll_df.columns:
        return []

    df = payroll_df.copy()
    df["Client"] = df["Client"].fillna("Unknown Client").astype(str)
    df["Total Bill"] = df["Total Bill"].apply(_normalize_money)

    if "Bill Rate" in df.columns:
        df["Bill Rate"] = df["Bill Rate"].apply(_normalize_money)
    else:
        df["Bill Rate"] = pd.NA

    grouped = (
        df.groupby("Client", as_index=False)
        .agg(total_bill=("Total Bill", "sum"), average_bill_rate=("Bill Rate", "mean"))
        .sort_values("total_bill", ascending=False)
        .head(5)
    )

    results: List[Dict[str, Any]] = []
    for _, row in grouped.iterrows():
        total_bill_value = float(row["total_bill"]) if pd.notna(row["total_bill"]) else 0.0
        avg_bill_rate_value = (
            float(row["average_bill_rate"]) if pd.notna(row["average_bill_rate"]) else None
        )
        results.append(
            {
                "client": row["Client"],
                "totalBill": total_bill_value,
                "averageBillRate": avg_bill_rate_value,
            }
        )

    return results


def _update_workbook(payroll_df: pd.DataFrame, open_shifts: int) -> Dict[str, Any]:
    if not WORKBOOK_PATH.exists():
        raise FileNotFoundError(
            "Sales and Staffing workbook not found. Expected to find it at " f"{WORKBOOK_PATH}."
        )

    payroll_df = payroll_df.copy()
    if "Total Bill" not in payroll_df.columns:
        raise ValueError('"Total Bill" column not found in the payroll workbook.')
    if "Date" not in payroll_df.columns:
        raise ValueError('"Date" column not found in the payroll workbook.')

    payroll_df["Date"] = _to_date_series(payroll_df["Date"])
    if payroll_df["Date"].dropna().empty:
        raise ValueError("No valid dates found in the payroll workbook.")

    if "Client Won Date" in payroll_df.columns:
        payroll_df["Client Won Date"] = _to_date_series(payroll_df["Client Won Date"])
    else:
        payroll_df["Client Won Date"] = pd.NaT

    max_date = payroll_df["Date"].max()
    week_ending = _next_sunday_on_or_after(max_date)

    payroll_df["Total Bill"] = payroll_df["Total Bill"].apply(_normalize_money)
    total_revenue = float(payroll_df["Total Bill"].sum())

    six_months_prior = week_ending - relativedelta(months=6)
    mask_new_sales = (
        payroll_df["Client Won Date"].notna()
        & (payroll_df["Client Won Date"] >= six_months_prior)
        & (payroll_df["Client Won Date"] <= week_ending)
    )
    new_sales_revenue = float(payroll_df.loc[mask_new_sales, "Total Bill"].sum())
    new_sales_pct = new_sales_revenue / total_revenue if total_revenue > 0 else 0.0

    shift_count = int((payroll_df["Total Bill"] > 0).sum())
    open_shifts = max(0, int(open_shifts))
    total_shifts = shift_count + open_shifts
    fill_rate = shift_count / total_shifts if total_shifts > 0 else 0.0

    workbook = load_workbook(filename=WORKBOOK_PATH)

    if "Revenue" not in workbook.sheetnames:
        raise ValueError('Missing "Revenue" sheet in the Sales and Staffing workbook.')
    revenue_sheet = workbook["Revenue"]
    revenue_headers = _ensure_headers(revenue_sheet)
    revenue_row, revenue_headers = _find_or_create_week_row(
        revenue_sheet, revenue_headers, week_ending
    )
    _set_cell(revenue_sheet, revenue_row, revenue_headers, "2025 Revenue", total_revenue)
    _set_cell(revenue_sheet, revenue_row, revenue_headers, "New Sales Revenue", new_sales_revenue)
    _set_cell(revenue_sheet, revenue_row, revenue_headers, "New Sales % of Revenue", new_sales_pct)

    if "Shift Count" not in workbook.sheetnames:
        raise ValueError('Missing "Shift Count" sheet in the Sales and Staffing workbook.')
    shift_sheet = workbook["Shift Count"]
    shift_headers = _ensure_headers(shift_sheet)
    shift_row, shift_headers = _find_or_create_week_row(
        shift_sheet, shift_headers, week_ending
    )
    _set_cell(shift_sheet, shift_row, shift_headers, "2025 (Shift Count)", shift_count)
    _set_cell(shift_sheet, shift_row, shift_headers, "2025 (Fill Rate)", fill_rate)

    workbook.save(WORKBOOK_PATH)

    metrics = {
        "week_ending": week_ending,
        "total_revenue": total_revenue,
        "new_sales_revenue": new_sales_revenue,
        "new_sales_pct": new_sales_pct,
        "shift_count": shift_count,
        "open_shifts": open_shifts,
        "fill_rate": fill_rate,
    }

    _write_metrics_export(metrics)

    top_clients = _calculate_top_clients(payroll_df)
    dashboard_payload = {
        "weekEnding": week_ending,
        "weekLabel": week_ending,
        "topClients": top_clients,
    }
    _write_dashboard_data(dashboard_payload)

    return metrics


def _read_payroll(upload: UploadFile) -> pd.DataFrame:
    try:
        upload.file.seek(0)
        payload = upload.file.read()
        if not payload:
            raise ValueError("The uploaded payroll workbook is empty.")
        return pd.read_excel(io.BytesIO(payload), engine="openpyxl")
    except ValueError:
        raise
    except Exception as exc:  # pragma: no cover - defensive
        raise ValueError(f"Unable to read Excel file '{upload.filename}'.") from exc


def _build_page_context(**extra: Any) -> Dict[str, Any]:
    base_context = {
        "workbook_path": WORKBOOK_PATH,
        "metrics_export_path": METRICS_EXPORT_PATH,
        "chart_data": _load_chart_data(),
    }
    base_context.update(extra)
    return base_context


@router.get("")
async def page(request: Request):
    context = _build_page_context(request=request)
    return templates.TemplateResponse("apps/sales_staffing_metrics.html", context)


@router.post("/update")
async def update(
    request: Request,
    payroll: UploadFile = File(...),
    open_shifts: str = Form(""),
):
    context = _build_page_context(request=request)

    try:
        payroll_df = await run_in_threadpool(_read_payroll, payroll)
    except ValueError as exc:
        context.update({"error": str(exc)})
        return templates.TemplateResponse(
            "apps/sales_staffing_metrics.html",
            context,
            status_code=400,
        )
    finally:
        await payroll.close()

    try:
        open_shifts_value = int(open_shifts.replace(",", "").strip()) if open_shifts.strip() else 0
    except ValueError:
        context.update({"error": "Open shifts must be a whole number."})
        return templates.TemplateResponse(
            "apps/sales_staffing_metrics.html",
            context,
            status_code=400,
        )

    try:
        result = await run_in_threadpool(_update_workbook, payroll_df, open_shifts_value)
    except FileNotFoundError as exc:
        context.update({"error": str(exc)})
        return templates.TemplateResponse(
            "apps/sales_staffing_metrics.html",
            context,
            status_code=404,
        )
    except ValueError as exc:
        context.update({"error": str(exc)})
        return templates.TemplateResponse(
            "apps/sales_staffing_metrics.html",
            context,
            status_code=400,
        )

    context.update({"result": result, "chart_data": _load_chart_data()})
    return templates.TemplateResponse("apps/sales_staffing_metrics.html", context)
