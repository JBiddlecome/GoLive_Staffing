from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
PAYROLL_PATH = BASE_DIR / "payroll.xlsx"

REQUIRED_COLS = ["Date", "Client", "Staffing Manager"]


class PayrollDataError(ValueError):
    """Raised when the payroll workbook cannot be loaded."""


def load_payroll_data(path: Path | None = None) -> pd.DataFrame:
    source_path = path or PAYROLL_PATH
    if not source_path.exists():
        raise PayrollDataError(
            "Payroll workbook not found. Commit payroll.xlsx to the repository root."
        )

    try:
        df = pd.read_excel(source_path, engine="openpyxl")
    except Exception as exc:  # pragma: no cover - pandas specific error
        raise PayrollDataError(f"Unable to read payroll workbook: {exc}") from exc

    missing = [col for col in REQUIRED_COLS if col not in df.columns]
    if missing:
        raise PayrollDataError(f"Missing required columns: {', '.join(missing)}")

    df = df[REQUIRED_COLS].copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    df["Client"] = df["Client"].astype(str).str.strip()
    df["Staffing Manager"] = (
        df["Staffing Manager"].fillna("").astype(str).str.strip()
    )
    df.loc[df["Staffing Manager"] == "", "Staffing Manager"] = "Unassigned"

    df = df.dropna(subset=["Date", "Client"])
    df = df[df["Client"] != ""]

    if df.empty:
        raise PayrollDataError(
            "Payroll workbook has no rows with the required data after cleaning."
        )

    return df


def calculate_drop_offs(
    dataframe: pd.DataFrame,
    lookback_months: int = 2,
    recent_days: int = 7,
) -> tuple[list[dict[str, object]], date, date, date]:
    max_date = dataframe["Date"].max()
    if max_date is None:
        raise PayrollDataError("Payroll workbook has no valid dates.")

    lookback_start = (
        pd.Timestamp(max_date) - pd.DateOffset(months=lookback_months)
    ).date()
    recent_cutoff = max_date - timedelta(days=recent_days - 1)

    filtered = dataframe[
        (dataframe["Date"] >= lookback_start) & (dataframe["Date"] <= max_date)
    ].copy()

    filtered = filtered[~filtered["Client"].str.contains("private", case=False, na=False)]

    recent_clients = filtered.loc[
        filtered["Date"] >= recent_cutoff, "Client"
    ].unique()
    if len(recent_clients) > 0:
        filtered = filtered[~filtered["Client"].isin(recent_clients)]

    if filtered.empty:
        return [], lookback_start, recent_cutoff, max_date

    latest_rows = (
        filtered.sort_values(["Client", "Date"])
        .drop_duplicates(subset=["Client"], keep="last")
        .rename(columns={"Date": "Last Shift"})
    )
    summary = latest_rows[["Client", "Last Shift", "Staffing Manager"]].copy()
    summary["Days Since Last Shift"] = (
        pd.to_datetime(max_date) - pd.to_datetime(summary["Last Shift"])
    ).dt.days

    summary = summary.sort_values(
        by=["Days Since Last Shift", "Client"], ascending=[False, True]
    )

    records = summary.to_dict(orient="records")
    return records, lookback_start, recent_cutoff, max_date
