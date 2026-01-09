from __future__ import annotations

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
PAYROLL_PATH = BASE_DIR / "payroll.xlsx"

REQUIRED_COLS = [
    "Date",
    "Position",
    "Pay Rate",
    "Bill Rate",
    "County of Venue",
    "Industry",
]


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
    for col in ["Pay Rate", "Bill Rate"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["Position", "County of Venue", "Industry"]:
        df[col] = df[col].astype(str).str.strip()

    df = df.dropna(
        subset=[
            "Date",
            "Bill Rate",
            "Pay Rate",
            "Position",
            "Industry",
            "County of Venue",
        ]
    )

    return df
