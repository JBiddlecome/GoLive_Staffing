from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import apps.sales_staffing_metrics.views as views


def _build_sample_workbook(path: Path) -> None:
    weeks = [
        datetime(2025, 1, 5),
        datetime(2025, 1, 12),
        datetime(2025, 1, 19),
    ]
    revenue_df = pd.DataFrame(
        {
            "Week Ending": weeks,
            "2025 Revenue": [1000.0, 2000.0, None],
            "2025 Revenue Goal": [1500.0, 2200.0, 2500.0],
            "New Sales Revenue": [250.0, 400.0, None],
            "New Sales % of Revenue": [0.25, 0.2, None],
        }
    )
    shift_df = pd.DataFrame(
        {
            "Week Ending (Shift Count)": weeks,
            "2024 (Shift Count)": [10, 20, 30],
            "2025 (Shift Count)": [12, 22, None],
            "2024 (Fill Rate)": [0.9, 0.95, 0.96],
            "2025 (Fill Rate)": [0.92, 0.97, None],
        }
    )
    with pd.ExcelWriter(path) as writer:
        revenue_df.to_excel(writer, sheet_name="Revenue", index=False)
        shift_df.to_excel(writer, sheet_name="Shift Count", index=False)


def test_chart_data_selects_latest_week_with_detail(tmp_path, monkeypatch):
    workbook_path = tmp_path / "Sales and Staffing Charts.xlsx"
    _build_sample_workbook(workbook_path)

    payroll_df = pd.DataFrame(
        {
            "Date": [
                datetime(2025, 1, 8),
                datetime(2025, 1, 9),
            ],
            "Client": ["Client A", "Client B"],
            "Total Bill": [5000.0, 3500.0],
            "Bill Rate": [45.0, 38.0],
            "Client Won Date": [
                datetime(2024, 12, 15),
                datetime(2024, 12, 20),
            ],
        }
    )

    monkeypatch.setattr(
        views,
        "_load_payroll_csv",
        lambda path=None: payroll_df,
    )
    monkeypatch.setattr(
        views,
        "_load_dashboard_data",
        lambda path=views.DASHBOARD_DATA_PATH: {},
    )

    chart_data = views._load_chart_data(path=workbook_path)

    assert chart_data["weeks"], "Expected at least one revenue week"

    detail_week = None
    for week in reversed(chart_data["weeks"]):
        detail = chart_data["weeklyDetails"].get(week["weekEnding"])
        if detail and any(detail.get(key) for key in ("topClients", "newClients", "industries")):
            detail_week = week
            break

    assert detail_week is not None, "Should find a week containing detailed metrics"
    assert chart_data["topClientsWeekEnding"] == detail_week["weekEnding"]
    assert chart_data["topClientsWeekLabel"] == detail_week["label"]
    detail = chart_data["weeklyDetails"][detail_week["weekEnding"]]
    assert chart_data["topClients"] == detail.get("topClients", [])
    assert chart_data["topClients"], "Top clients should default to the detailed week"


def test_resolve_workbook_path_accepts_ampersand(tmp_path):
    workbook_path = tmp_path / "Sales & Staffing Charts.xlsx"
    _build_sample_workbook(workbook_path)

    resolved = views._resolve_workbook_path(base_dir=tmp_path)

    assert resolved == workbook_path
