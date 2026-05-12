from datetime import date

import pandas as pd

from apps.look_back_history.views import build_client_rows, subtract_calendar_year


def test_subtract_calendar_year_normal_date():
    assert subtract_calendar_year(date(2026, 5, 10)) == date(2025, 5, 10)


def test_subtract_calendar_year_leap_day_falls_back_to_feb_28():
    assert subtract_calendar_year(date(2024, 2, 29)) == date(2023, 2, 28)


def test_build_client_rows_splits_active_and_uses_order_line_counts():
    orders_df = pd.DataFrame(
        [
            {
                "client_id": 1,
                "client_name": "Inactive Client",
                "position_name": "Server",
                "order_lines": 2,
                "staff_requested": 10,
            },
            {
                "client_id": 2,
                "client_name": "Active Client",
                "position_name": "Bartender",
                "order_lines": 1,
                "staff_requested": 4,
            },
        ]
    )
    current_df = pd.DataFrame(
        [
            {
                "client_id": 2,
                "current_order_lines_last_30_days": 3,
                "current_staff_requested_last_30_days": 8,
                "last_current_shift_date": date(2026, 5, 12),
            }
        ]
    )

    rows = build_client_rows(orders_df, current_df, {1: 1000.0, 2: 2500.5})
    by_client = {row["client_id"]: row for row in rows}

    assert by_client[1]["status"] == "inactive"
    assert by_client[1]["prior_year_order_lines"] == 2
    assert by_client[1]["prior_year_staff_requested"] == 10
    assert by_client[1]["prior_year_revenue"] == 1000.0
    assert by_client[2]["status"] == "active"
    assert by_client[2]["current_order_lines_last_30_days"] == 3
