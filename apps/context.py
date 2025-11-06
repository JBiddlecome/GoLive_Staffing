from __future__ import annotations

from typing import Any, Dict


def default_text_blast_context() -> Dict[str, Any]:
    """Baseline context dictionary for the text blast template block."""

    return {
        "text_options": None,
        "text_selected": {
            "shift_position_title": "",
            "employee_status": "",
            "miles_from_location": 50,
        },
        "text_file_token": None,
        "text_uploaded_filename": None,
        "text_error": None,
    }


def default_employee_filter_context() -> Dict[str, Any]:
    """Baseline context dictionary for the employee list filter block."""

    return {
        "employee_options": None,
        "employee_selected": {
            "statuses": [],
            "cities": [],
            "state": "All",
            "start_date_start": "",
            "start_date_end": "",
            "positions": [],
            "counties": [],
        },
        "employee_file_token": None,
        "employee_uploaded_filename": None,
        "employee_error": None,
    }
