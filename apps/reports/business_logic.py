"""
GoLive Staffing — AI Analytics Business Logic Library
======================================================
Reads golive_database_reference.md at startup and injects relevant
sections into the AI prompt based on which tables the planner selected.

Single source of truth: golive_database_reference.md in the repo root.
"""

from pathlib import Path
import re

# ─── Load and parse the reference document ───────────────────────────────────

_REF_PATH = Path(__file__).resolve().parents[2] / "golive_database_reference.md"

def _load_sections() -> dict[str, str]:
    """Parse the markdown into sections keyed by '## N. Title' headers."""
    if not _REF_PATH.exists():
        print(f"WARNING: {_REF_PATH} not found — business logic will be empty")
        return {}

    text = _REF_PATH.read_text(encoding="utf-8")
    sections: dict[str, str] = {}
    parts = re.split(r'^(## \d+\.\s+.+)$', text, flags=re.MULTILINE)

    for i in range(1, len(parts) - 1, 2):
        header = parts[i].strip()
        body = parts[i + 1].strip()
        m = re.match(r'## \d+\.\s+(.+)', header)
        if m:
            key = m.group(1).lower().replace(" ", "_").replace("&", "and")
            key = re.sub(r'[^a-z0-9_]', '', key)
            sections[key] = f"{header}\n\n{body}"

    return sections


_SECTIONS = _load_sections()

# ─── Trigger map: which tables/keywords activate which sections ──────────────

_TABLE_TRIGGERS: dict[str, list[str]] = {
    "domain_overview": [
        "timesheet", "shift_employee", "shift_position", "shift",
        "event", "client", "venue", "employee",
    ],
    "entity_hierarchy_and_join_paths": [
        "timesheet", "shift_employee", "shift_position", "shift",
        "event", "client", "venue", "employee", "certification",
        "employee_certification", "venue_certification", "dnr", "exclusive",
    ],
    "enum__status_code_reference": [
        "timesheet", "shift_employee", "employee", "client",
        "shift_position", "event",
    ],
    "key_table_field_reference": [
        "timesheet", "shift_employee", "shift_position", "shift",
        "event", "client", "employee", "venue",
    ],
    "scheduled_vs_actual_hours": [
        "timesheet", "shift", "shift_employee",
    ],
    "meal_break_policy_statebased": [
        "timesheet", "shift", "event",
    ],
    "minimum_pay_and_minimum_billing_rules": [
        "timesheet", "shift", "shift_employee", "event",
    ],
    "billing_and_pay_calculation_formulas": [
        "timesheet", "shift_employee", "shift_position", "event",
        "msp", "wc_code", "additional_shift_pay",
    ],
    "cancellation_deadline_logic": [
        "timesheet", "shift_employee", "client", "shift",
    ],
    "shift_lifecycle_complete_state_machine": [
        "timesheet", "shift_employee", "shift", "event",
    ],
    "common_sql_query_patterns": [
        "timesheet", "shift_employee", "shift_position", "shift",
        "event", "employee", "client", "venue",
    ],
    "data_integrity_notes": [
        "timesheet", "shift_employee", "shift_position", "shift",
        "event", "client",
    ],
}

_KEYWORD_TRIGGERS: dict[str, list[str]] = {
    "minimum_pay_and_minimum_billing_rules": [
        "min pay", "minimum pay", "non-worked", "nonworked", "non worked",
        "senthome", "sent home", "cancelled", "canceled",
    ],
    "billing_and_pay_calculation_formulas": [
        "bill", "billing", "revenue", "pay", "payroll", "profit",
        "gross pay", "total pay", "total bill", "service charge",
        "overtime", "double time", "doubletime",
    ],
    "meal_break_policy_statebased": [
        "meal break", "break policy", "break deduction",
    ],
    "cancellation_deadline_logic": [
        "cancel", "cancellation", "deadline",
    ],
    "scheduled_vs_actual_hours": [
        "hours", "scheduled hours", "actual hours", "worked hours",
        "clock in", "clock out",
    ],
}


def get_business_logic_prompt(tables_used: list[str], question: str = "") -> str:
    """Returns relevant business logic sections for the given tables and question."""
    if not _SECTIONS:
        return ""

    tables_set = set(t.lower() for t in tables_used)
    question_lower = question.lower()
    needed_keys: set[str] = set()

    for section_key, trigger_tables in _TABLE_TRIGGERS.items():
        if tables_set & set(trigger_tables):
            needed_keys.add(section_key)

    for section_key, keywords in _KEYWORD_TRIGGERS.items():
        if any(kw in question_lower for kw in keywords):
            needed_keys.add(section_key)

    core_tables = {"timesheet", "shift_employee", "shift_position", "shift",
                   "event", "client", "venue", "employee"}
    if tables_set & core_tables:
        needed_keys.add("domain_overview")
        needed_keys.add("entity_hierarchy_and_join_paths")

    if not needed_keys:
        return ""

    parts = []
    for key in _SECTIONS:
        if key in needed_keys:
            parts.append(_SECTIONS[key])

    return (
        "\n\n## GoLive Business Logic Reference\n"
        "Use the following domain knowledge when writing SQL. "
        "This is authoritative — follow these rules exactly.\n\n"
        + "\n\n---\n\n".join(parts)
    )


def get_planner_context() -> str:
    """Returns a compact domain summary for the Step A planner prompt."""
    parts = []
    for key in ("domain_overview", "entity_hierarchy_and_join_paths"):
        if key in _SECTIONS:
            parts.append(_SECTIONS[key])
    if not parts:
        return ""
    return "\n\n## Domain Context\n" + "\n\n".join(parts)
