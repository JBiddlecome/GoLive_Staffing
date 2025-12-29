import json
import uuid
from pathlib import Path
from typing import Dict, List, Optional

DATA_FILE = Path("data/contacts.json")

DEPARTMENT_HEADERS: Dict[str, str] = {
    "Staffing": "staffingteam@culinarystaffing.com",
    "HR & Office Management": "hr@culinarystaffing.com",
    "Recruiting": "gethired@culinarystaffing.com",
    "Billing & Payroll": "timesheets@culinarystaffing.com or accounting@culinarystaffing.com",
    "Sales": "info@culinarystaffing.com",
    "Tech Support": "",
    "Owner & Operators": "",
}


def _ensure_data_file() -> None:
    if not DATA_FILE.exists():
        DATA_FILE.write_text(json.dumps({"departments": []}, indent=2))


def load_contacts() -> Dict[str, List[dict]]:
    _ensure_data_file()
    with DATA_FILE.open() as f:
        return json.load(f)


def save_contacts(data: Dict[str, List[dict]]) -> None:
    DATA_FILE.write_text(json.dumps(data, indent=2))


def add_contact(
    *,
    department: str,
    name: str,
    title: str,
    email: str,
    phone: str,
    extension: str,
    staff_type: str,
) -> None:
    data = load_contacts()
    departments: List[dict] = data.get("departments", [])

    department_entry: Optional[dict] = next(
        (dept for dept in departments if dept.get("name") == department), None
    )

    if department_entry is None:
        department_entry = {
            "name": department,
            "group_email": DEPARTMENT_HEADERS.get(department, ""),
            "contacts": [],
        }
        departments.append(department_entry)

    contact = {
        "id": str(uuid.uuid4()),
        "name": name,
        "title": title,
        "email": email,
        "phone": phone,
        "extension": extension,
        "staff_type": staff_type,
    }

    department_entry.setdefault("contacts", []).append(contact)
    save_contacts({"departments": departments})


def remove_contact(contact_id: str) -> None:
    data = load_contacts()
    departments: List[dict] = data.get("departments", [])
    for department in departments:
        contacts: List[dict] = department.get("contacts", [])
        department["contacts"] = [c for c in contacts if c.get("id") != contact_id]
    save_contacts({"departments": departments})
