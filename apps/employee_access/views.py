from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from uuid import uuid4

from fastapi import APIRouter, Form, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="templates")

def _resolve_data_dir() -> Path:
    import os
    env_dir = os.getenv("DATA_DIR") or os.getenv("RENDER_DISK_PATH")
    if env_dir:
        return Path(env_dir)
    if Path("/var/data").exists():
        return Path("/var/data")
    if any(os.getenv(e) for e in ("RENDER", "RENDER_SERVICE_ID")):
        return Path("/var/data")
    return Path("data")

DATA_FILE = _resolve_data_dir() / "employee_access.json"
DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

DEPARTMENTS = [
    "Staffing",
    "Sales",
    "Payroll Billing & Accounting",
    "Recruiting",
    "Human Resources",
    "Admin",
    "Development",
]

INITIAL_EMPLOYEES = [
    {"name": "Trina McCue", "department": "Staffing"},
    {"name": "Monica Sanchez", "department": "Staffing"},
    {"name": "Dan Stone", "department": "Sales"},
    {"name": "Jason Cabral", "department": "Sales"},
    {"name": "Corrinne Bellinger", "department": "Payroll Billing & Accounting"},
    {"name": "Debra Vega", "department": "Payroll Billing & Accounting"},
    {"name": "Jack Wetzel", "department": "Payroll Billing & Accounting"},
    {"name": "Monish Kumar", "department": "Payroll Billing & Accounting"},
    {"name": "Suraj Singh", "department": "Payroll Billing & Accounting"},
    {"name": "Anmol Singh", "department": "Recruiting"},
    {"name": "Praful Kumar", "department": "Recruiting"},
    {"name": "Piyush Kumar", "department": "Recruiting"},
    {"name": "Rebecca Moran", "department": "Human Resources"},
    {"name": "Mercedes Banwart", "department": "Human Resources"},
    {"name": "Jake Biddlecome", "department": "Development"},
]

INITIAL_ACCESS_ITEMS = [
    ("Action Capital", "Payroll Billing & Accounting"),
    ("Adobe Acrobat", "All Departments"),
    ("Aflac", "Human Resources"),
    ("Agile1", "Payroll Billing & Accounting"),
    ("Amazon", "Human Resources"),
    ("Anaconda Navigator", "Payroll Billing & Accounting"),
    ("Android Play Store", "Development"),
    ("Apple", "Development"),
    ("Apple App Store", "Development"),
    ("ASI Benefits (Cobra)", "Human Resources"),
    ("AT&T", "Payroll Billing & Accounting"),
    ("AWS", "Development"),
    ("Banner", "Equipment"),
    ("Beeline", "Payroll Billing & Accounting"),
    ("Bill.com", "Payroll Billing & Accounting"),
    ("Biteable", "Marketing"),
    ("Bureau of Labor Statistics", "Development"),
    ("Cal Jobs", "Recruiting"),
    ("CalSavers", "Payroll Billing & Accounting"),
    ("Canva", "Marketing"),
    ("CDTFA (Environmental Fee)", "Payroll Billing & Accounting"),
    ("Chase Bank", "Payroll Billing & Accounting"),
    ("ChatGPT", "All Departments"),
    ("Child Support Portal", "Human Resources"),
    ("Clickboarding", "Recruiting"),
    ("Close", "Sales"),
    ("Cloudinary", "Development"),
    ("Concentra", "Human Resources"),
    ("CoPower", "Payroll Billing & Accounting"),
    ("Coupa", "Payroll Billing & Accounting"),
    ("Dash", "Human Resources"),
    ("DCSS CA Compliance Center", "Human Resources"),
    ("EDD", "Human Resources"),
    ("Everify", "Recruiting"),
    ("Expo", "Development"),
    ("Facebook", "Marketing"),
    ("FedEx", "Human Resources"),
    ("Fieldglass", "Payroll Billing & Accounting"),
    ("Files.com", "Development"),
    ("GitHub", "Development"),
    ("GoLive!", "All Departments"),
    ("HelloSign/DocuSign", "Sales"),
    ("Hootsuite", "Marketing"),
    ("Indeed", "Recruiting"),
    ("InSource | Payroll Dashboard", "Payroll Billing & Accounting"),
    ("Instagram", "Marketing"),
    ("Intuit", "Payroll Billing & Accounting"),
    ("Invoiced", "Payroll Billing & Accounting"),
    ("iStock", "Marketing"),
    ("JotForm", "Development"),
    ("Kaiser California", "Human Resources"),
    ("Kaiser Oregon", "Human Resources"),
    ("Laptop", "Equipment"),
    ("LastPass", "Payroll Billing & Accounting"),
    ("LinkedIn", "Recruiting"),
    ("Monitor", "Equipment"),
    ("Namecheap (Domain)", "Development"),
    ("Office 365", "All Departments"),
    ("Office Keys", "Equipment"),
    ("OnePoint", "Payroll Billing & Accounting"),
    ("Orange Tree", "Human Resources"),
    ("Outlook", "All Departments"),
    ("PandaDoc", "Sales"),
    ("Perks at Work", "Human Resources"),
    ("Planner (part of 365)", "All Departments"),
    ("Quickbooks Online", "Payroll Billing & Accounting"),
    ("RingCentral", "All Departments"),
    ("SAIF (WC Oregon)", "Human Resources"),
    ("Schwab", "Payroll Billing & Accounting"),
    ("Sendinblue", "Marketing"),
    ("SFTP Server/FileZilla", "Development"),
    ("SharePoint", "All Departments"),
    ("Simplify", "Payroll Billing & Accounting"),
    ("Stripe", "Payroll Billing & Accounting"),
    ("Survey Monkey", "Marketing"),
    ("Tik Tok", "Marketing"),
    ("Trello", "All Departments"),
    ("UCLA", "Payroll Billing & Accounting"),
    ("UPS", "Human Resources"),
    ("Verizon", "Payroll Billing & Accounting"),
    ("Vimeo", "Marketing"),
    ("Vistaprint", "Marketing"),
    ("AWS Workspaces", "Development"),
    ("X", "Marketing"),
    ("Yelp", "Marketing"),
    ("YouTube", "Marketing"),
    ("Zapier", "Development"),
]


@router.get("")
async def employee_access_page(request: Request):
    data = _load_data()
    _ensure_employee_access_maps(data)

    active = [employee for employee in data["employees"] if employee.get("status") == "active"]
    archived = [employee for employee in data["employees"] if employee.get("status") == "terminated"]

    grouped_employees: Dict[str, List[dict[str, Any]]] = {department: [] for department in DEPARTMENTS}
    for employee in sorted(active, key=lambda item: item.get("name", "").lower()):
        grouped_employees.setdefault(employee["department"], []).append(employee)

    visible_access_items = [item for item in data["access_items"] if item.get("active", True)]
    access_departments = _available_access_departments(data)

    employees_view = {}
    for employee in active:
        employees_view[employee["id"]] = _build_employee_access_view(employee, visible_access_items)

    archived_view = {}
    for employee in archived:
        archived_view[employee["id"]] = _build_employee_access_view(employee, data["access_items"], include_inactive=True)

    return templates.TemplateResponse(
        "apps/employee_access.html",
        {
            "request": request,
            "departments": DEPARTMENTS,
            "grouped_employees": grouped_employees,
            "archived_employees": sorted(
                archived,
                key=lambda item: item.get("terminated_at") or item.get("updated_at") or "",
                reverse=True,
            ),
            "employees_view": employees_view,
            "archived_view": archived_view,
            "access_items": visible_access_items,
            "access_departments": access_departments,
            "notice": request.query_params.get("notice", ""),
            "error": request.query_params.get("error", ""),
        },
    )


@router.post("/employees")
async def add_employee(name: str = Form(...), title: str = Form(""), department: str = Form(...)):
    data = _load_data()
    normalized_department = _normalize_department(department)

    cleaned_name = re.sub(r"\s+", " ", name).strip()
    cleaned_title = re.sub(r"\s+", " ", title).strip()
    if not cleaned_name:
        return _redirect(error="Employee name is required.")

    data["employees"].append(
        {
            "id": uuid4().hex,
            "name": cleaned_name,
            "title": cleaned_title,
            "department": normalized_department,
            "status": "active",
            "access": {},
            "history": [],
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "terminated_at": "",
        }
    )
    _save_data(data)
    return _redirect(notice=f"Added {cleaned_name}.")


@router.post("/employees/{employee_id}/access")
async def update_employee_access(
    employee_id: str,
    access_id: str = Form(...),
    granted: str | None = Form(None),
    removed: str | None = Form(None),
):
    data = _load_data()
    employee = _find_employee(data, employee_id)
    if employee is None:
        return JSONResponse({"ok": False, "error": "Employee was not found."}, status_code=404)
    if employee.get("status") == "terminated":
        return JSONResponse({"ok": False, "error": "Cannot update access for terminated employees."}, status_code=400)

    _ensure_employee_access_maps(data)

    granted_flag = granted is not None
    removed_flag = removed is not None

    access_state = employee["access"].get(access_id, {"granted": False, "removed": False, "updated_at": ""})
    previous_granted = bool(access_state.get("granted", False))
    previous_removed = bool(access_state.get("removed", False))

    access_state["granted"] = granted_flag
    access_state["removed"] = removed_flag
    access_state["updated_at"] = _now_iso()
    employee["access"][access_id] = access_state
    employee["updated_at"] = _now_iso()

    if previous_granted != granted_flag:
        _append_history(
            employee,
            access_id,
            "granted" if granted_flag else "grant_unchecked",
            {"from": previous_granted, "to": granted_flag},
        )

    if previous_removed != removed_flag:
        _append_history(
            employee,
            access_id,
            "removed" if removed_flag else "removal_unchecked",
            {"from": previous_removed, "to": removed_flag},
        )

    _save_data(data)
    return JSONResponse({"ok": True})


@router.post("/employees/{employee_id}/profile")
async def update_employee_profile(
    employee_id: str,
    title: str = Form(""),
    department: str = Form(...),
):
    data = _load_data()
    employee = _find_employee(data, employee_id)
    if employee is None:
        return JSONResponse({"ok": False, "error": "Employee was not found."}, status_code=404)
    if employee.get("status") == "terminated":
        return JSONResponse({"ok": False, "error": "Cannot update terminated employees."}, status_code=400)

    previous_title = employee.get("title", "")
    previous_department = employee.get("department", "")

    cleaned_title = re.sub(r"\s+", " ", title).strip()
    normalized_department = _normalize_department(department)
    employee["title"] = cleaned_title
    employee["department"] = normalized_department
    employee["updated_at"] = _now_iso()

    if previous_title != cleaned_title:
        _append_history(
            employee,
            None,
            "title_updated",
            {"from": previous_title, "to": cleaned_title},
        )

    if previous_department != normalized_department:
        _append_history(
            employee,
            None,
            "department_updated",
            {"from": previous_department, "to": normalized_department},
        )

    _save_data(data)
    return JSONResponse({"ok": True})


@router.post("/employees/{employee_id}/terminate")
async def terminate_employee(employee_id: str):
    data = _load_data()
    employee = _find_employee(data, employee_id)
    if employee is None:
        return _redirect(error="Employee was not found.")

    if employee.get("status") != "terminated":
        employee["status"] = "terminated"
        employee["terminated_at"] = _now_iso()
        employee["updated_at"] = _now_iso()
        _append_history(employee, None, "terminated", {"employee": employee.get("name", "")})
        _save_data(data)

    return _redirect(notice=f"Archived {employee.get('name', 'employee')}.")


@router.post("/employees/{employee_id}/restore")
async def restore_employee(employee_id: str):
    data = _load_data()
    employee = _find_employee(data, employee_id)
    if employee is None:
        return _redirect(error="Employee was not found.")

    if employee.get("status") == "terminated":
        employee["status"] = "active"
        employee["terminated_at"] = ""
        employee["updated_at"] = _now_iso()
        _append_history(employee, None, "restored", {"employee": employee.get("name", "")})
        _save_data(data)

    return _redirect(notice=f"Restored {employee.get('name', 'employee')} to active status.")


@router.post("/employees/{employee_id}/delete")
async def delete_employee(employee_id: str):
    data = _load_data()
    employee = _find_employee(data, employee_id)
    if employee is None:
        return _redirect(error="Employee was not found.")

    data["employees"] = [emp for emp in data["employees"] if emp.get("id") != employee_id]
    _save_data(data)

    return _redirect(notice=f"Deleted {employee.get('name', 'employee')} entirely.")


@router.post("/access-items")
async def add_access_item(name: str = Form(...), department: str = Form(...)):
    data = _load_data()
    cleaned_name = re.sub(r"\s+", " ", name).strip()
    if not cleaned_name:
        return _redirect(error="Access name is required.")

    department_name = _normalize_access_department(department)
    existing = next(
        (
            item
            for item in data["access_items"]
            if item["name"].lower() == cleaned_name.lower()
            and item["department"].lower() == department_name.lower()
        ),
        None,
    )

    if existing:
        existing["active"] = True
        existing["updated_at"] = _now_iso()
        _save_data(data)
        return _redirect(notice=f"Reactivated access item: {cleaned_name}.")

    data["access_items"].append(
        {
            "id": uuid4().hex,
            "name": cleaned_name,
            "department": department_name,
            "active": True,
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        }
    )
    _save_data(data)
    return _redirect(notice=f"Added access item: {cleaned_name}.")


@router.post("/access-items/{access_id}/remove")
async def remove_access_item(access_id: str):
    data = _load_data()
    item = next((record for record in data["access_items"] if record["id"] == access_id), None)
    if item is None:
        return _redirect(error="Access item not found.")

    item["active"] = False
    item["updated_at"] = _now_iso()
    _save_data(data)
    return _redirect(notice=f"Removed access item: {item.get('name', 'item')}.")


@router.post("/access-items/remove-bulk")
async def remove_access_items_bulk(request: Request):
    form_data = await request.form()
    access_ids = form_data.getlist("access_ids")
    
    if not access_ids:
        return _redirect(error="No access items selected.")

    data = _load_data()
    removed_count = 0
    for access_id in access_ids:
        item = next((record for record in data["access_items"] if record["id"] == str(access_id)), None)
        if item and item.get("active", True):
            item["active"] = False
            item["updated_at"] = _now_iso()
            removed_count += 1
            
    if removed_count > 0:
        _save_data(data)
        return _redirect(notice=f"Removed {removed_count} access item(s).")
    
    return _redirect(notice="No active access items were removed.")


def _build_employee_access_view(employee: dict[str, Any], access_items: list[dict[str, Any]], include_inactive: bool = False):
    access_state = employee.get("access", {})

    all_departments: list[dict[str, Any]] = []
    own_department: list[dict[str, Any]] = []
    other_departments: Dict[str, list[dict[str, Any]]] = {}

    for item in sorted(access_items, key=lambda value: (value.get("department", "").lower(), value.get("name", "").lower())):
        if not include_inactive and not item.get("active", True):
            continue

        state = access_state.get(item["id"], {"granted": False, "removed": False})
        entry = {
            "id": item["id"],
            "name": item["name"],
            "department": item["department"],
            "granted": bool(state.get("granted", False)),
            "removed": bool(state.get("removed", False)),
            "active": item.get("active", True),
        }

        if item["department"] == "All Departments":
            all_departments.append(entry)
        elif item["department"] == employee.get("department"):
            own_department.append(entry)
        else:
            other_departments.setdefault(item["department"], []).append(entry)

    ordered_other = []
    for dept_name in sorted(other_departments.keys(), key=lambda value: value.lower()):
        ordered_other.extend(other_departments[dept_name])

    all_departments = _sort_access_rows_by_granted(all_departments)
    own_department = _sort_access_rows_by_granted(own_department)
    ordered_other = _sort_access_rows_by_granted(ordered_other)

    return {
        "all": all_departments,
        "own": own_department,
        "other": ordered_other,
        "history": list(reversed(employee.get("history", [])))[:30],
    }


def _sort_access_rows_by_granted(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            not row.get("granted", False),
            row.get("department", "").lower(),
            row.get("name", "").lower(),
        ),
    )


def _redirect(notice: str = "", error: str = ""):
    query_parts = []
    if notice:
        query_parts.append(f"notice={notice}")
    if error:
        query_parts.append(f"error={error}")
    suffix = f"?{'&'.join(query_parts)}" if query_parts else ""
    return RedirectResponse(url=f"/employee-access{suffix}", status_code=303)


def _normalize_department(department: str) -> str:
    cleaned = re.sub(r"\s+", " ", department).strip()
    if cleaned in DEPARTMENTS:
        return cleaned
    return cleaned or "Admin"


def _normalize_access_department(department: str) -> str:
    cleaned = re.sub(r"\s+", " ", department).strip()
    if cleaned == "All Departments":
        return cleaned
    return _normalize_department(cleaned)


def _available_access_departments(data: dict[str, Any]) -> list[str]:
    from_data = {
        re.sub(r"\s+", " ", item.get("department", "")).strip()
        for item in data.get("access_items", [])
        if item.get("department")
    }
    all_departments = set(DEPARTMENTS)
    all_departments.update(from_data)
    all_departments.add("All Departments")
    return sorted(all_departments, key=lambda value: (value != "All Departments", value.lower()))


def _find_employee(data: dict[str, Any], employee_id: str):
    return next((employee for employee in data["employees"] if employee.get("id") == employee_id), None)


def _append_history(employee: dict[str, Any], access_id: str | None, action: str, details: dict[str, Any]):
    employee.setdefault("history", []).append(
        {
            "id": uuid4().hex,
            "when": _now_iso(),
            "access_id": access_id,
            "action": action,
            "details": details,
        }
    )


def _load_data() -> dict[str, Any]:
    if not DATA_FILE.exists():
        data = _default_data()
        _save_data(data)
        return data

    raw = DATA_FILE.read_text(encoding="utf-8").strip()
    if not raw:
        data = _default_data()
        _save_data(data)
        return data

    parsed = json.loads(raw)
    parsed.setdefault("employees", [])
    parsed.setdefault("access_items", [])
    return parsed


def _save_data(data: dict[str, Any]):
    DATA_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _default_data() -> dict[str, Any]:
    now = _now_iso()
    access_items = [
        {
            "id": uuid4().hex,
            "name": name,
            "department": department,
            "active": True,
            "created_at": now,
            "updated_at": now,
        }
        for name, department in INITIAL_ACCESS_ITEMS
    ]

    employees = [
        {
            "id": uuid4().hex,
            "name": employee["name"],
            "title": employee.get("title", ""),
            "department": employee["department"],
            "status": "active",
            "access": {},
            "history": [],
            "created_at": now,
            "updated_at": now,
            "terminated_at": "",
        }
        for employee in INITIAL_EMPLOYEES
    ]

    return {"employees": employees, "access_items": access_items}


def _ensure_employee_access_maps(data: dict[str, Any]):
    for employee in data.get("employees", []):
        access_map = employee.get("access")
        if not isinstance(access_map, dict):
            employee["access"] = {}
        employee.setdefault("history", [])


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
