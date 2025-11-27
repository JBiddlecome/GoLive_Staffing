import math
import os
import re
import subprocess
import sys
from pathlib import Path
from datetime import date

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


def log(msg: str):
    print(f"[LOG] {msg}")


# ----------------------------------------------------------------------
# CONFIG (now read from environment for security)
# ----------------------------------------------------------------------
BASE_URL = os.environ.get("GOLIVE_BASE_URL", "https://dev7.culinarystaffing.com")
USERNAME = os.environ.get("GOLIVE_USERNAME", "replace_me@example.com")
PASSWORD = os.environ.get("GOLIVE_PASSWORD", "replace_me_password")


# ----------------------------------------------------------------------
# DATA LOADING / PARSING
# ----------------------------------------------------------------------
def load_data(path: Path) -> pd.DataFrame:
    """Load the data file as a pandas DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    if path.suffix.lower() in [".xlsx", ".xls"]:
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)

    if "Field Name" not in df.columns or "Entry" not in df.columns:
        raise ValueError(
            "Expected columns 'Field Name' and 'Entry' not found. "
            "Make sure the CSV/Excel matches the template."
        )
    return df
    

def parse_client_positions_venue(df: pd.DataFrame):
    """
    Parse the single-sheet template into:
      - client_data: dict of field -> value
      - positions: list of dicts {name, bill_rate, pay_rate, surcharge}
      - contacts: list of dicts {name, email, phone, invoicing}
      - venue_data: dict of field -> value
    """
    client_data = {}
    positions = []
    contacts = []
    venue_data = {}

    client_section_started = False
    positions_section_started = False
    contacts_section_started = False
    venue_section_started = False

    pos_header = None
    contact_header = None

    for _, row in df.iterrows():
        field = str(row.get("Field Name")).strip() if not pd.isna(row.get("Field Name")) else None
        entry = row.get("Entry")

        # Detect section headers
        if field == "Client Tab":
            client_section_started = True
            positions_section_started = False
            contacts_section_started = False
            venue_section_started = False
            continue

        if field == "Positions Tab":
            client_section_started = False
            positions_section_started = True
            contacts_section_started = False
            venue_section_started = False
            pos_header = {
                "bill_col": row.get("Entry"),
                "pay_col": row.get("Unnamed: 2"),
                "surcharge_col": row.get("Unnamed: 3"),
            }
            continue

        if field == "Contacts Tab":
            client_section_started = False
            positions_section_started = False
            contacts_section_started = True
            venue_section_started = False
            contact_header = {
                "name_col": row.get("Entry") or "Name",
                "email_col": row.get("Unnamed: 2"),
                "phone_col": row.get("Unnamed: 3"),
                "invoice_col": row.get("Unnamed: 4"),
            }
            continue

        if field == "Venue Tab":
            client_section_started = False
            positions_section_started = False
            contacts_section_started = False
            venue_section_started = True
            continue

        # CLIENT SECTION
        if client_section_started and field:
            client_data[field] = entry

        # POSITIONS SECTION
        if positions_section_started:
            if not field:
                continue

            bill_rate = row.get("Entry")
            pay_rate = row.get("Unnamed: 2")
            surcharge = row.get("Unnamed: 3")

            # Skip header row which we already used
            if pos_header and field == pos_header["bill_col"]:
                continue

            if (
                (field is None or field == "" or field == "nan")
                and (pd.isna(bill_rate) and pd.isna(pay_rate) and pd.isna(surcharge))
            ):
                continue

            positions.append(
                {
                    "name": field,
                    "bill_rate": bill_rate,
                    "pay_rate": pay_rate,
                    "surcharge": surcharge,
                }
            )

        # CONTACTS SECTION
        if contacts_section_started:
            if not field and pd.isna(entry) and pd.isna(row.get("Unnamed: 2")) and pd.isna(row.get("Unnamed: 3")):
                continue

            if contact_header and field in {contact_header.get("name_col"), "Name", "Contact Name"}:
                continue

            contacts.append(
                {
                    "name": field,
                    "email": entry,
                    "phone": row.get("Unnamed: 2"),
                    "invoicing": row.get("Unnamed: 3"),
                }
            )

        # VENUE SECTION
        if venue_section_started and field:
            venue_data[field] = entry

    return client_data, positions, contacts, venue_data


def ensure_chromium_installed():
    """Install Playwright's Chromium browser if it is missing."""

    default_cache_dir = Path.home() / ".cache" / "ms-playwright"
    browsers_dir = Path(os.environ.get("PLAYWRIGHT_BROWSERS_PATH", default_cache_dir))

    chromium_present = any(
        browsers_dir.glob("chromium*/*/chrome-linux/*chrome")
    ) or any(browsers_dir.glob("chromium*/*/chrome-linux/headless_shell"))

    if chromium_present:
        return

    log("Chromium not found. Installing Playwright browsers (chromium)...")
    try:
        subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            "Failed to install Playwright Chromium. "
            "Please run `python -m playwright install chromium` manually and retry."
        ) from exc


# ----------------------------------------------------------------------
# PLAYWRIGHT HELPERS
# ----------------------------------------------------------------------
def set_field_by_label(page, label: str, value):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return

    text = str(value).strip()
    locator = page.get_by_label(label)

    # Try select (dropdown) using label or value
    for kwargs in ({"label": text}, {"value": text}):
        try:
            locator.select_option(**kwargs)
            return
        except Exception:
            pass

    # Try fill (text input / textarea)
    try:
        locator.fill(text)
        return
    except Exception:
        pass

    # Try checkbox / toggle if value is yes/no
    if text.lower() in ["yes", "true", "1"]:
        try:
            locator.check()
            return
        except Exception:
            pass
    elif text.lower() in ["no", "false", "0"]:
        try:
            locator.uncheck()
            return
        except Exception:
            pass

    print(f"[WARN] Could not set field with label '{label}' to '{text}'")


def safe_click_top_nav(page, tab_name: str):
    try:
        page.get_by_role("link", name=tab_name).click()
    except Exception:
        page.get_by_text(tab_name, exact=True).click()


# ----------------------------------------------------------------------
# APP-SPECIFIC ACTIONS
# ----------------------------------------------------------------------
def login(page):
    log("Opening login page")
    page.goto(BASE_URL, wait_until="domcontentloaded")

    page.get_by_label("Email").fill(USERNAME)
    page.get_by_label("Password").fill(PASSWORD)

    def click_login_button():
        for name in ["Sign in", "Sign In", "Log in", "Log In", "Sign In »"]:
            try:
                page.get_by_role("button", name=name, exact=False).click(timeout=3000)
                return True
            except Exception:
                continue
        try:
            page.locator("button:has-text('Sign in')").first.click(timeout=3000)
            return True
        except Exception:
            return False

    clicked = click_login_button()

    if not clicked:
        page.get_by_label("Password").press("Enter")

    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except PlaywrightTimeoutError:
        pass

    try:
        page.wait_for_function(
            "!!window && !window.location.href.toLowerCase().includes('login')",
            timeout=20000,
        )
    except PlaywrightTimeoutError:
        pass

    log("Login flow complete")


def create_client(page, client_data: dict):
    log("Starting client creation")
    page.goto(f"{BASE_URL}/clients/create", wait_until="networkidle")
    page.wait_for_load_state("domcontentloaded")

    def format_mmddyyyy(value):
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        try:
            if hasattr(value, "strftime"):
                return value.strftime("%m/%d/%Y")
            text = str(value).strip()
            digits = "".join(ch for ch in text if ch.isdigit())
            if len(digits) == 8:
                if int(digits[:4]) > 1900:
                    digits = digits[4:] + digits[:4]
                return f"{digits[:2]}/{digits[2:4]}/{digits[4:]}"
            return text
        except Exception:
            return str(value)

    def select_custom_dropdown(selector: str, option_text: str, type_text: str | None = None):
        if not option_text:
            return
        try:
            dropdown = page.locator(selector).first
            dropdown.click()
            if type_text:
                dropdown.fill(type_text)
            try:
                page.get_by_role("option", name=option_text, exact=True).click(timeout=3000)
            except Exception:
                page.get_by_text(option_text, exact=True).click(timeout=3000)
        except Exception as exc:
            print(f"[WARN] Could not select '{option_text}' via {selector}: {exc}")

    def fill_textbox_by_name(name: str, text: str, exact: bool = True):
        if text is None or (isinstance(text, float) and math.isnan(text)):
            return
        try:
            page.get_by_role("textbox", name=name, exact=exact).fill(str(text))
        except Exception as exc:
            print(f"[WARN] Could not fill textbox '{name}': {exc}")

    custom_dropdowns = {
        "Status": '[id^="status_"]',
        "Bundle": '[id^="bundle_"]',
        "MSP": '[id^="msp_id_"]',
        "Division": '[id^="division_id_"]',
    }

    for field, selector in custom_dropdowns.items():
        select_custom_dropdown(selector, client_data.get(field))

    select_custom_dropdown('[id^="industry_"]', client_data.get("Industry"))
    select_custom_dropdown('[id^="state_"]', client_data.get("State"), type_text=str(client_data.get("State") or "")[:3])
    select_custom_dropdown('[id^="min_wage_id_"]', client_data.get("Minimum Wage"), type_text=str(client_data.get("Minimum Wage") or "")[:5])
    select_custom_dropdown('[id^="net_terms_entry_id_"]', str(client_data.get("Net Terms") or ""))
    select_custom_dropdown('[id^="separate_venue_"]', client_data.get("Invoice Venues Separately"))
    select_custom_dropdown('[id^="background_"]', client_data.get("Background Accepted"))
    select_custom_dropdown('[id^="felony_ids_"]', client_data.get("Felony(s) Allowed"))
    select_custom_dropdown('[id^="misdemeanor_ids_"]', client_data.get("Misdemeanor(s) Allowed"))
    select_custom_dropdown('[id^="payment_type_"]', client_data.get("Payment Type"))
    select_custom_dropdown('[id^="no_break_penalty_"]', client_data.get("Break Penalty"))

    def fill_text_and_fallback(name: str, value, exact: bool = True):
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return
        text_val = str(value)
        try:
            page.get_by_label(name).fill(text_val)
            return
        except Exception:
            pass
        try:
            page.get_by_role("textbox", name=name, exact=exact).fill(text_val, timeout=2000)
        except Exception:
            print(f"[WARN] Could not fill '{name}' via label or textbox role")

    fill_text_and_fallback("Name", client_data.get("Name"))
    fill_text_and_fallback("Address", client_data.get("Address"), exact=True)
    fill_text_and_fallback("Address (optional)", client_data.get("Address (optional)"), exact=True)
    fill_text_and_fallback("City", client_data.get("City"))
    fill_text_and_fallback("Zip Code", client_data.get("Zip Code"))
    fill_text_and_fallback("MSA Expiration *", format_mmddyyyy(client_data.get("MSA Expiration")))

    field_labels = [
        "Surcharge Deadline",
        "Shift Auto Confirm",
        "Pending Background Allowed",
        "Visible Tattoos Allowed",
    ]

    for label in field_labels:
        value = client_data.get(label)
        set_field_by_label(page, label, value)

    log("Clicking Save on client form")
    page.locator("div").filter(has_text=re.compile(r"^Save$")).get_by_role("button").click()
    page.wait_for_timeout(1500)
    log("Client form saved")


def create_positions_and_rates(page, positions):
    today_str = date.today().strftime("%m/%d/%Y")

    def select_position_dropdown(name: str):
        try:
            dropdown = page.locator('[id^="position_id_"]').first
            dropdown.click()
            try:
                page.get_by_role("option", name=name, exact=True).click(timeout=3000)
                return True
            except Exception:
                pass
            try:
                dropdown.fill(str(name))
                page.get_by_role("option", name=name, exact=False).click(timeout=3000)
                return True
            except Exception:
                print(f"[WARN] Could not select position option '{name}'")
                return False
        except Exception as exc:
            print(f"[WARN] Could not open position dropdown: {exc}")
            return False

    for pos in positions:
        name = str(pos["name"])

        log(f"Adding position: {name}")
        safe_click_top_nav(page, "Positions")
        page.get_by_role("button", name="Add New").click()

        if not select_position_dropdown(name):
            set_field_by_label(page, "Position", name)

        page.get_by_role("button", name="Save").click()
        page.wait_for_load_state("networkidle")

        page.get_by_role("button", name="Add Rate").click()

        try:
            page.locator("#bill_rate").fill(str(pos["bill_rate"]))
            page.locator("#pay_rate").fill(str(pos["pay_rate"]))
            page.locator("#surcharge").fill(str(pos["surcharge"]))
            # if there's an effective date field, you could fill today_str here
        except Exception as exc:
            print(f"[WARN] Could not fill rate fields for {name}: {exc}")

        page.get_by_role("button", name="Save").click()
        page.wait_for_timeout(250)


def create_contacts(page, contacts):
    if not contacts:
        return
    log(f"Adding {len(contacts)} contacts")

    def is_truthy(val) -> bool:
        return str(val).strip().lower() in {"yes", "true", "1", "y", "t"}

    try:
        page.get_by_role("tab", name="Contacts").click()
    except Exception:
        try:
            page.get_by_text("Contacts", exact=True).click()
        except Exception:
            print("[WARN] Could not open Contacts tab")
            return

    for contact in contacts:
        page.get_by_role("button", name="Add New").click()
        try:
            page.get_by_role("textbox", name="Name *").fill(str(contact.get("name") or ""))
            page.get_by_role("textbox", name="Email").fill(str(contact.get("email") or ""))
            page.get_by_role("textbox", name="Direct (D)").fill(str(contact.get("phone") or ""))
        except Exception as exc:
            print(f"[WARN] Could not fill contact fields: {exc}")

        if is_truthy(contact.get("invoicing")):
            try:
                page.get_by_role("checkbox", name="Invoicing").check()
            except Exception:
                pass

        try:
            page.get_by_role("button", name="Save").click()
            page.wait_for_timeout(500)
        except Exception as exc:
            print(f"[WARN] Could not save contact: {exc}")


def create_venue(page, venue_data: dict):
    log("Opening Venues tab")
    safe_click_top_nav(page, "Venues")

    try:
        page.get_by_role("link", name="Add New").click()
    except Exception:
        page.get_by_role("button", name="Add New").click()
    log("Starting new venue entry")

    label_map = {
        "Name": "Name",
        "Status": "Status",
        "Address": "Address",
        "Address (optional)": "Address (optional)",
        "City": "City",
        "State": "State",
        "Zip": "Zip",
        "County": "County",
        "Parking": "Parking",
        "Min Wage": "Min Wage",
        "Time Clock": "Time Clock",
        "Time Clock Code Holder": "Time Clock Code Holder",
        "Time Clock Tolerance (minutes)": "Time Clock Tolerance (minutes)",
        "Time Clock Prestart Interval (minutes)": "Time Clock Prestart Interval (minutes)",
        "Parking Note": "Parking Note",
        "Directions": "Directions",
        "Check In": "Check In",
    }

    for template_field, label in label_map.items():
        value = venue_data.get(template_field)
        set_field_by_label(page, label, value)

    page.get_by_role("button", name="Save").click()
    page.wait_for_load_state("networkidle")


# ----------------------------------------------------------------------
# PUBLIC ENTRY POINT FOR WEB APP
# ----------------------------------------------------------------------
def run_profile_creator(data_file: Path):
    """
    Main entrypoint to be called by your web app.
    `data_file` is a Path to the uploaded CSV/XLSX.
    """
    log(f"Loading data from: {data_file}")
    df = load_data(data_file)
    client_data, positions, contacts, venue_data = parse_client_positions_venue(df)

    ensure_chromium_installed()

    with sync_playwright() as p:
        # headless=True for cloud/server environments
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        login(page)
        create_client(page, client_data)
        create_positions_and_rates(page, positions)
        create_contacts(page, contacts)
        create_venue(page, venue_data)

        log("Automation complete.")
        browser.close()


if __name__ == "__main__":
    # Allow local CLI usage:
    import sys

    if len(sys.argv) != 2:
        print("Usage: python golive_profile_creator.py <path_to_data_file>")
        raise SystemExit(1)

    run_profile_creator(Path(sys.argv[1]))
