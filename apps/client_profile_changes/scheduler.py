import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import json
from pathlib import Path
from sqlalchemy import text
from apps.client_profile_changes.views import _engine

# ---------------------------------------------------------------------------
# Persistent state file — use Render persistent disk when available
# ---------------------------------------------------------------------------
_RENDER_DATA_DIR = Path("/var/data")
_LOCAL_STATE_PATH = Path("apps/client_profile_changes/cpc_state.json")

if any(os.getenv(v) for v in ("RENDER", "RENDER_SERVICE_ID", "RENDER_EXTERNAL_URL")):
    _state_dir = _RENDER_DATA_DIR
elif _RENDER_DATA_DIR.exists():
    _state_dir = _RENDER_DATA_DIR
else:
    _state_dir = _LOCAL_STATE_PATH.parent

_state_dir.mkdir(parents=True, exist_ok=True)
_state_file = _state_dir / "cpc_state.json"

# In-memory state
_client_cache = {}        # {client_id: {field: value, ...}}
_venue_cache = {}         # {venue_id: {client_id, client_name, venue_name}}
_late_fee_cache = {}      # {client_id: {policies: set(), client_name: str}}
_changes_log = []         # list of change dicts
_is_initialized = False
_last_email_sent_date = None

# ---------------------------------------------------------------------------
# Lookup table maps  (id -> display text)
# ---------------------------------------------------------------------------
_msp_map = {}
_division_map = {}
_net_terms_map = {}
_billing_type_map = {}
_late_fee_policy_map = {}

# Payment type is a simple integer enum — no lookup table
_PAYMENT_TYPE_LABELS = {
    1: "Credit Card",
    2: "Invoice",
}

_SEPARATE_VENUE_MAP = {
    0: "No",
    1: "Invoice venues separately",
    2: "Venues are separate customers",
    3: "Invioce venues separately and by day",
    4: "Venues are separate customers and separated by day",
    5: "Invoice separately by PO number",
}

_NO_BREAK_PENALTY_MAP = {
    1: "Enabled",
    2: "Disabled",
}
# Human-readable labels for each tracked field
_FIELD_LABELS = {
    "name": "Client Name",
    "msp_id": "MSP",
    "division_id": "Division",
    "net_terms_entry_id": "Net Terms",
    "separate_venue": "Invoice Separation",
    "invoices_offset": "Invoices Offset",
    "payment_type": "Payment Type",
    "billing_type_id": "Billing Type",
    "credit_card_expiration": "CC Expiration",
    "credit_card_authorization_date": "CC Authorization Date",
    "pay_notes": "Pay Notes",
    "discount": "Discount",
    "discount_vaild_date": "Discount Valid Date",
    "markup": "Markup",
    "surcharge_deadline": "Surcharge Deadline",
    "exposure_limit": "Exposure Limit",
    "no_break_penalty": "Break Penalties",
}

# Fields whose integer values should be resolved via lookup tables
_LOOKUP_FIELDS = {
    "msp_id": "_msp_map",
    "division_id": "_division_map",
    "net_terms_entry_id": "_net_terms_map",
    "billing_type_id": "_billing_type_map",
    "payment_type": "_PAYMENT_TYPE_LABELS",
    "separate_venue": "_SEPARATE_VENUE_MAP",
    "no_break_penalty": "_NO_BREAK_PENALTY_MAP",
}

# Client table columns to monitor
_CLIENT_FIELDS = [
    "name", "msp_id", "division_id", "net_terms_entry_id",
    "separate_venue", "invoices_offset", "payment_type",
    "billing_type_id", "credit_card_expiration",
    "credit_card_authorization_date", "pay_notes", "discount",
    "discount_vaild_date", "markup", "surcharge_deadline",
    "exposure_limit", "no_break_penalty",
]


# ---------------------------------------------------------------------------
# Helpers — serialisation-safe conversion
# ---------------------------------------------------------------------------
def _serialise_val(v):
    """Convert DB value to JSON-safe string."""
    if v is None:
        return None
    if isinstance(v, (datetime,)):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _display_val(field, raw_value):
    """Return a human-readable display value for a field."""
    if raw_value is None or raw_value == "None":
        return "(empty)"

    lookup_attr = _LOOKUP_FIELDS.get(field)
    if lookup_attr:
        if lookup_attr == "_msp_map":
            lookup = _msp_map
        elif lookup_attr == "_division_map":
            lookup = _division_map
        elif lookup_attr == "_net_terms_map":
            lookup = _net_terms_map
        elif lookup_attr == "_billing_type_map":
            lookup = _billing_type_map
        elif lookup_attr == "_PAYMENT_TYPE_LABELS":
            lookup = _PAYMENT_TYPE_LABELS
        elif lookup_attr == "_SEPARATE_VENUE_MAP":
            lookup = _SEPARATE_VENUE_MAP
        elif lookup_attr == "_NO_BREAK_PENALTY_MAP":
            lookup = _NO_BREAK_PENALTY_MAP
        else:
            lookup = {}

        try:
            int_key = int(raw_value)
            return lookup.get(int_key, f"Unknown ({raw_value})")
        except (ValueError, TypeError):
            return str(raw_value)

    return str(raw_value)


# ---------------------------------------------------------------------------
# State persistence  (matches AR Contact Updates pattern)
# ---------------------------------------------------------------------------
def _load_state():
    if _state_file.exists():
        try:
            with open(_state_file, "r") as f:
                state = json.load(f)
                return (
                    state.get("client_cache", {}),
                    state.get("venue_cache", {}),
                    state.get("late_fee_cache", {}),
                    state.get("changes", []),
                    state.get("last_email_sent_date"),
                )
        except Exception:
            return {}, {}, {}, [], None
    return {}, {}, {}, [], None


def _save_state(client_cache, venue_cache, late_fee_cache, changes, last_email_sent_date=None):
    try:
        existing_cc, existing_vc, existing_lfc, existing_changes, existing_email_date = _load_state()

        # Merge changes by unique key to prevent duplicates
        merged = {
            f"{c.get('timestamp','')}_{c.get('client_name','')}_{c.get('field','')}_{c.get('section','')}"
            : c for c in existing_changes
        }
        for c in changes:
            merged[f"{c.get('timestamp','')}_{c.get('client_name','')}_{c.get('field','')}_{c.get('section','')}"] = c

        all_changes = list(merged.values())

        # Keep only last 90 days
        now_la = datetime.now(ZoneInfo("America/Los_Angeles"))
        cutoff = now_la - timedelta(days=90)
        filtered = []
        for c in all_changes:
            try:
                ctime = datetime.strptime(c["timestamp"], "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=ZoneInfo("America/Los_Angeles")
                )
                if ctime >= cutoff:
                    filtered.append(c)
            except Exception:
                pass

        filtered.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

        final_email_date = last_email_sent_date or existing_email_date

        # Convert late_fee_cache sets to lists for JSON
        lfc_serialisable = {}
        for cid, val in late_fee_cache.items():
            if isinstance(val, dict):
                policies = val.get("policies", set())
                lfc_serialisable[str(cid)] = {
                    "policies": list(policies) if isinstance(policies, set) else policies,
                    "client_name": val.get("client_name", ""),
                }
            elif isinstance(val, set):
                lfc_serialisable[str(cid)] = list(val)
            else:
                lfc_serialisable[str(cid)] = val

        # Only write to disk on Render; skip locally to keep the workspace clean
        _is_render = any(os.getenv(v) for v in ("RENDER", "RENDER_SERVICE_ID", "RENDER_EXTERNAL_URL"))
        if not _is_render:
            return filtered

        # Atomic write
        temp = _state_file.with_suffix(".tmp")
        with open(temp, "w") as f:
            json.dump({
                "client_cache": client_cache,
                "venue_cache": venue_cache,
                "late_fee_cache": lfc_serialisable,
                "changes": filtered,
                "last_email_sent_date": final_email_date,
            }, f)
        temp.replace(_state_file)

        return filtered
    except Exception as e:
        print(f"[Client Profile Changes] Error saving state: {e}")
        import traceback
        traceback.print_exc()
        return changes


# ---------------------------------------------------------------------------
# Lookup table loaders
# ---------------------------------------------------------------------------
def _load_lookup_tables():
    global _msp_map, _division_map, _net_terms_map, _billing_type_map, _late_fee_policy_map
    engine = _engine()
    try:
        with engine.begin() as conn:
            rows = conn.execute(text("SELECT id, name FROM msp")).mappings().all()
            _msp_map = {r["id"]: r["name"] for r in rows}

            rows = conn.execute(text("SELECT id, name FROM division")).mappings().all()
            _division_map = {r["id"]: r["name"] for r in rows}

            rows = conn.execute(text("SELECT id, net_terms FROM net_terms_entry")).mappings().all()
            _net_terms_map = {r["id"]: str(r["net_terms"]) for r in rows}

            rows = conn.execute(text("SELECT id, name FROM billing_type")).mappings().all()
            _billing_type_map = {r["id"]: r["name"] for r in rows}

            rows = conn.execute(text("SELECT id, name FROM late_fee_policy")).mappings().all()
            _late_fee_policy_map = {r["id"]: r["name"] for r in rows}
    except Exception as e:
        print(f"[Client Profile Changes] Error loading lookup tables: {e}")
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# Fetch current snapshots
# ---------------------------------------------------------------------------
def _fetch_clients():
    """Return dict  {str(client_id): {field: serialised_value, ..., 'name': ...}}"""
    cols = ", ".join([f"c.{f}" for f in _CLIENT_FIELDS])
    sql = text(f"""
        SELECT c.client_id, {cols}
        FROM client c
        WHERE c.deleted_at IS NULL
    """)
    engine = _engine()
    try:
        with engine.begin() as conn:
            rows = conn.execute(sql).mappings().all()
            result = {}
            for r in rows:
                cid = str(r["client_id"])
                result[cid] = {f: _serialise_val(r[f]) for f in _CLIENT_FIELDS}
            return result
    except Exception as e:
        print(f"[Client Profile Changes] Error fetching clients: {e}")
        return None
    finally:
        engine.dispose()


def _fetch_venues():
    """Return dict  {str(venue_id): {client_id, client_name, venue_name}}"""
    sql = text("""
        SELECT v.venue_id, v.client_id, v.name AS venue_name, c.name AS client_name
        FROM venue v
        JOIN client c ON v.client_id = c.client_id
        WHERE c.deleted_at IS NULL AND v.deleted_at IS NULL
    """)
    engine = _engine()
    try:
        with engine.begin() as conn:
            rows = conn.execute(sql).mappings().all()
            return {
                str(r["venue_id"]): {
                    "client_id": str(r["client_id"]),
                    "client_name": r["client_name"] or "N/A",
                    "venue_name": r["venue_name"] or "N/A",
                } for r in rows
            }
    except Exception as e:
        print(f"[Client Profile Changes] Error fetching venues: {e}")
        return None
    finally:
        engine.dispose()


def _fetch_late_fees():
    """Return dict  {str(client_id): {policies: set of int, client_name: str}}"""
    sql = text("""
        SELECT clf.client_id, clf.late_fee_policy_id, c.name AS client_name
        FROM client_late_fee clf
        JOIN client c ON clf.client_id = c.client_id
        WHERE c.deleted_at IS NULL
    """)
    engine = _engine()
    try:
        with engine.begin() as conn:
            rows = conn.execute(sql).mappings().all()
            result = {}
            for r in rows:
                cid = str(r["client_id"])
                if cid not in result:
                    result[cid] = {"policies": set(), "client_name": r["client_name"] or "N/A"}
                result[cid]["policies"].add(int(r["late_fee_policy_id"]))
            return result
    except Exception as e:
        print(f"[Client Profile Changes] Error fetching late fees: {e}")
        return None
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------
def _send_daily_email(changes: list[dict]):
    # Only send emails from the live Render server — skip on local dev machines
    _is_render = any(os.getenv(v) for v in ("RENDER", "RENDER_SERVICE_ID", "RENDER_EXTERNAL_URL"))
    if not _is_render:
        print("[Client Profile Changes] Skipping daily email: not running on Render (local environment detected).")
        return

    sender_email = "golive@culinarystaffing.com"
    receiver_emails = ["jake@culinarystaffing.com", "accounting@culinarystaffing.com", "caleb@culinarystaffing.com", "dan.stone@culinarystaffing.com"]

    import requests

    tenant_id = os.getenv("O365_TENANT_ID")
    client_id = os.getenv("O365_CLIENT_ID")
    client_secret = os.getenv("O365_CLIENT_SECRET")

    if not all([tenant_id, client_id, client_secret]):
        print("[Client Profile Changes] Skipping email: Microsoft 365 OAuth credentials not fully set.")
        return

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
    }

    try:
        r = requests.post(token_url, data=token_data)
        r.raise_for_status()
        access_token = r.json().get("access_token")
    except Exception as e:
        print(f"[Client Profile Changes] Failed to authenticate with Microsoft Graph: {e}")
        return

    subject = f"Client Profile Changes: {len(changes)} Change{'s' if len(changes) != 1 else ''} Today"

    html_body = """
    <html>
      <body style="font-family: Arial, sans-serif;">
        <h2 style="color: #1e40af;">Client Profile Changes — Daily Report</h2>
        <p>The following """ + str(len(changes)) + """ client profile change(s) were detected today:</p>
        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%; max-width: 900px;">
            <tr style="background-color: #f3f4f6; text-align: left;">
                <th>Timestamp</th>
                <th>Section</th>
                <th>Client</th>
                <th>Field</th>
                <th>Old Value</th>
                <th>New Value</th>
            </tr>
    """

    for c in changes:
        section = c.get("section", "Client")
        html_body += f"""
            <tr>
                <td>{c.get('timestamp', 'N/A')}</td>
                <td>{section}</td>
                <td>{c.get('client_name', 'N/A')}</td>
                <td>{c.get('field_label', c.get('field', 'N/A'))}</td>
                <td style="color: #991b1b;">{c.get('old_display', c.get('old_value', 'N/A'))}</td>
                <td style="color: #065f46;">{c.get('new_display', c.get('new_value', 'N/A'))}</td>
            </tr>
        """

    html_body += """
        </table>
        <p style="margin-top: 20px; font-size: 12px; color: #6b7280;"><em>This is an automated alert from GoLive Staffing Tools.</em></p>
      </body>
    </html>
    """

    email_msg = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": email}} for email in receiver_emails],
        },
        "saveToSentItems": "false",
    }

    send_url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        send_res = requests.post(send_url, headers=headers, json=email_msg)
        send_res.raise_for_status()
        print(f"[Client Profile Changes] Successfully sent daily report email to {receiver_emails}.")
    except Exception as e:
        print(f"[Client Profile Changes] Failed to send email via MS Graph: {e}")


# ---------------------------------------------------------------------------
# Public accessor  (called by views.py /data endpoint)
# ---------------------------------------------------------------------------
def get_changes_log():
    return sorted(_changes_log, key=lambda x: x.get("timestamp", ""), reverse=True)


# ---------------------------------------------------------------------------
# Main monitoring loop  (follows AR Contact Updates pattern)
# ---------------------------------------------------------------------------
async def client_profile_changes_monitoring_loop():
    global _client_cache, _venue_cache, _late_fee_cache
    global _changes_log, _is_initialized, _last_email_sent_date

    # Load initial state from disk
    loaded_cc, loaded_vc, loaded_lfc, loaded_changes, loaded_email_date = _load_state()
    if loaded_cc:
        _client_cache = loaded_cc
        _venue_cache = loaded_vc or {}
        # Restore late_fee_cache from disk (convert lists back to sets)
        _late_fee_cache = {}
        for cid, val in loaded_lfc.items():
            if isinstance(val, list):
                _late_fee_cache[cid] = {"policies": set(val), "client_name": ""}
            elif isinstance(val, dict):
                _late_fee_cache[cid] = {
                    "policies": set(val.get("policies", [])),
                    "client_name": val.get("client_name", ""),
                }
        _changes_log = loaded_changes
        if loaded_email_date:
            _last_email_sent_date = loaded_email_date
        _is_initialized = True
        print(f"[Client Profile Changes] Loaded {len(_client_cache)} clients, "
              f"{len(_venue_cache)} venues, {len(_changes_log)} changes from disk.")
    elif loaded_changes:
        # No cache but we have changes — restore changes so they're not lost
        _changes_log = loaded_changes
        if loaded_email_date:
            _last_email_sent_date = loaded_email_date
        print(f"[Client Profile Changes] Loaded {len(_changes_log)} changes from disk (no cache, will re-init).")

    # Stagger startup
    await asyncio.sleep(30)

    print("[Client Profile Changes] Monitor started.")

    while True:
        try:
            # Load lookup tables for display values
            _load_lookup_tables()

            # Fetch current DB state
            current_clients = _fetch_clients()
            current_venues = _fetch_venues()
            current_late_fees = _fetch_late_fees()

            if current_clients is not None:
                if not _is_initialized:
                    # First load — populate cache without recording changes
                    _client_cache = current_clients
                    _venue_cache = current_venues or {}
                    _late_fee_cache = current_late_fees or {}
                    _is_initialized = True
                    # Save baseline to disk so it survives restarts
                    _save_state(
                        _client_cache, _venue_cache,
                        _late_fee_cache, _changes_log,
                    )
                    print(f"[Client Profile Changes] Initialized with {len(_client_cache)} clients, "
                          f"{len(_venue_cache)} venues. Next check will detect diffs.")
                else:
                    # Diff against cache
                    timestamp = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d %H:%M:%S")
                    found_changes = False

                    # ── Client field changes ──
                    for cid, current in current_clients.items():
                        cached = _client_cache.get(cid)
                        if cached is None:
                            continue  # new client, not a "change"
                        client_name = current.get("name") or cached.get("name") or f"Client #{cid}"
                        for field in _CLIENT_FIELDS:
                            old_val = cached.get(field)
                            new_val = current.get(field)
                            if old_val != new_val:
                                change = {
                                    "timestamp": timestamp,
                                    "section": "Client",
                                    "client_name": client_name,
                                    "field": field,
                                    "field_label": _FIELD_LABELS.get(field, field),
                                    "old_value": old_val,
                                    "new_value": new_val,
                                    "old_display": _display_val(field, old_val),
                                    "new_display": _display_val(field, new_val),
                                }
                                _changes_log.append(change)
                                found_changes = True
                                print(f"[Client Profile Changes] {client_name}: "
                                      f"{_FIELD_LABELS.get(field, field)} "
                                      f"'{_display_val(field, old_val)}' -> '{_display_val(field, new_val)}'")
                    _client_cache = current_clients

                    # ── Venue name changes ──
                    if current_venues is not None:
                        for vid, current_v in current_venues.items():
                            cached_v = _venue_cache.get(vid)
                            if cached_v is None:
                                continue
                            if cached_v.get("venue_name") != current_v.get("venue_name"):
                                change = {
                                    "timestamp": timestamp,
                                    "section": "Venue",
                                    "client_name": current_v["client_name"],
                                    "field": "venue_name",
                                    "field_label": "Venue Name",
                                    "old_value": cached_v.get("venue_name"),
                                    "new_value": current_v.get("venue_name"),
                                    "old_display": cached_v.get("venue_name") or "(empty)",
                                    "new_display": current_v.get("venue_name") or "(empty)",
                                }
                                _changes_log.append(change)
                                found_changes = True
                                print(f"[Client Profile Changes] Venue change for "
                                      f"{current_v['client_name']}: "
                                      f"'{cached_v.get('venue_name')}' -> '{current_v.get('venue_name')}'")
                        _venue_cache = current_venues

                    # ── Late fee policy changes ──
                    if current_late_fees is not None:
                        all_client_ids = set(list(_late_fee_cache.keys()) + list(current_late_fees.keys()))
                        for cid in all_client_ids:
                            old_entry = _late_fee_cache.get(cid, {"policies": set(), "client_name": ""})
                            new_entry = current_late_fees.get(cid, {"policies": set(), "client_name": ""})

                            old_policies = old_entry.get("policies", set())
                            new_policies = new_entry.get("policies", set())
                            if isinstance(old_policies, list):
                                old_policies = set(old_policies)
                            if isinstance(new_policies, list):
                                new_policies = set(new_policies)

                            if old_policies != new_policies:
                                client_name = new_entry.get("client_name") or old_entry.get("client_name") or f"Client #{cid}"
                                old_names = sorted([_late_fee_policy_map.get(pid, f"ID {pid}") for pid in old_policies])
                                new_names = sorted([_late_fee_policy_map.get(pid, f"ID {pid}") for pid in new_policies])
                                change = {
                                    "timestamp": timestamp,
                                    "section": "Late Fees",
                                    "client_name": client_name,
                                    "field": "late_fee_policies",
                                    "field_label": "Late Fee Policies",
                                    "old_value": ", ".join(old_names) if old_names else "(none)",
                                    "new_value": ", ".join(new_names) if new_names else "(none)",
                                    "old_display": ", ".join(old_names) if old_names else "(none)",
                                    "new_display": ", ".join(new_names) if new_names else "(none)",
                                }
                                _changes_log.append(change)
                                found_changes = True
                                print(f"[Client Profile Changes] Late fee change for "
                                      f"{client_name}: {old_names} -> {new_names}")
                        _late_fee_cache = current_late_fees

                    # Always save updated cache + any new changes
                    if found_changes:
                        _changes_log[:] = _save_state(
                            _client_cache, _venue_cache,
                            _late_fee_cache, _changes_log,
                        )

                    print(f"[Client Profile Changes] Check complete. "
                          f"changes_found={found_changes}, total_changes={len(_changes_log)}, "
                          f"cache_size={len(_client_cache)}")

        except Exception as e:
            print(f"[Client Profile Changes] Exception in loop: {e}")
            import traceback
            traceback.print_exc()

        # ── Daily email check ──
        if _client_cache:
            try:
                now_la = datetime.now(ZoneInfo("America/Los_Angeles"))
                if now_la.hour >= 8:
                    today_str = now_la.strftime("%Y-%m-%d")

                    # Re-check disk in case another worker already sent
                    _, _, _, _, disk_email_date = _load_state()
                    if disk_email_date == today_str:
                        _last_email_sent_date = today_str

                    if _last_email_sent_date != today_str:
                        yesterday = now_la - timedelta(days=1)
                        yesterday_str = yesterday.strftime("%Y-%m-%d")
                        recent_changes = []
                        for c in _changes_log:
                            ts = c.get("timestamp", "")
                            if ts.startswith(today_str) or ts.startswith(yesterday_str):
                                recent_changes.append(c)

                        if recent_changes:
                            _send_daily_email(recent_changes)
                        else:
                            print("[Client Profile Changes] No changes today, skipping email.")

                        _last_email_sent_date = today_str
                        _changes_log[:] = _save_state(
                            _client_cache, _venue_cache,
                            _late_fee_cache, _changes_log, _last_email_sent_date,
                        )
            except Exception as e:
                print(f"[Client Profile Changes] Error sending daily email: {e}")

        # Check every 15 minutes (same as AR Contacts)
        await asyncio.sleep(900)
