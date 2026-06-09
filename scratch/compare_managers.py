from dotenv import load_dotenv
load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

import json
from apps.orders.knowledge_base import get_staffing_manager_for_client

with open("data/orders_inbox.json", "r") as f:
    state = json.load(f)

tickets = state.get("pending_tickets", [])
print(f"Checking {len(tickets)} pending tickets:")

mismatch_count = 0
for t in tickets:
    client_id = t.get("client_id")
    cached_mgr = t.get("staffing_manager")
    
    if client_id:
        db_mgr = get_staffing_manager_for_client(client_id)
        if cached_mgr != db_mgr:
            print(f"Ticket ID: {t.get('id')[:15]}...")
            print(f"  Client ID: {client_id} ({t.get('client_name')})")
            print(f"  Sender: {t.get('sender_email')}")
            print(f"  Cached SM: {cached_mgr}")
            print(f"  DB SM:     {db_mgr}")
            print(f"  Account (Mailbox): {t.get('account')}")
            mismatch_count += 1

print(f"Total mismatches: {mismatch_count}")
