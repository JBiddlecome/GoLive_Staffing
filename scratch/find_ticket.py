import json

with open("data/orders_inbox.json", "r") as f:
    state = json.load(f)
    
tickets = state.get("pending_tickets", [])
print(f"Total pending tickets: {len(tickets)}")

found = []
for t in tickets:
    text_to_search = str(t.values()).lower()
    if "1618" in text_to_search or "trubbq" in text_to_search or "tru bbq" in text_to_search:
        found.append(t)

print(f"Found {len(found)} tickets:")
for f in found:
    print(json.dumps(f, indent=2))
