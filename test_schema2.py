import asyncio
from apps.reportable.views import _list_columns

print("VENUE COLUMNS:")
try:
    cols = _list_columns("venue")
    for c in cols:
        print(c['name'])
except Exception as e:
    print("Error:", e)

print("\nCLIENT COLUMNS:")
try:
    cols = _list_columns("client")
    for c in cols:
        print(c['name'])
except Exception as e:
    print("Error:", e)

print("\nSTATUS COLUMNS:")
try:
    cols = _list_columns("status")
    for c in cols:
        print(c['name'])
except Exception as e:
    print("Error:", e)
