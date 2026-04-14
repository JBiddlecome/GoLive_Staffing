import traceback
from apps.concierge_tracker_2.views import _maybe_sync_from_db

try:
    _maybe_sync_from_db()
    print("DB sync success")
except Exception as e:
    traceback.print_exc()
