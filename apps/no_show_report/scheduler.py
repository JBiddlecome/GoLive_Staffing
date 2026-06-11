import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from apps.no_show_report.views import (
    get_previous_week_range,
    send_no_show_report_email,
)

_STATE_FILE = Path("apps/no_show_report/no_show_report_state.json")
LA_TZ = ZoneInfo("America/Los_Angeles")


def _load_sent_state() -> dict:
    if _STATE_FILE.exists():
        try:
            with open(_STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"last_sent_date": ""}


def _save_sent_state(date_str: str) -> None:
    # Save the execution state to avoid multiple emails in the same 9am hour block
    try:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(_STATE_FILE, "w") as f:
            json.dump({"last_sent_date": date_str}, f)
    except Exception as e:
        print(f"[No Show Report Scheduler] Failed to save execution state: {e}")


def is_monday_at_9am(now_dt: datetime) -> bool:
    # weekday() -> 0 is Monday
    return now_dt.weekday() == 0 and now_dt.hour == 9


async def no_show_report_loop():
    """Background loop to periodically check and dispatch the weekly No Show report on Mondays at 9am."""
    # Stagger startup offset to avoid collision
    await asyncio.sleep(200)

    if os.getenv("RENDER", "").lower() != "true":
        print("[No Show Report Scheduler] Outside Render environment (local). Automated scheduler loop disabled.")
        return

    print("[No Show Report Scheduler] Weekly background monitor started.")

    while True:
        try:
            now = datetime.now(LA_TZ)
            today_str = now.strftime("%Y-%m-%d")

            if is_monday_at_9am(now):
                state = _load_sent_state()
                if state.get("last_sent_date") != today_str:
                    print(f"[No Show Report Scheduler] Time condition matched (Monday 9am). Dispatching report for {today_str}.")
                    
                    # Calculate date range for previous Monday to Sunday
                    start_date, end_date = get_previous_week_range()
                    
                    res = send_no_show_report_email(
                        start_date=start_date,
                        end_date=end_date,
                        recipient="jake@culinarystaffing.com",
                    )
                    
                    if res.get("success"):
                        _save_sent_state(today_str)
                        print(f"[No Show Report Scheduler] Weekly report sent successfully. State logged for {today_str}.")
                    else:
                        print(f"[No Show Report Scheduler] Weekly report failed to send: {res.get('error') or 'Unknown error'}")

        except Exception as error:
            print(f"[No Show Report Scheduler] Exception cycle interrupt: {error}")

        # Check every 20 minutes (1200 seconds)
        await asyncio.sleep(1200)
