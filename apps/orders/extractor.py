import os
import json
import openai
from dotenv import load_dotenv

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    load_dotenv()

OPENAI_API_KEY = os.getenv("STAND_ALONE") or os.getenv("OPENAI_API_KEY")

from datetime import datetime
from zoneinfo import ZoneInfo

PT = ZoneInfo("America/Los_Angeles")

SYSTEM_PROMPT = """You are an expert Staffing Manager assistant for GoLive Staffing.
Your job is to extract shift and event order details from raw text (emails or dictations) and map them to our database schema.
You will be provided with a 'Client Context' containing their typical ordering habits and available positions.

Return ONLY a valid JSON object matching this exact schema:
{
    "basic_information": {
        "client_name": "string (Fuzzy match to the client context if possible)",
        "client_id": "integer (From client context)",
        "event_name": "string (Must ALWAYS be identical to the venue_name. Do NOT put any custom event name here)",
        "explicit_event_name": "string (The explicit title of the event mentioned in the text, e.g. 'Summer Gala', ONLY if explicitly stated and different from the venue name. Otherwise leave EMPTY string)",
        "venue_name": "string (The location. Predict based on client context if missing, but if completely ambiguous or multiple venues exist, leave it EMPTY string so the UI can flag it)",
        "event_address1": "string (The physical street address of the event if explicitly provided, otherwise leave EMPTY string)",
        "event_address2": "string (The suite, unit, or apartment number if explicitly provided, e.g., 'Suite 100', otherwise leave EMPTY string)",
        "event_city": "string (The city of the event if explicitly provided, otherwise leave EMPTY string)",
        "event_state": "string (The 2-letter state abbreviation if explicitly provided, otherwise leave EMPTY string)",
        "event_zip": "string (The zip code if explicitly provided, otherwise leave EMPTY string)",
        "purchase_order": "string (If specified)"
    },
    "shift_information": [
        {
            "id": "integer (incrementing ID starting from 1)",
            "action": "string (Allowed values: 'CREATE', 'UPDATE', 'REMOVE'. Defaults to 'CREATE'. Set to 'UPDATE' if the email asks to update/change times or details for an existing shift on this date. Set to 'REMOVE' if the email asks to delete/remove/cancel an existing shift on this date)",
            "date": "string (YYYY-MM-DD format. Each shift must have its own date if the order spans multiple days)",
            "start_time": "string (HH:MM format, 24-hour)",
            "end_time": "string (HH:MM format, 24-hour. Leave EMPTY if not explicitly stated AND cannot be inferred from typical_shift_times)",
            "end_time_inferred": "boolean (true if end_time was guessed from typical_shift_times rather than explicitly stated in the order)",
            "position": "string (Must exactly match one of the available positions in the Client Context. If vague, like 'Cook', map to the typical position used like 'Cook G' or 'Cook 2')",
            "staff_count": "integer (Number of staff needed for this position)",
            "details": {
                "grooming": "string (Grooming or uniform requirements)",
                "tools": ["string"] (List of required tools),
                "certifications": ["string"] (List of required certifications),
                "publication_rules": "string (e.g. 'Preferred Employees First', 'All Employees', etc.)",
                "requested_employee_name": "string (Name of the employee if explicitly requested for this shift, else null)",
                "requested_employee_id": "integer (ID of the requested employee if matched from preferred_employees in context, else null)"
            }
        }
    ]
}

Guidelines:
- ALWAYS use the exact position names from the Client Context. Do not invent positions.
- If an end time or venue address is missing, return an empty string (""). The UI will highlight these for the user to manually fill.
- Support multi-day orders by creating separate shift_information objects with their respective dates.
- Format all times in 24-hour HH:MM format.
- IMPORTANT TIME PARSING RULES:
  1. For a range like "6-1:30pm" or "6 to 1:30pm", the first number is the start time ("06:00") and the second is the end time ("13:30").
  2. DO NOT treat the second number as a duration.
  3. Logically infer AM vs PM. If an end time is PM and the start time is a small number (e.g. 6, 7, 8, 9, 10, 11), the start time is almost certainly AM.
- DATE PARSING RULE: If multiple shifts are requested in the same email (e.g. "a cook tomorrow at 7am, and a server at 12pm"), ensure they all receive the EXACT same 'date' string. Do not leave subsequent shift dates empty or guess a different date unless explicitly stated.
- SHIFT COUNT DISTRIBUTION RULE: If a request specifies a total number of workers for a position (e.g., '2 bartenders') along with multiple distinct shift times (e.g., '4pm to 12am' and '5:30pm to 1:30am'), this is a request for that total count divided across the listed shifts (e.g., 1 bartender for the 4pm shift and 1 bartender for the 5:30pm shift). Do NOT apply the full count to each shift time (which would result in 4 bartenders total) unless the text explicitly states the count is "each" or "per shift" (e.g., "2 bartenders each shift"). Distribute the total count logically and evenly among the distinct shift times.
- SIMPLE CONFIRMATION RULE: If the order text (or latest email message) is just a simple confirmation, reply, or follow-up that does not contain a new request for shifts (e.g., "Thank you, see you tomorrow!", "Thanks for confirming!", "Looks good!", "see you tomorrow"), you MUST return an empty shift_information list (i.e. "shift_information": []). Do NOT guess, predict, or extract any shifts based on typical times or typical positions in the context if the text is merely acknowledging or confirming without placing a new order request.
- DATE RANGE & RECURRING SHIFTS RULE: If the request specifies a date range (e.g. "June 9 to July 10" or "6/9 to 7/10") along with a repeating pattern or weekdays (e.g. "Monday to Friday", "every Tuesday and Thursday", "daily"), you MUST expand this range and create a SEPARATE shift_information object for EACH and EVERY matching individual date in that range (inclusive of start and end dates). Do NOT just create two shifts for the start and end dates. Determine the correct calendar dates in the range for 2026 (e.g., if starting June 9, 2026 to July 10, 2026, Monday to Friday, you should output separate shifts for June 9, June 10, June 11, June 12, June 15, June 16, etc., all the way through July 10). List each resulting shift as its own object in the shift_information array.
- WEEKDAY HEADERS & MULTI-DAY EXPANSION RULE: If a list of shifts is placed under a weekday range header (e.g. "Tuesday to Friday", "Monday - Wednesday", "Mon-Fri", "Thursday/Friday"), you MUST duplicate and create a separate shift object in shift_information for EACH and EVERY weekday in that range (e.g. for "Tuesday to Friday", create the shifts for Tuesday, Wednesday, Thursday, and Friday).
- SUBJECT WEEK SPECIFICATION RULE: If the email subject specifies a week (e.g. "week of May 25th" or "week of 5/25"), all weekdays mentioned in the body (e.g. "Tuesday to Friday", "Monday") must resolve to calendar dates within that specific week (for the week of May 25, 2026, this resolves to: Monday=2026-05-25, Tuesday=2026-05-26, Wednesday=2026-05-27, Thursday=2026-05-28, Friday=2026-05-29, Saturday=2026-05-30, Sunday=2026-05-31), overriding the general relative calendar context.
- EMAIL THREAD RULE: Emails may contain a history of previous messages (a thread chain) at the bottom. You MUST focus on the latest (top-most) message. Do NOT extract shifts or details from historical messages in the thread UNLESS the top-most message explicitly directs you to do so (e.g., "please repeat the order below", "book the same shifts as below", "see forwarded request"). If the latest message is just a simple confirmation or follow-up that does not request shifts, do not extract any shifts.
- ACTION SELECTION RULE: Identify if the request is to add/create new shifts (CREATE), change details/times of existing shifts (UPDATE), or delete/cancel existing shifts (REMOVE). For example:
  1. "please change the Cook G start time on 6/3/2026 to 8am start time" -> action is "UPDATE", position is "Cook G", date is "2026-06-03", start_time is "08:00".
  2. "please remove the cook G shift on 6/3/2026" -> action is "REMOVE", position is "Cook G", date is "2026-06-03".
  3. "We need a cook G on 6/3/2026 from 7am-3pm" -> action is "CREATE", position is "Cook G", date is "2026-06-03", start_time is "07:00", end_time is "15:00".
- Use the 'Calendar Context (Relative dates)' list provided in the user prompt to resolve relative day names (like 'Friday', 'tomorrow', 'next Monday', 'today') to their correct YYYY-MM-DD calendar dates. Do NOT guess or perform manual date arithmetic.
- Do NOT return markdown formatting (no ```json).
"""

def parse_reference_date(ref_date_str: str) -> datetime:
    """Parse reference date string into a datetime object robustly and convert to Pacific Time."""
    if not ref_date_str:
        return datetime.now(PT)
    try:
        # Standard ISO format with 'Z'
        if ref_date_str.endswith('Z'):
            ref_date_str = ref_date_str[:-1] + '+00:00'
        dt = datetime.fromisoformat(ref_date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt.astimezone(PT)
    except Exception:
        try:
            # Fallback to simple split if it has 'T' or space
            date_part = ref_date_str.split('T')[0].split(' ')[0]
            dt = datetime.strptime(date_part, "%Y-%m-%d")
            return dt.replace(tzinfo=PT)
        except Exception:
            return datetime.now(PT)
def get_latest_email_message(body: str) -> str:
    """Extracts only the latest (top-most) email message from a thread chain."""
    if not body:
        return ""
    lines = body.splitlines()
    latest_lines = []
    for line in lines:
        stripped = line.strip()
        # Outlook horizontal line separator
        if stripped.startswith("___") and all(c == "_" for c in stripped[:10]):
            break
        # Standard email headers or horizontal line separators indicating older emails
        if stripped.startswith("---") and "Original Message" in stripped:
            break
        if stripped.startswith("From:") and len(latest_lines) > 2:
            break
        if stripped.startswith("On ") and stripped.endswith("wrote:") and len(latest_lines) > 2:
            break
        latest_lines.append(line)
    return "\n".join(latest_lines).strip()

def clean_email_thread_if_needed(text: str) -> str:
    """
    Conditionally strips historical thread context from email messages.
    If the top-most reply contains explicit thread references or any digits (dates/times),
    retains the full thread. Otherwise, strips it.
    """
    if not text:
        return ""
    # Check if a thread separator or header is present at all
    has_thread = ("___" in text or "Original Message" in text or "From:" in text or "On " in text)
    if not has_thread:
        return text

    latest_message = get_latest_email_message(text)
    latest_lower = latest_message.lower()
    
    # Check if the latest message explicitly references or forwards the email below.
    # We avoid generic terms like 'tomorrow' or 'today' which frequently appear in confirmations.
    keywords = [
        "below", "forward", "repeat", "underneath", "previous", "same shifts", "copy", "attached",
        "coverage", "schedule", "rebook"
    ]
    
    import re
    has_keyword = any(kw in latest_lower for kw in keywords)
    has_digit = bool(re.search(r'\d', latest_lower))
    
    if has_keyword or has_digit:
        # Keep the full thread
        return text
    
    return latest_message

async def ai_extract_order(text: str, client_context: dict, reference_date: str = None, thread_mode: str = 'latest') -> dict:
    """Run structured AI extraction on order text."""
    if thread_mode == 'latest':
        cleaned_text = clean_email_thread_if_needed(text)
    else:
        cleaned_text = text
    
    if reference_date:
        ref_dt = parse_reference_date(reference_date)
    else:
        ref_dt = datetime.now(PT)
        
    current_date = ref_dt.strftime("%A, %B %d, %Y")
    
    # Generate relative calendar context to prevent LLM calendar math errors
    from datetime import timedelta
    calendar_lines = []
    for i in range(-2, 12):
        dt = ref_dt + timedelta(days=i)
        day_label = dt.strftime("%A")
        date_label = dt.strftime("%Y-%m-%d")
        rel = ""
        if i == 0:
            rel = " (Today)"
        elif i == 1:
            rel = " (Tomorrow)"
        elif i == -1:
            rel = " (Yesterday)"
        calendar_lines.append(f"- {day_label}: {date_label}{rel}")
    calendar_context = "\n".join(calendar_lines)
    
    user_msg = (
        f"Current Date: {current_date}\n"
        f"Calendar Context (Relative dates):\n{calendar_context}\n\n"
        f"Client Context:\n{json.dumps(client_context, indent=2)}\n\n"
        f"Raw Order Text:\n{cleaned_text}\n\n"
        f"Extract the details into the required JSON schema."
    )
    
    try:
        client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)
        response = await client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL_AUTOMATION", "gpt-4.1-nano"),
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            response_format={"type": "json_object"},
            temperature=0.0
        )
        content = response.choices[0].message.content or "{}"
        return json.loads(content)
    except Exception as e:
        print(f"AI Extraction Error: {str(e)}")
        # Return empty structured data on failure
        return {
            "basic_information": {},
            "shift_information": []
        }
