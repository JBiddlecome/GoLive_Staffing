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

SYSTEM_PROMPT = """You are an expert Staffing Manager assistant for GoLive Staffing.
Your job is to extract shift and event order details from raw text (emails or dictations) and map them to our database schema.
You will be provided with a 'Client Context' containing their typical ordering habits and available positions.

Return ONLY a valid JSON object matching this exact schema:
{
    "basic_information": {
        "client_name": "string (Fuzzy match to the client context if possible)",
        "client_id": "integer (From client context)",
        "event_name": "string (The title of the event, default to the venue name, e.g. 'Convention Center', if not specified. NEVER default to the client name.)",
        "venue_name": "string (The location. Predict based on client context if missing, but if completely ambiguous or multiple venues exist, leave it EMPTY string so the UI can flag it)",
        "purchase_order": "string (If specified)"
    },
    "shift_information": [
        {
            "id": "integer (incrementing ID starting from 1)",
            "date": "string (YYYY-MM-DD format. Each shift must have its own date if the order spans multiple days)",
            "start_time": "string (HH:MM format, 24-hour)",
            "end_time": "string (HH:MM format, 24-hour. Leave EMPTY if not explicitly stated)",
            "position": "string (Must exactly match one of the available positions in the Client Context. If vague, like 'Cook', map to the typical position used like 'Cook G' or 'Cook 2')",
            "staff_count": "integer (Number of staff needed for this position)",
            "details": {
                "grooming": "string (Grooming or uniform requirements)",
                "tools": ["string"] (List of required tools),
                "certifications": ["string"] (List of required certifications),
                "publication_rules": "string (e.g. 'Preferred Employees First', 'All Employees', etc.)"
            }
        }
    ]
}

Guidelines:
- ALWAYS use the exact position names from the Client Context. Do not invent positions.
- If an end time or venue address is missing, return an empty string (""). The UI will highlight these for the user to manually fill.
- Support multi-day orders by creating separate shift_information objects with their respective dates.
- Format all times in 24-hour HH:MM format.
- Do NOT return markdown formatting (no ```json).
"""

async def ai_extract_order(text: str, client_context: dict) -> dict:
    """Run structured AI extraction on order text."""
    current_date = datetime.now().strftime("%A, %B %d, %Y")
    user_msg = (
        f"Current Date: {current_date}\n\n"
        f"Client Context:\n{json.dumps(client_context, indent=2)}\n\n"
        f"Raw Order Text:\n{text}\n\n"
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
