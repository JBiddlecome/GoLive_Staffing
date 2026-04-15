import asyncio
import json
import os
import re
from pathlib import Path

import httpx

DATA_FILE = Path("data/position_requests.json")

def load_records():
    if not DATA_FILE.exists():
        return []
    try:
        return json.loads(DATA_FILE.read_text())
    except:
        return []

def save_records(records):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    DATA_FILE.write_text(json.dumps(records, indent=2))

async def fetch_submissions():
    jotform_key = os.getenv("JOTFORM_KEY")
    if not jotform_key:
        print("Missing JOTFORM_KEY for position requests")
        return
        
    form_id = "240575384332053"
    url = f"https://api.jotform.com/form/{form_id}/submissions"
    
    headers = {
        "APIKEY": jotform_key
    }
    
    # We can fetch latest 100 submissions
    params = {
        "limit": 100
    }

    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
            if data.get("responseCode") != 200:
                print("Jotform error:", data.get("message"))
                return
            submissions = data.get("content", [])
        except Exception as e:
            print("Error fetching submissions for position requests:", str(e))
            return

        records = load_records()
        existing_ids = {r.get("message_id") for r in records if r.get("message_id")}
        added = False

        # Process from oldest to newest conceptually if we reverse Jotform's default descending order
        # But actually Jotform sorted newest first. Reversing helps apply oldest to records.insert(0, ...)
        for sub in reversed(submissions):
            sub_id = str(sub.get("id"))
            if sub_id in existing_ids:
                continue
                
            answers = sub.get("answers", {})
            name = "Unknown"
            phone = ""
            positions = ""
            experience = ""
            resume_link = ""

            for key, val in answers.items():
                text_label = val.get("text", "").lower()
                answer_val = val.get("answer", "")
                
                if not answer_val:
                    continue

                if "name" in text_label:
                    if isinstance(answer_val, dict):
                        name = f"{answer_val.get('first', '')} {answer_val.get('last', '')}".strip()
                    else:
                        name = str(answer_val)
                elif "phone" in text_label:
                    if isinstance(answer_val, dict):
                        phone = str(answer_val.get('full', answer_val))
                    else:
                        phone = str(answer_val)
                elif "positions" in text_label:
                    # Depending on Jotform widget, it could be a list or comma string
                    if isinstance(answer_val, list):
                        positions = ", ".join(map(str, answer_val))
                    elif isinstance(answer_val, dict):
                        positions = ", ".join(f"{k}: {v}" for k, v in answer_val.items() if v)
                    else:
                        positions = str(answer_val)
                elif "resume" in text_label:
                    if isinstance(answer_val, list):
                        resume_link = str(answer_val[0]) if answer_val else ""
                    else:
                        resume_link = str(answer_val)
                elif "experience" in text_label:
                    # In case they put a URL directly into the experience text box
                    val_str = str(answer_val).strip()
                    if val_str.startswith("http") and not "\n" in val_str and not " " in val_str:
                        if not resume_link:
                            resume_link = val_str
                    else:
                        experience = val_str

            new_record = {
                "message_id": sub_id,  # Using submission id as message_id for compatibility
                "employee_name": name,
                "phone": phone,
                "positions": positions[:500],
                "experience": experience,
                "resume_link": resume_link,
                "recruiter": "",
                "completed": False,
                "received_date": sub.get("created_at", "")
            }
            records.insert(0, new_record)
            added = True
            
        if added:
            save_records(records)

async def position_requests_monitoring_loop():
    while True:
        try:
            await fetch_submissions()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print("Error in position_requests_monitoring_loop:", str(e))
        
        await asyncio.sleep(60 * 15)  # 15 minutes
