import json
from pathlib import Path
from typing import Dict

# Define the data file path within the project root / data directory
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_FILE = REPO_ROOT / "data" / "staffing_dashboard_notes.json"

def _ensure_data_file() -> None:
    """Ensure the data file exists."""
    if not DATA_FILE.exists():
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        DATA_FILE.write_text(json.dumps({}, indent=2))

def load_notes() -> Dict[str, str]:
    """Load all notes from the JSON file."""
    _ensure_data_file()
    try:
        with DATA_FILE.open('r') as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}

def save_notes(notes: Dict[str, str]) -> None:
    """Save the notes to the JSON file."""
    _ensure_data_file()
    DATA_FILE.write_text(json.dumps(notes, indent=2))

def update_note(client_id: str, note: str) -> bool:
    """Update an existing note. Returns True if successful."""
    notes = load_notes()
    notes[str(client_id)] = note
    save_notes(notes)
    return True
