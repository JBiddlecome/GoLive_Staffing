import json
import uuid
from pathlib import Path
from typing import List, Dict, Optional

# Define the data file path within the project root / data directory
def _resolve_data_dir() -> Path:
    import os
    env_dir = os.getenv("DATA_DIR") or os.getenv("RENDER_DISK_PATH")
    if env_dir:
        return Path(env_dir)
    if Path("/var/data").exists():
        return Path("/var/data")
    if any(os.getenv(e) for e in ("RENDER", "RENDER_SERVICE_ID")):
        return Path("/var/data")
    return Path(__file__).resolve().parent.parent.parent / "data"

DATA_FILE = _resolve_data_dir() / "recruiting_ads.json"

def _ensure_data_file() -> None:
    """Ensure the data file exists."""
    if not DATA_FILE.exists():
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        DATA_FILE.write_text(json.dumps([], indent=2))

def load_ads() -> List[Dict]:
    """Load all ads from the JSON file."""
    _ensure_data_file()
    try:
        with DATA_FILE.open('r') as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return []

def save_ads(ads: List[Dict]) -> None:
    """Save the list of ads to the JSON file."""
    _ensure_data_file()
    DATA_FILE.write_text(json.dumps(ads, indent=2))

def add_ad(position: str, region: str, notes: str) -> Dict:
    """Add a new ad and return the created object."""
    ads = load_ads()
    new_ad = {
        "id": str(uuid.uuid4()),
        "position": position,
        "region": region,
        "notes": notes
    }
    ads.append(new_ad)
    save_ads(ads)
    return new_ad

def update_ad(ad_id: str, position: str, region: str, notes: str) -> bool:
    """Update an existing ad. Returns True if successful."""
    ads = load_ads()
    for ad in ads:
        if ad.get("id") == ad_id:
            ad["position"] = position
            ad["region"] = region
            ad["notes"] = notes
            save_ads(ads)
            return True
    return False

def delete_ad(ad_id: str) -> bool:
    """Delete an ad by ID. Returns True if successful."""
    ads = load_ads()
    initial_len = len(ads)
    ads = [ad for ad in ads if ad.get("id") != ad_id]
    if len(ads) < initial_len:
        save_ads(ads)
        return True
    return False
