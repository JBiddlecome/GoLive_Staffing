# GoLive Staffing — Tools

Single FastAPI web app hosting multiple internal tools:
- Health Benefits
- UCLA Hours Tool
- Text Blast Filter

## Local Dev

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app:app --reload
