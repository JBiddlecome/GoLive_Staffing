# GoLive Staffing — Tools

Single FastAPI web app hosting multiple internal tools:
RECRUITING
-Clickboarding Check: Match Clickboarding candidates who have completed onboarding with Active employees in GoLive.
-Text Blast Filter: Upload either Available Employees List or Employee List, select postions, statuses, or counties and output a formatted datasheet that can be used for SMS text blasts or other communication.
-Employee Phone & County Audit: Upload an Employee List report and find any employee with incorrect phone number or county

HUMAN RESOURCES
-Health Benefits: Upload an Employee List and select a date range for benefits. The webs app will determine which employees have worked over 360 hours the following 3 months after 30 days of employment.

PAYROLL
-UCLA Hours Tool: Upload Payroll Report and Assignment List. Output the Agile1 UCLA Timesheet or determine if any employee needs an ID assigned.



## Local Dev

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app:app --reload
