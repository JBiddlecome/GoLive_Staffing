from __future__ import annotations

import io
import os
import re
import zipfile
from datetime import date, datetime, timedelta
from typing import Dict

import pandas as pd
from fastapi import APIRouter, Request, UploadFile, File, Query
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pypdf import PdfReader, PdfWriter
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

templates = Jinja2Templates(directory="templates")
router = APIRouter()


def _normalise_key(value: str) -> str:
    """Create a simplified key for column lookups."""

    return re.sub(r"[^a-z0-9]", "", value.lower())


def _lookup_column(columns: Dict[str, str], *candidates: str) -> str:
    for candidate in candidates:
        key = _normalise_key(candidate)
        if key in columns:
            return columns[key]
    raise ValueError(f"Could not find required column. Looked for one of: {', '.join(candidates)}")


def _clean_numeric(series: pd.Series) -> pd.Series:
    return (
        pd.to_numeric(series.astype(str).str.replace(r"[^0-9.\-]", "", regex=True), errors="coerce")
        .fillna(0)
    )


def _normalise_name(value: str) -> str:
    return re.sub(r"\s+", " ", str(value).strip()).upper()


async def _read_excel(upload: UploadFile) -> pd.DataFrame:
    try:
        payload = await upload.read()
        return pd.read_excel(io.BytesIO(payload))
    except Exception as exc:  # pragma: no cover - defensive
        raise ValueError(f"Unable to read Excel file '{upload.filename}'.") from exc


def _prepare_ucla_hours(payroll_df: pd.DataFrame, assignments_df: pd.DataFrame) -> tuple[pd.DataFrame, date]:
    payroll_columns = {_normalise_key(col): col for col in payroll_df.columns}
    assignments_columns = {_normalise_key(col): col for col in assignments_df.columns}

    client_col = _lookup_column(payroll_columns, "Client", "Client Name")
    reg_hours_col = _lookup_column(payroll_columns, "Reg H (e)", "Regular Hours")
    ot_hours_col = _lookup_column(payroll_columns, "OT H (e)", "Overtime Hours")
    dt_hours_col = _lookup_column(payroll_columns, "DT H (e)", "Doubletime Hours")
    first_name_col = _lookup_column(payroll_columns, "First Name")
    last_name_col = _lookup_column(payroll_columns, "Last Name")
    payroll_rate_col = _lookup_column(payroll_columns, "Pay Rate")

    assign_no_col = _lookup_column(assignments_columns, "Assign No", "Assignment #", "Assignment Number")
    assign_name_col = _lookup_column(assignments_columns, "Full Name", "Employee Name")
    assign_rate_col = _lookup_column(assignments_columns, "Pay Rate")

    # Filter payroll to UCLA clients only
    payroll_df = payroll_df[
        payroll_df[client_col].astype(str).str.contains("UCLA", case=False, na=False)
    ].copy()

    if payroll_df.empty:
        raise ValueError("No UCLA records found in the payroll spreadsheet.")

    for column in (reg_hours_col, ot_hours_col, dt_hours_col, payroll_rate_col):
        payroll_df[column] = _clean_numeric(payroll_df[column])

    payroll_df["Employee Name"] = (
        payroll_df[first_name_col].fillna("").astype(str).str.strip()
        + " "
        + payroll_df[last_name_col].fillna("").astype(str).str.strip()
    ).str.replace(r"\s+", " ", regex=True).str.strip()
    payroll_df["Employee Key"] = payroll_df["Employee Name"].map(_normalise_name)
    payroll_df = payroll_df[payroll_df["Employee Key"] != ""].copy()
    payroll_df["Total Hours"] = (
        payroll_df[reg_hours_col] + payroll_df[ot_hours_col] + payroll_df[dt_hours_col]
    )
    payroll_df["Pay Rate Clean"] = payroll_df[payroll_rate_col].round(2)

    grouped = (
        payroll_df.groupby(["Employee Key", "Pay Rate Clean"], as_index=False)
        .agg({"Total Hours": "sum", "Employee Name": "first"})
        .rename(columns={"Total Hours": "Hours"})
    )
    grouped["Hours"] = grouped["Hours"].round(2)

    assignments_df = assignments_df.copy()
    assignments_df["Employee Name"] = (
        assignments_df[assign_name_col]
        .astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )
    assignments_df["Employee Key"] = assignments_df["Employee Name"].map(_normalise_name)
    assignments_df["Pay Rate Clean"] = _clean_numeric(assignments_df[assign_rate_col]).round(2)
    assignments_df = assignments_df.dropna(subset=["Employee Key", "Pay Rate Clean"])
    assignments_df = assignments_df[assignments_df["Employee Key"] != ""].copy()
    assignments_df = assignments_df.drop_duplicates(subset=["Employee Key", "Pay Rate Clean"])

    merged = grouped.merge(
        assignments_df[["Employee Key", "Pay Rate Clean", assign_no_col]],
        on=["Employee Key", "Pay Rate Clean"],
        how="left",
    )

    missing_assignments = merged[merged[assign_no_col].isna()]
    if not missing_assignments.empty:
        details = []
        for name, rate in zip(
            missing_assignments["Employee Name"],
            missing_assignments["Pay Rate Clean"],
        ):
            if pd.notna(rate):
                details.append(f"{name} @ {rate:.2f}")
            else:
                details.append(str(name))
        raise ValueError("Missing assignment numbers for: " + ", ".join(details))

    merged = merged.rename(columns={assign_no_col: "Assignment #"})
    merged = merged.sort_values(["Assignment #", "Employee Name"]).reset_index(drop=True)

    today = datetime.now().date()
    most_recent_sunday = today - timedelta(days=(today.weekday() + 1) % 7)
    work_date_str = most_recent_sunday.strftime("%m/%d/%Y")
    id_prefix = most_recent_sunday.strftime("%Y%m%d")

    merged["Pay Rate"] = merged["Pay Rate Clean"].round(2)
    merged["Hours"] = merged["Hours"].round(2)
    merged["Work Date"] = work_date_str
    merged["Weekending Date"] = work_date_str
    merged["Unique Line ID"] = [f"{id_prefix}{i:04d}" for i in range(1, len(merged) + 1)]

    return (
        merged[
            [
                "Assignment #",
                "Employee Name",
                "Pay Rate",
                "Work Date",
                "Weekending Date",
                "Hours",
                "Unique Line ID",
            ]
        ],
        most_recent_sunday,
    )


@router.get("")
async def page(request: Request):
    return templates.TemplateResponse("apps/ucla_hours_tool.html", {"request": request})


@router.post("/upload")
async def upload(
    request: Request,
    employee_list: UploadFile = File(...),
    payroll: UploadFile = File(...),
):
    try:
        payroll_df = await _read_excel(payroll)
        assignments_df = await _read_excel(employee_list)
        output_df, sunday = _prepare_ucla_hours(payroll_df, assignments_df)
    except ValueError as exc:
        return templates.TemplateResponse(
            "apps/ucla_hours_tool.html",
            {"request": request, "error": str(exc)},
        )
    except Exception:  # pragma: no cover - defensive
        return templates.TemplateResponse(
            "apps/ucla_hours_tool.html",
            {
                "request": request,
                "error": "An unexpected error occurred while processing the workbooks.",
            },
            status_code=500,
        )

    buffer = io.BytesIO()
    # Use the default pandas engine (openpyxl) so that we do not require the optional
    # ``xlsxwriter`` dependency at runtime.
    with pd.ExcelWriter(buffer) as writer:
        output_df.to_excel(writer, index=False, sheet_name="UCLA Hours")
    buffer.seek(0)

    filename = f"ucla_hours_{sunday.strftime('%Y%m%d')}.xlsx"
    headers = {"Content-Disposition": f"attachment; filename=\"{filename}\""}
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@router.post("/split-pdf")
async def split_pdf(request: Request, pdf_file: UploadFile = File(...)):
    try:
        # Extract employee name from filename
        filename = pdf_file.filename
        # Remove date pattern and extension
        name_part = re.sub(r'-\d{2}-\d{2}-\d{4}.*$', '', filename, flags=re.IGNORECASE)
        name_part = re.sub(r'\.pdf$', '', name_part, flags=re.IGNORECASE)
        employee_name = name_part.replace('-', ' ')
        
        pdf_bytes = await pdf_file.read()
        reader = PdfReader(io.BytesIO(pdf_bytes))
        
        if len(reader.pages) < 6:
            raise ValueError("The uploaded PDF must have at least 6 pages.")
            
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            # General: pages 1-2
            writer_gen = PdfWriter()
            writer_gen.add_page(reader.pages[0])
            writer_gen.add_page(reader.pages[1])
            gen_buffer = io.BytesIO()
            writer_gen.write(gen_buffer)
            zip_file.writestr(f"{employee_name} - UCLA Acknowledgment Letter (General).pdf", gen_buffer.getvalue())
            
            # Server: pages 3-4
            writer_srv = PdfWriter()
            writer_srv.add_page(reader.pages[2])
            writer_srv.add_page(reader.pages[3])
            srv_buffer = io.BytesIO()
            writer_srv.write(srv_buffer)
            zip_file.writestr(f"{employee_name} - UCLA Acknowledgment Letter (Server).pdf", srv_buffer.getvalue())
            
            # Cook: pages 5-6
            writer_cook = PdfWriter()
            writer_cook.add_page(reader.pages[4])
            writer_cook.add_page(reader.pages[5])
            cook_buffer = io.BytesIO()
            writer_cook.write(cook_buffer)
            zip_file.writestr(f"{employee_name} - UCLA Acknowledgment Letter (Cook).pdf", cook_buffer.getvalue())
            
        zip_buffer.seek(0)
        
        headers = {"Content-Disposition": f"attachment; filename=\"{employee_name} UCLA Acknowledgment Letters.zip\""}
        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers=headers
        )
        
    except ValueError as exc:
        return templates.TemplateResponse(
            "apps/ucla_hours_tool.html",
            {"request": request, "error": str(exc)},
        )
    except Exception as e:
        return templates.TemplateResponse(
            "apps/ucla_hours_tool.html",
            {
                "request": request,
                "error": "An unexpected error occurred while splitting the PDF.",
            },
            status_code=500,
        )


def _db_url_from_env() -> URL:
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))
    return URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name,
    )


def _engine():
    return create_engine(_db_url_from_env(), pool_pre_ping=True)


def _fetch_estimates_data(start_date: str, end_date: str) -> list[dict]:
    engine = _engine()
    sql = text("""
        SELECT 
            e.date AS event_date,
            c.name AS client_name,
            v.name AS venue_name,
            p.description AS position_name,
            s.start AS shift_start,
            s.end AS shift_end,
            sp.count AS position_count,
            sp.shift_position_id,
            sp.bill_rate AS default_bill_rate,
            se.shift_employee_id,
            se.bill_rate AS employee_bill_rate,
            se.confirmed,
            CONCAT(emp.first_name, ' ', emp.last_name) AS employee_name
        FROM event e
        JOIN client c ON e.client_id = c.client_id
        JOIN venue v ON e.venue_id = v.venue_id
        JOIN shift s ON s.event_id = e.event_id
        JOIN shift_position sp ON sp.shift_id = s.shift_id
        JOIN position p ON sp.position_id = p.position_id
        LEFT JOIN shift_employee se ON se.shift_position_id = sp.shift_position_id 
            AND se.deleted_at IS NULL 
            AND se.cancel_reason = 0
        LEFT JOIN employee emp ON se.employee_id = emp.employee_id 
            AND emp.deleted_at IS NULL
        WHERE c.client_id IN (345, 1710, 205, 1399)
          AND e.date BETWEEN :start_date AND :end_date
          AND e.deleted_at IS NULL
          AND c.deleted_at IS NULL
          AND v.deleted_at IS NULL
          AND s.deleted_at IS NULL
          AND sp.deleted_at IS NULL
        ORDER BY e.date ASC, s.start ASC, p.description ASC, emp.first_name ASC, emp.last_name ASC
    """)
    
    with engine.connect() as conn:
        results = conn.execute(sql, {"start_date": start_date, "end_date": end_date}).mappings().all()
        
    groups = {}
    for row in results:
        sp_id = row['shift_position_id']
        if sp_id not in groups:
            groups[sp_id] = {
                'event_date': row['event_date'],
                'client_name': row['client_name'],
                'venue_name': row['venue_name'],
                'position_name': row['position_name'],
                'shift_start': row['shift_start'],
                'shift_end': row['shift_end'],
                'position_count': row['position_count'],
                'default_bill_rate': row['default_bill_rate'],
                'employees': []
            }
        if row['employee_name'] is not None:
            groups[sp_id]['employees'].append({
                'name': row['employee_name'],
                'bill_rate': row['employee_bill_rate'],
                'confirmed': row['confirmed']
            })
    
    report_rows = []
    for sp_id, g in groups.items():
        start_dt = g['shift_start']
        end_dt = g['shift_end']
        hours = 0.0
        if start_dt and end_dt:
            diff_hours = (end_dt - start_dt).total_seconds() / 3600.0
            if diff_hours > 5.0:
                diff_hours -= 0.5
            hours = round(diff_hours, 2)
        
        # Filled slots
        for emp in g['employees']:
            rate = float(emp['bill_rate']) if (emp['bill_rate'] is not None and emp['bill_rate'] > 0) else float(g['default_bill_rate'] or 0)
            amount = round(hours * rate, 2)
            
            report_rows.append({
                'date': g['event_date'].strftime('%Y-%m-%d') if g['event_date'] else '',
                'client': g['client_name'],
                'venue': g['venue_name'],
                'position': g['position_name'],
                'start_time': start_dt.strftime('%I:%M %p') if start_dt else '',
                'end_time': end_dt.strftime('%I:%M %p') if end_dt else '',
                'employee': emp['name'],
                'hours': hours,
                'bill_rate': rate,
                'amount': amount,
                'filled': True
            })
        
        # Unfilled slots
        unfilled_count = max(0, g['position_count'] - len(g['employees']))
        for _ in range(unfilled_count):
            rate = float(g['default_bill_rate'] or 0)
            amount = round(hours * rate, 2)
            
            report_rows.append({
                'date': g['event_date'].strftime('%Y-%m-%d') if g['event_date'] else '',
                'client': g['client_name'],
                'venue': g['venue_name'],
                'position': g['position_name'],
                'start_time': start_dt.strftime('%I:%M %p') if start_dt else '',
                'end_time': end_dt.strftime('%I:%M %p') if end_dt else '',
                'employee': '',
                'hours': hours,
                'bill_rate': rate,
                'amount': amount,
                'filled': False
            })
            
    return report_rows


@router.get("/estimates")
async def get_ucla_estimates(
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    try:
        data = _fetch_estimates_data(start_date, end_date)
        return {"data": data}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/estimates/download")
async def download_ucla_estimates(
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    try:
        data = _fetch_estimates_data(start_date, end_date)
        
        # Build pandas DataFrame for Excel export
        report_rows = []
        for r in data:
            report_rows.append({
                'Event Date': r['date'],
                'Client Name': r['client'],
                'Venue Name': r['venue'],
                'Position': r['position'],
                'Start Time': r['start_time'],
                'End Time': r['end_time'],
                'Employee Name': r['employee'],
                'Est. Hours': r['hours'],
                'Bill Rate': r['bill_rate'],
                'Total Bill Amount': r['amount']
            })
            
        df = pd.DataFrame(report_rows)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer) as writer:
            df.to_excel(writer, index=False, sheet_name="UCLA Estimates")
        buffer.seek(0)
        
        filename = f"ucla_estimates_{start_date}_to_{end_date}.xlsx"
        headers = {"Content-Disposition": f"attachment; filename=\"{filename}\""}
        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


def _generate_estimates_pdf(data: list[dict], start_date: str, end_date: str) -> io.BytesIO:
    import fitz
    doc = fitz.open()
    
    total_hours = sum(r['hours'] for r in data)
    total_amount = sum(r['amount'] for r in data)
    
    col_x = [40, 95, 275, 365, 475, 580, 635, 680, 752]
    headers = ["Date", "Venue", "Position", "Times", "Employee", "Est. Hours", "Rate", "Amount"]
    alignments = [0, 0, 0, 0, 0, 2, 2, 2] # 0=left, 1=center, 2=right
    
    def add_page_with_headers(page_num):
        page = doc.new_page(width=792, height=612)
        
        # Header text
        page.insert_text((40, 32), "Culinary Staffing Services", fontsize=14, fontname="hebo", color=(0.1, 0.45, 0.3))
        page.insert_text((40, 46), f"UCLA Shift Estimates: {start_date} to {end_date}", fontsize=9, fontname="helv", color=(0.3, 0.3, 0.3))
        page.insert_text((40, 58), "Note: Hours are estimated. Shifts over 5 hours deduct 30 minutes for a meal break.", fontsize=7.5, fontname="helv", color=(0.5, 0.5, 0.5))
        
        # Horizontal rule
        page.draw_line((40, 64), (752, 64), color=(0.8, 0.8, 0.8), width=1)
        
        # Table headers
        hy = 77
        for idx, h in enumerate(headers):
            rect = fitz.Rect(col_x[idx], hy - 10, col_x[idx+1], hy + 12)
            page.insert_textbox(rect, h, fontsize=8, fontname="hebo", align=alignments[idx], color=(0.2, 0.2, 0.2))
            
        page.draw_line((40, hy + 14), (752, hy + 14), color=(0.6, 0.6, 0.6), width=1)
        
        # Page number footer
        page.insert_text((710, 585), f"Page {page_num}", fontsize=8, fontname="helv", color=(0.5, 0.5, 0.5))
        return page

    page_num = 1
    page = add_page_with_headers(page_num)
    y = 105
    row_height = 32
    
    for row_idx, r in enumerate(data):
        if y + row_height > 560:
            page_num += 1
            page = add_page_with_headers(page_num)
            y = 105
            
        # Draw cells
        # Date
        rect = fitz.Rect(col_x[0], y - 6, col_x[1] - 4, y + 10)
        page.insert_textbox(rect, r['date'], fontsize=8, fontname="helv", align=0)
        
        # Venue Name
        rect = fitz.Rect(col_x[1], y - 6, col_x[2] - 4, y + 26)
        page.insert_textbox(rect, r['venue'], fontsize=8, fontname="helv", align=0)
        
        # Position
        rect = fitz.Rect(col_x[2], y - 6, col_x[3] - 4, y + 10)
        page.insert_textbox(rect, r['position'], fontsize=8, fontname="helv", align=0)
        
        # Times (drawn as stacked times in column 3)
        rect = fitz.Rect(col_x[3], y - 6, col_x[4] - 4, y + 26)
        times_text = f"{r['start_time']} -\n{r['end_time']}"
        page.insert_textbox(rect, times_text, fontsize=8, fontname="helv", align=0)
        
        # Employee
        rect = fitz.Rect(col_x[4], y - 6, col_x[5] - 4, y + 10)
        emp_text = r['employee'] if r['employee'] else "Unfilled"
        emp_color = (0.2, 0.2, 0.2) if r['employee'] else (0.8, 0.4, 0.0)
        page.insert_textbox(rect, emp_text, fontsize=8, fontname="hebo" if not r['employee'] else "helv", align=0, color=emp_color)
        
        # Hours
        rect = fitz.Rect(col_x[5], y - 6, col_x[6] - 4, y + 10)
        page.insert_textbox(rect, f"{r['hours']:.2f}", fontsize=8, fontname="helv", align=2)
        
        # Bill Rate
        rect = fitz.Rect(col_x[6], y - 6, col_x[7] - 4, y + 10)
        page.insert_textbox(rect, f"${r['bill_rate']:.2f}", fontsize=8, fontname="helv", align=2)
        
        # Amount
        rect = fitz.Rect(col_x[7], y - 6, col_x[8], y + 10)
        page.insert_textbox(rect, f"${r['amount']:.2f}", fontsize=8, fontname="hebo", align=2, color=(0.05, 0.4, 0.2))
        
        # Draw soft divider line
        page.draw_line((40, y + 25), (752, y + 25), color=(0.93, 0.93, 0.93), width=0.5)
        y += row_height
        
    if y + 25 > 560:
        page_num += 1
        page = add_page_with_headers(page_num)
        y = 105
        
    page.draw_line((40, y - 12), (752, y - 12), color=(0.2, 0.2, 0.2), width=1)
    page.draw_line((40, y - 10), (752, y - 10), color=(0.2, 0.2, 0.2), width=1)
    
    rect = fitz.Rect(col_x[4], y - 8, col_x[5] - 4, y + 10)
    page.insert_textbox(rect, "Totals:", fontsize=9, fontname="hebo", align=0)
    
    rect = fitz.Rect(col_x[5], y - 8, col_x[6] - 4, y + 10)
    page.insert_textbox(rect, f"{total_hours:.2f}", fontsize=9, fontname="hebo", align=2)
    
    rect = fitz.Rect(col_x[7], y - 8, col_x[8], y + 10)
    page.insert_textbox(rect, f"${total_amount:.2f}", fontsize=9, fontname="hebo", align=2, color=(0.05, 0.4, 0.2))
    
    page.draw_line((40, y + 14), (752, y + 14), color=(0.2, 0.2, 0.2), width=1)
    page.draw_line((40, y + 16), (752, y + 16), color=(0.2, 0.2, 0.2), width=1)
    
    total_pages = doc.page_count
    for page_idx in range(total_pages):
        p = doc[page_idx]
        p.insert_text((740, 585), f"/ {total_pages}", fontsize=8, fontname="helv", color=(0.5, 0.5, 0.5))
        
    buffer = io.BytesIO()
    doc.save(buffer)
    doc.close()
    buffer.seek(0)
    return buffer


@router.get("/estimates/download-pdf")
async def download_ucla_estimates_pdf(
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    try:
        data = _fetch_estimates_data(start_date, end_date)
        pdf_buffer = _generate_estimates_pdf(data, start_date, end_date)
        
        filename = f"ucla_estimates_{start_date}_to_{end_date}.pdf"
        headers = {"Content-Disposition": f"attachment; filename=\"{filename}\""}
        return StreamingResponse(
            pdf_buffer,
            media_type="application/pdf",
            headers=headers,
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

