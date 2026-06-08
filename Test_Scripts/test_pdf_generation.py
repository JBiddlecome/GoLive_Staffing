import io
import fitz

def _generate_estimates_pdf(data: list[dict], start_date: str, end_date: str):
    doc = fitz.open()
    
    total_hours = sum(r['hours'] for r in data)
    total_amount = sum(r['amount'] for r in data)
    
    col_x = [40, 100, 260, 360, 485, 590, 630, 675, 752]
    headers = ["Date", "Client & Venue", "Position", "Times", "Employee", "Hours", "Rate", "Amount"]
    alignments = [0, 0, 0, 0, 0, 2, 2, 2] # 0=left, 1=center, 2=right
    
    def add_page_with_headers(page_num):
        page = doc.new_page(width=792, height=612)
        
        # Header text
        page.insert_text((40, 35), "Culinary Staffing Services", fontsize=14, fontname="hebo", color=(0.1, 0.45, 0.3))
        page.insert_text((40, 52), f"UCLA Shift Estimates: {start_date} to {end_date}", fontsize=9, fontname="helv", color=(0.3, 0.3, 0.3))
        
        # Horizontal rule
        page.draw_line((40, 60), (752, 60), color=(0.8, 0.8, 0.8), width=1)
        
        # Table headers
        hy = 73
        for idx, h in enumerate(headers):
            rect = fitz.Rect(col_x[idx], hy - 8, col_x[idx+1], hy + 8)
            page.insert_textbox(rect, h, fontsize=8, fontname="hebo", align=alignments[idx], color=(0.2, 0.2, 0.2))
            
        page.draw_line((40, hy + 10), (752, hy + 10), color=(0.6, 0.6, 0.6), width=1)
        
        # Page number footer
        page.insert_text((710, 585), f"Page {page_num}", fontsize=8, fontname="helv", color=(0.5, 0.5, 0.5))
        return page

    page_num = 1
    page = add_page_with_headers(page_num)
    y = 98
    row_height = 24
    
    for row_idx, r in enumerate(data):
        if y + row_height > 560:
            page_num += 1
            page = add_page_with_headers(page_num)
            y = 98
            
        # Draw cells
        # Date
        rect = fitz.Rect(col_x[0], y - 8, col_x[1] - 4, y + 14)
        page.insert_textbox(rect, r['date'], fontsize=8, fontname="helv", align=0)
        
        # Client & Venue
        rect = fitz.Rect(col_x[1], y - 8, col_x[2] - 4, y + 14)
        client_venue_text = f"{r['client']}\n{r['venue']}"
        page.insert_textbox(rect, client_venue_text, fontsize=8, fontname="helv", align=0)
        
        # Position
        rect = fitz.Rect(col_x[2], y - 8, col_x[3] - 4, y + 14)
        page.insert_textbox(rect, r['position'], fontsize=8, fontname="helv", align=0)
        
        # Times
        rect = fitz.Rect(col_x[3], y - 8, col_x[4] - 4, y + 14)
        times_text = f"{r['start_time']} -\n{r['end_time']}"
        page.insert_textbox(rect, times_text, fontsize=8, fontname="helv", align=0)
        
        # Employee
        rect = fitz.Rect(col_x[4], y - 8, col_x[5] - 4, y + 14)
        emp_text = r['employee'] if r['employee'] else "Unfilled"
        emp_color = (0.2, 0.2, 0.2) if r['employee'] else (0.8, 0.4, 0.0)
        page.insert_textbox(rect, emp_text, fontsize=8, fontname="hebo" if not r['employee'] else "helv", align=0, color=emp_color)
        
        # Hours
        rect = fitz.Rect(col_x[5], y - 8, col_x[6] - 4, y + 14)
        page.insert_textbox(rect, f"{r['hours']:.2f}", fontsize=8, fontname="helv", align=2)
        
        # Bill Rate
        rect = fitz.Rect(col_x[6], y - 8, col_x[7] - 4, y + 14)
        page.insert_textbox(rect, f"${r['bill_rate']:.2f}", fontsize=8, fontname="helv", align=2)
        
        # Amount
        rect = fitz.Rect(col_x[7], y - 8, col_x[8], y + 14)
        page.insert_textbox(rect, f"${r['amount']:.2f}", fontsize=8, fontname="hebo", align=2, color=(0.05, 0.4, 0.2))
        
        # Draw soft divider line
        page.draw_line((40, y + 15), (752, y + 15), color=(0.93, 0.93, 0.93), width=0.5)
        y += row_height
        
    if y + 25 > 560:
        page_num += 1
        page = add_page_with_headers(page_num)
        y = 98
        
    page.draw_line((40, y - 5), (752, y - 5), color=(0.2, 0.2, 0.2), width=1)
    page.draw_line((40, y - 3), (752, y - 3), color=(0.2, 0.2, 0.2), width=1)
    
    rect = fitz.Rect(col_x[4], y - 8, col_x[5] - 4, y + 14)
    page.insert_textbox(rect, "Totals:", fontsize=9, fontname="hebo", align=0)
    
    rect = fitz.Rect(col_x[5], y - 8, col_x[6] - 4, y + 14)
    page.insert_textbox(rect, f"{total_hours:.2f}", fontsize=9, fontname="hebo", align=2)
    
    rect = fitz.Rect(col_x[7], y - 8, col_x[8], y + 14)
    page.insert_textbox(rect, f"${total_amount:.2f}", fontsize=9, fontname="hebo", align=2, color=(0.05, 0.4, 0.2))
    
    page.draw_line((40, y + 12), (752, y + 12), color=(0.2, 0.2, 0.2), width=1)
    page.draw_line((40, y + 14), (752, y + 14), color=(0.2, 0.2, 0.2), width=1)
    
    total_pages = doc.page_count
    for page_idx in range(total_pages):
        p = doc[page_idx]
        p.insert_text((740, 585), f"/ {total_pages}", fontsize=8, fontname="helv", color=(0.5, 0.5, 0.5))
        
    doc.save("test_report_multi_page.pdf")
    doc.close()
    print("Multi-page PDF generated successfully.")

# Mock data (30 records, spans 2 pages)
data = [{
    'date': '2026-05-14',
    'client': 'UCLA Luskin Center',
    'venue': 'Banquets (FOH)',
    'position': 'Server 2',
    'start_time': '04:30 AM',
    'end_time': '01:00 PM',
    'employee': f'Employee {i}' if i % 2 == 0 else '',
    'hours': 8.5,
    'bill_rate': 48.0,
    'amount': 408.0
} for i in range(30)]

_generate_estimates_pdf(data, '2026-05-14', '2026-05-14')
