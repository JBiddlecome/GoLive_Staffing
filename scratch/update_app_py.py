with open(r'c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\app.py', 'r') as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    new_lines.append(line)
    if 'app.include_router(reports_router, prefix="/reports", tags=["Reports"])' in line:
        new_lines.append('app.include_router(client_notifications_router, prefix="/client-notifications", tags=["Client Notifications"])\n')

with open(r'c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\app.py', 'w') as f:
    f.writelines(new_lines)
