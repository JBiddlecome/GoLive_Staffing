with open(r'c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\app.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    new_lines.append(line)
    if 'from apps.new_employee_position_approver.scheduler import new_employee_position_approver_loop' in line:
        new_lines.append('from apps.client_notifications.scheduler import client_notifications_loop\n')
    if 'nepa_task = asyncio.create_task(new_employee_position_approver_loop())' in line:
        new_lines.append('    cn_task = asyncio.create_task(client_notifications_loop())\n')
    if 'nepa_task.cancel()' in line:
        new_lines.append('    cn_task.cancel()\n')

with open(r'c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\app.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
