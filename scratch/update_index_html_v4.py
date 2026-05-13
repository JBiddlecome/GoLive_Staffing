import re
import sys

file_path = r'c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\templates\index.html'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

new_link = """            <li>
              <a href="/client-notifications"
                class="flex items-start justify-between gap-3 px-6 py-4 hover:bg-white transition">
                <div>
                  <p class="font-semibold text-indigo-600">Client Notifications</p>
                  <p class="text-sm text-gray-600">Monthly activity reviews and performance summaries for clients.</p>
                </div>
                <span class="text-indigo-600">↗</span>
              </a>
            </li>"""

# Match the reports link in the staffing section
# We look for "Staffing Tools" or "id=\"staffing-tools\"" first
staffing_pos = content.find('id="staffing-tools"')
if staffing_pos == -1:
    print("Staffing tools section not found")
    sys.exit(1)

# Find the next /reports link after that
reports_pos = content.find('href="/reports"', staffing_pos)
if reports_pos == -1:
    print("/reports link not found")
    sys.exit(1)

# Find the closing </li>
li_end = content.find('</li>', reports_pos)
if li_end == -1:
    print("</li> not found")
    sys.exit(1)

insertion_point = li_end + 5
new_content = content[:insertion_point] + "\n" + new_link + content[insertion_point:]

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(new_content)
print("Success")
