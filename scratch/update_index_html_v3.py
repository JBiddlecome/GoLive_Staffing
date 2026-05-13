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

target = 'href="/reports"'
if target in content:
    # Find the end of the </li> that contains this href
    pos = content.find(target)
    li_end = content.find('</li>', pos)
    if li_end != -1:
        insertion_point = li_end + 5
        new_content = content[:insertion_point] + "\n" + new_link + content[insertion_point:]
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print("Success")
    else:
        print("li_end not found")
else:
    print("target not found")
