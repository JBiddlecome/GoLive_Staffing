with open(r'c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\templates\index.html', 'r') as f:
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

if 'href="/reports"' in content:
    # Find the closing </li> after /reports in the staffing tools section
    staffing_split = content.split('id="staffing-tools"')
    if len(staffing_split) > 1:
        staffing_part = staffing_split[1]
        reports_split = staffing_part.split('href="/reports"')
        if len(reports_split) > 1:
            reports_part = reports_split[1]
            li_end_index = reports_part.find('</li>') + 5
            
            # Reconstruct the whole file
            # content = before_staffing + "id=\"staffing-tools\"" + staffing_before_reports + "href=\"/reports\"" + reports_before_li_end + "</li>" + NEW_LINK + staffing_after_li_end
            
            before_staffing = staffing_split[0]
            staffing_before_reports = reports_split[0]
            reports_before_li_end = reports_part[:li_end_index]
            staffing_after_li_end = reports_part[li_end_index:]
            
            new_content = before_staffing + 'id="staffing-tools"' + staffing_before_reports + 'href="/reports"' + reports_before_li_end + "\n" + new_link + staffing_after_li_end
            
            with open(r'c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\templates\index.html', 'w') as f:
                f.write(new_content)
            print("Successfully updated index.html")
        else:
            print("Could not find /reports link in staffing section")
    else:
        print("Could not find staffing-tools section")
else:
    print("Could not find /reports link")
