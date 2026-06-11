import urllib.request

try:
    url = "http://127.0.0.1:8000/flag-audit"
    print(f"Requesting {url}...")
    with urllib.request.urlopen(url) as response:
        html = response.read().decode('utf-8')
        print(f"Status code: {response.status}")
        
        # Check for Green and No Flag checkbox fields
        print("Checking for filters...")
        if 'value="2"' in html:
            print("  Found Green Flag checkbox value='2'!")
        else:
            print("  MISSING Green Flag checkbox value='2'!")
            
        if 'value="none"' in html:
            print("  Found No Flag checkbox value='none'!")
        else:
            print("  MISSING No Flag checkbox value='none'!")
            
        # Check for Shifts (Last Year) column header
        print("Checking for column header...")
        if 'Shifts (Last Year)' in html:
            print("  Found Shifts (Last Year) header!")
        else:
            print("  MISSING Shifts (Last Year) header!")

        # Check if the tables have been filled out properly
        if 'No employees match the selected flag filters' in html:
            print("  No employees listed (default flags 0 and 1 empty or loaded?)")
        else:
            print("  Employees are listed on the page!")
            
except Exception as e:
    print(f"Error requesting: {e}")
