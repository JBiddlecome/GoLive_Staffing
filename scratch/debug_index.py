with open(r'c:\Users\jakeb\OneDrive\Documents\GitHub\GoLive_Staffing\templates\index.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if '/reports' in line:
        print(f"{i+1}: {repr(line)}")
