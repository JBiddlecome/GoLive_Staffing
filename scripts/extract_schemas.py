import sys
import re

file_path = 'apps/reportable/cstaffing_live_schema.sql'
tables_to_find = {'client', 'client_position', 'client_position_amount', 'position', 'venue', 'county', 'event', 'shift', 'shift_position', 'shift_employee'}

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Match the CREATE TABLE block for specific tables
output = ''
for table in tables_to_find:
    pattern = re.compile(f'CREATE TABLE `{table}`.*?;', re.DOTALL)
    match = pattern.search(content)
    if match:
        output += match.group(0) + '\n\n'

with open('tmp/relevant_schemas.sql', 'w', encoding='utf-8') as out:
    out.write(output)
print('Done. Extracted to tmp/relevant_schemas.sql')
