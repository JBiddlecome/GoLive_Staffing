import os
from sqlalchemy import create_engine, inspect

# Use standard pymysql URL
url = "mysql+pymysql://cstaffing:nWryUxSbGD@127.0.0.1:3307/cstaffing_live"

engine = create_engine(url)
inspector = inspect(engine)

tables = inspector.get_table_names()
venue_related_tables = []

for table in tables:
    columns = [col['name'] for col in inspector.get_columns(table)]
    if 'venue_id' in columns:
        venue_related_tables.append(table)
    elif table in ['contact', 'document', 'venue_attestation', 'venue_position', 'attestation', 'position']: # Check other potential tables
        print(f"Interesting table found without venue_id (maybe linked differently): {table}")
        
print("Tables with venue_id column:")
print(venue_related_tables)

print("\nDetailed columns for some important tables:")
for t in ['venue', 'dnr', 'exclusive', 'venue_document', 'document', 'venue_contact', 'contact', 'venue_position', 'position', 'venue_attestation', 'attestation']:
    if t in tables:
        cols = inspector.get_columns(t)
        print(f"\n--- {t} ---")
        for c in cols:
            print(f"{c['name']} : {c['type']}")

