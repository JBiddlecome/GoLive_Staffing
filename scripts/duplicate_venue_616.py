import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

url = "mysql+pymysql://cstaffing:nWryUxSbGD@127.0.0.1:3307/cstaffing_live"
engine = create_engine(url)

OLD_VENUE_ID = 616
NEW_CLIENT_ID = 1619

with engine.connect() as conn:
    with conn.begin():
        # 1. Copy Venue
        venue_row = conn.execute(text(f"SELECT * FROM venue WHERE venue_id = {OLD_VENUE_ID}")).mappings().fetchone()
        if not venue_row:
            raise Exception("Venue 616 not found")
        
        venue_dict = dict(venue_row)
        venue_dict.pop('venue_id') # Remove primary key
        venue_dict['client_id'] = NEW_CLIENT_ID
        
        columns = ", ".join([f"`{k}`" for k in venue_dict.keys()])
        placeholders = ", ".join([f":{k}" for k in venue_dict.keys()])
        insert_query = text(f"INSERT INTO venue ({columns}) VALUES ({placeholders})")
        
        res = conn.execute(insert_query, venue_dict)
        new_venue_id = res.lastrowid
        print(f"Created new venue with ID: {new_venue_id}")

        # 2. Copy DNR
        dnr_rows = conn.execute(text(f"SELECT * FROM dnr WHERE venue_id = {OLD_VENUE_ID}")).mappings().fetchall()
        for r in dnr_rows:
            d = dict(r)
            d.pop('dnr_id')
            d['client_id'] = NEW_CLIENT_ID
            d['venue_id'] = new_venue_id
            cols = ", ".join([f"`{k}`" for k in d.keys()])
            vals = ", ".join([f":{k}" for k in d.keys()])
            conn.execute(text(f"INSERT INTO dnr ({cols}) VALUES ({vals})"), d)
        print(f"Copied {len(dnr_rows)} DNR records")

        # 3. Copy Exclusive
        exc_rows = conn.execute(text(f"SELECT * FROM exclusive WHERE venue_id = {OLD_VENUE_ID}")).mappings().fetchall()
        for r in exc_rows:
            d = dict(r)
            d.pop('exclusive_id')
            d['client_id'] = NEW_CLIENT_ID
            d['venue_id'] = new_venue_id
            cols = ", ".join([f"`{k}`" for k in d.keys()])
            vals = ", ".join([f":{k}" for k in d.keys()])
            conn.execute(text(f"INSERT INTO exclusive ({cols}) VALUES ({vals})"), d)
        print(f"Copied {len(exc_rows)} Exclusive records")

        # 4. Copy venue_document
        vdoc_rows = conn.execute(text(f"SELECT * FROM venue_document WHERE venue_id = {OLD_VENUE_ID}")).mappings().fetchall()
        for r in vdoc_rows:
            d = dict(r)
            d.pop('id')
            d['venue_id'] = new_venue_id
            cols = ", ".join([f"`{k}`" for k in d.keys()])
            vals = ", ".join([f":{k}" for k in d.keys()])
            conn.execute(text(f"INSERT INTO venue_document ({cols}) VALUES ({vals})"), d)
        print(f"Copied {len(vdoc_rows)} venue_document records")

        # 5. Copy venue_contact
        vcon_rows = conn.execute(text(f"SELECT * FROM venue_contact WHERE venue_id = {OLD_VENUE_ID}")).mappings().fetchall()
        for r in vcon_rows:
            d = dict(r)
            d.pop('id')
            d['venue_id'] = new_venue_id
            cols = ", ".join([f"`{k}`" for k in d.keys()])
            vals = ", ".join([f":{k}" for k in d.keys()])
            conn.execute(text(f"INSERT INTO venue_contact ({cols}) VALUES ({vals})"), d)
        print(f"Copied {len(vcon_rows)} venue_contact records")

        # 6. Copy venue_position
        vpos_rows = conn.execute(text(f"SELECT * FROM venue_position WHERE venue_id = {OLD_VENUE_ID}")).mappings().fetchall()
        for r in vpos_rows:
            d = dict(r)
            d.pop('venue_position_id')
            d['venue_id'] = new_venue_id
            d['del_client_id'] = NEW_CLIENT_ID
            cols = ", ".join([f"`{k}`" for k in d.keys()])
            vals = ", ".join([f":{k}" for k in d.keys()])
            conn.execute(text(f"INSERT INTO venue_position ({cols}) VALUES ({vals})"), d)
        print(f"Copied {len(vpos_rows)} venue_position records")

        # 7. Copy venue_attestation_question
        vaq_rows = conn.execute(text(f"SELECT * FROM venue_attestation_question WHERE venue_id = {OLD_VENUE_ID}")).mappings().fetchall()
        for r in vaq_rows:
            d = dict(r)
            d.pop('id')
            d['client_id'] = NEW_CLIENT_ID
            d['venue_id'] = new_venue_id
            cols = ", ".join([f"`{k}`" for k in d.keys()])
            vals = ", ".join([f":{k}" for k in d.keys()])
            conn.execute(text(f"INSERT INTO venue_attestation_question ({cols}) VALUES ({vals})"), d)
        print(f"Copied {len(vaq_rows)} venue_attestation_question records")
        
        print("\nAll done!")
