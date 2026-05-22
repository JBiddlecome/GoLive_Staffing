import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

host = os.getenv('DB_HOST', 'localhost')
name = os.getenv('DB_NAME', 'cstaffing_live')
user = os.getenv('DB_USER', 'root')
password = os.getenv('DB_PASSWORD', '')
port = int(os.getenv('DB_PORT', '3306'))
url = URL.create(drivername='mysql+pymysql', username=user, password=password, host=host, port=port, database=name)
engine = create_engine(url)

with engine.connect() as conn:
    res = conn.execute(text('''
        SELECT DISTINCT model 
        FROM history_entry 
        WHERE model LIKE '%pub%' OR model LIKE '%Pub%'
    ''')).fetchall()
    for r in res:
        print(r)
