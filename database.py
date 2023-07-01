import configparser
import sqlite3
from datetime import datetime, timedelta 

def init(db):
    
    '''
    Establish a connection to the queryCache.db
    Creates a database if it does not exist
    '''

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    queryCreateTable = '''
    CREATE TABLE IF NOT EXISTS requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATETIME NOT NULL,
    action TEXT NOT NULL,
    url TEXT NOT NULL,
    response INTEGER NOT NULL
    )
    '''

    cursor.execute(queryCreateTable)
    conn.commit()

    return conn

def record(action, url, response):

    config = configparser.ConfigParser()
    config.read('config.cfg')
    conn = init(config['POLYGON']['rateDb'])

    cursor = conn.cursor()
    
    action = (datetime.now(), action, url, response)
    
    # Remove entries older than 5 minutes to keep a small footprint
    purge()

    query = '''
    INSERT INTO requests (timestamp, action, url, response)
    VALUES (?, ?, ?, ?)
    '''

    cursor.execute(query, action)

    conn.commit()
    conn.close()

    return None

def purge():
    
    '''
    Remove all successful response codes (Status: 200) that are older than
    5 miutes to keep the footprint of the cache database small
    '''

    config = configparser.ConfigParser()
    config.read('config.cfg')
    conn = init(config['POLYGON']['rateDb'])

    cursor = conn.cursor()

    now = datetime.now()
    threshold = now - timedelta(minutes=5)

    query = '''DELETE FROM requests
    WHERE timestamp < ?
    AND response = 200
    '''

    cursor.execute(query, (threshold,))
    conn.commit()
    conn.close()

    return None

def queryRateExceeded(limit:int=5, minutes:int=1):

    '''
    Checks how many times a request has been sent to Polygon.io
    in the last minute. Compares this rate to a given limit to see
    if the limit has been exceeded.
    '''

    config = configparser.ConfigParser()
    config.read('config.cfg')
    conn = init(config['POLYGON']['rateDb'])
    
    cursor = conn.cursor()

    now = datetime.now()
    oneMinuteAgo = now - timedelta(minutes=minutes)

    query = '''
    SELECT COUNT(*) FROM requests
    WHERE timestamp >= ? AND timestamp <= ?
    AND response = 200
    '''

    cursor.execute(query, (oneMinuteAgo, now))
    count = cursor.fetchone()[0]
    conn.close()

    return count >= limit