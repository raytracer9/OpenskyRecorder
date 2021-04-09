#%% imports
import os, sys
import json
from sqlite3.dbapi2 import IntegrityError
import requests
from time import time, sleep
from math import floor
import sqlite3
import logging

with open("./.logindata.json", 'r') as loginfile:
    logindata = json.load(loginfile)


# %% Init DB
def initDB():
    global con
    global cur
    
    table_schema_itegrity = "[(0, 'time_state', 'integer', 0, None, 1), (1, 'icao24', 'text', 1, None, 2), (2, 'callsign', 'text', 0, None, 0), (3, 'origin_country', 'text', 0, None, 0), (4, 'time_position', 'integer', 0, None, 0), (5, 'last_contact', 'integer', 0, None, 0), (6, 'longitude', 'real', 0, None, 0), (7, 'latitude', 'real', 0, None, 0), (8, 'baro_altitude', 'real', 0, None, 0), (9, 'on_ground', 'integer', 0, None, 0), (10, 'velocity', 'real', 0, None, 0), (11, 'true_track', 'real', 0, None, 0), (12, 'vertical_rate', 'real', 0, None, 0), (13, 'sensors', 'text', 0, None, 0), (14, 'geo_altitude', 'real', 0, None, 0), (15, 'squawk', 'text', 0, None, 0), (16, 'spi', 'integer', 0, None, 0), (17, 'position_source', 'integer', 0, None, 0)]"

    if (os.path.isfile(dbLocation)):
        con = sqlite3.connect(dbLocation)
        cur = con.cursor()
        cur.execute("""PRAGMA table_info(states)""")
        if ( str(cur.fetchall()) != table_schema_itegrity):
            raise Exception("'states' table of existing Database does no match with schema. Abort.") 
        log.info("Existing database successfully opened.")
    else:
        log.info (f"Database does not exist at location. Creating new DB at {dbLocation}")
        con = sqlite3.connect(dbLocation)
        cur = con.cursor()
        con.execute("""CREATE TABLE states
                    (
                    time_state integer NOT NULL,
                    icao24 text NOT NULL,
                    callsign text,
                    origin_country text,
                    time_position integer,
                    last_contact integer,
                    longitude real,
                    latitude real,
                    baro_altitude real,
                    on_ground integer,
                    velocity real,
                    true_track real,
                    vertical_rate real,
                    sensors text,
                    geo_altitude real,
                    squawk text,
                    spi integer,
                    position_source integer,
                    PRIMARY KEY (time_state, icao24)
                    )
                    """)

def getStatesJSON(timestamp:int = floor(time())):
    response = requests.get(f"{apiURL}/states/all?time={timestamp}")
    if response.status_code == 200:
        content = response.content
    else:
        raise Exception(f"An error occurred while getting data: Error Code {response.status_code}")
    with open ("latestRequest.json", 'wb') as f:
        f.write(content)
    return json.loads(content.decode())

def saveDataToDB(jsonResponse:dict):
    stateTime = jsonResponse['time']
    states = jsonResponse['states']
    for i in range(len(states)): # convert the inner list to string. List is a invalid Datatype for DB.
        for l in range(len(states[i])):
            if (type(states[i][l]) == list):
                states[i][l] = str(states[i][l])
    try:
        cur.executemany(f"INSERT INTO states VALUES (\"{stateTime}\", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", states)
        con.commit()
    except sqlite3.IntegrityError as ex:
        log.warning (ex)
        rQueue.remove(stateTime)

def genQueue():
    cur.execute("SELECT DISTINCT time_state FROM states ORDER BY time_state DESC LIMIT 360") # get the 360 newest timecodes
    latestInDB = set(i[0] for i in cur.fetchall())
    lastValidTimeStamp = floor(time()/timeInterval)*timeInterval
    lastHour = set([ lastValidTimeStamp-i for i in range(0,3600,timeInterval) ])
    outQueue = lastHour - latestInDB
    log.debug(f"Current Queue Length is {len(outQueue)}")
    return outQueue

if __name__ == "__main__":
    # Logging Parms
    logging.basicConfig(filename='logfile.log', encoding='utf-8', level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)
    log_file_handler = logging.FileHandler("logfile.log")
    log_stdout_handler = logging.StreamHandler(sys.stdout)
    log_file_handler.setFormatter(logging.Formatter(fmt="%(asctime)s - %(levelname)s - %(message)s"))
    log_stdout_handler.setFormatter(logging.Formatter(fmt="%(asctime)s - %(levelname)s - %(message)s"))
    log.addHandler(log_file_handler)
    log.addHandler(log_stdout_handler)
    
    # General Params
    dbLocation = "./data.db"
    timeInterval = 30 # n secods between requests

    # API Config
    username = logindata['username']
    password = logindata['password']
    apiURL = f"https://{username}:{password}@opensky-network.org/api" # Use this for usage with account
    # apiURL = f"https://opensky-network.org/api" # Use this for usage without account

    # Initialize Database
    initDB()

    # Mainloop
    while True:
        try:
            rQueue = genQueue() # Update Queue in every iteration
            for timeCode in rQueue:
                log.info(f"working on {timeCode}")
                saveDataToDB(getStatesJSON(timeCode))
            log.info(f"Queue is empty, waiting {10} seconds")
            sleep(10)
            
        except KeyboardInterrupt:
            log.warning("KeyboardInterrupt. Closing Program")
            break

        except Exception as ex:
            log.error (ex)
            log.error ("Exception occurred. Retry in 4 seconds")
            sleep(4)
        
    con.close() # close Database after KeyboardInterrupt
    log.info("Database closed")
