"""

cdc_tracker file 

This pulls data from USGS public feed, Stores a snapshot in SQL lite and every runs pulls a new data

INSERT - if new records
UPDATE -anything has changed 
DELETE - data that was rolled off 


"""

import argparse
import hashlib
import sqlite3
from datetime import datetime,timezone

import pandas as pd
import requests
#-----------------------------------------------------
#Config 
#-----------------------------------------------------

FEED_URL= "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
DB_PATH = "cdc_tracker.db"
TRACKED_FIELDS = ["mag","place","status","tsunami","sig"]

#----------------------------------------------------------
# 1. Extract - pull fresh data from API 
#----------------------------------------------------------
def _hash_row(row: pd.Series) -> str: 
    """
    Concatenate all the columns into one string and hash it. This tracks if something has changed
    """
    raw = "|".join(str(row[field]) for field in TRACKED_FIELDS)
    return hashlib.md5(raw.encode()).hexdigest()

def fetch_current_data() -> pd.DataFrame:
    response= requests.get(FEED_URL,timeout=15)
    response.raise_for_status()
    geojson= response.json()

    rows=[]
    for feature in geojson["features"]:
        props = feature["properties"]
        lon,lat,depth = feature["geometry"]["coordinates"]
        rows.append(
            {
                "id":feature["id"],
                "place": props.get("place"),
                "mag":props.get("mag"),
                "status": props.get("status"),
                "tsunami": props.get("tsunami"),
                "sig":props.get("sig"),
                "source_updated":props.get("updated"),
                "latitude":lat,
                "longitude":lon,
                "depth_km":depth,


            }
        )
        df = pd.DataFrame(rows)
        df["row_hash"]=df.apply(_hash_row,axis=1)
        df["captured_at"]=datetime.now(timezone.utc).isoformat()
    return df


#---------------------------------------------------------------
#2.Storage - SQL lite snapshot + CDC log
#---------------------------------------------------------------
def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cdc_log(
        log_id  INTEGER PRIMARY KEY AUTOINCREMENT,
        quake_id TEXT,
        change_type TEXT, 
        field_name TEXT, 
        old_value TEXT, 
        new_value TEXT,
        detected_at TEXT
        )
        """
    )
    conn.commit() 

def load_previous_snapshot(conn:sqlite3.Connection)->pd.DataFrame:
    """Equivalent of : SELECT * FROM snapshot_current"""
    try:
        return pd.read_sql("SELECT * FROM snapshot_current",conn)
    except pd.error.DatabaseError:
        #Table does not existst, this is the first run
        return pd.DataFrame(columns=["id","row_hash"]+TRACKED_FIELDS)

def save_snapshot(conn:sqlite3.Connection, df:pd.DataFrame) -> None:
    """Equivalent of a full upset/INSERT into the snapshot_current"""
    df.to_sql("snapshot_current",if_exists="replace",index=False)
    conn.commit()


conn= sqlite3.connect(DB_PATH)
init_db(conn)   
print(fetch_current_data())    

