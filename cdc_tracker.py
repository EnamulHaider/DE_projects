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
    except pd.errors.DatabaseError:
        #Table does not existst, this is the first run
        return pd.DataFrame(columns=["id","row_hash"]+TRACKED_FIELDS)

def save_snapshot(conn:sqlite3.Connection, df:pd.DataFrame) -> None:
    """Equivalent of a full upset/INSERT into the snapshot_current"""
    df.to_sql("snapshot_current",conn,if_exists="replace",index=False)
    conn.commit()
#---------------------------------------------------------------------------
# 3. TRANSFORM - the actual CDC comparison logic
#---------------------------------------------------------------------------
def compute_changes(current: pd.DataFrame , previous:pd.DataFrame) -> pd.DataFrame:

    """
    This is the jeart of project: an OUTER JOIN on id, then classify each row based on which side it came from and if there is any column that has changesd
    """
    merged = current.merge(
        previous[["id","row_hash"]+TRACKED_FIELDS],
        on ="id",
        how="outer",
        suffixes=("_new","_old"),
        indicator=True
    )
    log_rows = []
    now = datetime.now(timezone.utc).isoformat()

    for _,rows in merged.iterrows():
        if rows["_merge"] == "left_only":
            ##Exists in current pull and not in previous data 
            log_rows.append(
            {
                "quake_id": rows["id"],
                "change_type":"INSERT",
                "field_name": None,
                "old_value" : None,
                "new_value" : None,
                "detected_at": now
            }
                )
        elif rows["_merge"]=="right_only":
            log_rows.append(
                {
                    "quake_id":rows["id"],
                    "change_type":"DELETE",
                    "field_name": None,
                    "old_value": None,
                    "new_value": None,
                    "detected_at":now
                }
            )
        else:
            #In both - only log it if the record has changed
            if rows["row_hash_new"] != rows["row_hash_old"]:
                for item in TRACKED_FIELDS:
                    old_item = rows[f"{item}_old"]
                    new_item = rows[f"{item}_new"]
                    if old_item != new_item:
                        log_rows.append ({
                                "quake_id":rows["id"],
                                "change_type":"UPDATE",
                                "field_name": item,
                                "new_value":new_item,
                                "old_value":old_item

                        })
    return pd.DataFrame(log_rows)
                        

#-------------------------------------------------------------
#4. LOAD - write tyhe cdc log and print a summary 
#-------------------------------------------------------------
def write_log(conn:sqlite3.Connection,changes : pd.DataFrame) -> None:
    if changes.empty:
        return 
    changes.to_sql("cdc_log",conn,if_exists="append",index=False)
    conn.commit()

def print_summary(current: pd.DataFrame,changes:pd.DataFrame,is_first_run: bool) -> None: 
                    if is_first_run:
                       print(f"First run: send the snapshot with {len(current)} earthquake from last 24 hours.")
                       print("Run the scrip again")
                       return
                    if changes.empty:
                       print("No changes were detected")
                       return
                    counts = changes["change_type"].value_counts()
                    print("Change summary:")
                    for change_type in  ["INSERT","UPDATE","DELETE"]:
                         print(f" {change_type:7s}: {counts.get(change_type,0)}")
                    updates = changes[changes["change_type"]=="UPDATE"]
                    if not updates.empty:
                         print("\nSample of revised fields:")
                         print(updates[["quake_id","field_name","old_value","new_value"]].head(10).to_string(index=False))

def show_full_log(conn:sqlite3.Connection) -> None:
     log_read = pd.read_sql("SELECT * FROM cdc_log ORDER BY detected_at DESC",conn)
     if log_read.empty:
          print("CDC log is empty.")
     else:
          print(log_read.to_string(index=False))  


#-----------------------------------------------------------------------
#Main everything does live here
#So importing this files never has side effets
#-----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="UGSC earthquake CDC tracker ")
    parser.add_argument("--show-log",action="store_true",help="Print the CDC log and exit")
    args=parser.parse_args()
    conn= sqlite3.connect(DB_PATH)
    init_db(conn)   
    previous = load_previous_snapshot(conn)
    is_first_run= previous.empty
    current = fetch_current_data()
    changes = compute_changes(current,previous)
    write_log(conn,changes)
    save_snapshot(conn,current)
    print_summary(current, changes, is_first_run)
    print("Execution complete.")
    conn.close()

if __name__=="__main__":
     main()



