"""

cdc_tracker file 

This pulls data from USGS public feed, Stroes a snapshot in SQL lite and every runs pulls a new data

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
print("Hello")