"""
A earthquake tracker dashboard that originates from live CDC log
"""

import sqlite3
import pandas as pd
import plotly.express as px
import streamlit as slt
import cdc_tracker as cdc