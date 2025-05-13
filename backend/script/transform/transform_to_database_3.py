from configfile.logger_utils import setup_logger
from backend.script.transform.json_to_df import  companies_df, market_df
from configfile.io_utils import read_json_file, write_json_file
import pandas as pd
import json
import numpy as np
import os
import datetime
def transform_to_database_exchange():
    df=market_df()
    col=["exchange_id","region_id","primary_exchange"]
    exchanges_df=df[col]
    exchanges_df=exchanges_df.rename(columns={"primary_exchange": "exchange_name"})
    exchanges_df=exchanges_df.drop_duplicates(subset=["exchange_id"])
    return exchanges_df.head(10)
def transform_to_database_region():
    markets_df=market_df()
    col=["region_id","region","local_open","local_close"]
    region_df=markets_df[col]
    region_df=region_df.drop_duplicates(subset=["region_id"])
    return region_df .head(10)