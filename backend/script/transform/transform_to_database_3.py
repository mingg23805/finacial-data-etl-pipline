from configfile.logger_utils import setup_logger
from configfile.io_utils import write_to_parquet
import pandas as pd
import numpy as np

def transform_to_database_exchange(df,file_path):
    
    col=["exchange_id","region_id","primary_exchange"]
    df=df[col]
    df=df.rename(columns={"primary_exchange": "exchange_name"})
    df=df.drop_duplicates(subset=["exchange_id"])
    
    write_to_parquet(df, file_path)
def transform_to_database_region(df,file_path):
    
    col=["region_id","region","local_open","local_close"]
    df=df[col]
    df=df.drop_duplicates(subset=["region_id"])
    
    write_to_parquet(df, file_path)