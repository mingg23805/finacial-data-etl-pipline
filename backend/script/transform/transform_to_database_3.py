from configfile.logger_utils import setup_logger
from configfile.io_utils import write_to_parquet
import pandas as pd
import numpy as np
import datetime
def get_current_date():
    """
    Get the current date in YYYY-MM-DD format.

    Returns:
        str: The current date as a string.
    """
    return datetime.datetime.now().strftime('%Y-%m-%d')
def transform_to_database_exchange(df):
    
    col=["exchange_id","region_id","primary_exchange"]
    df=df[col]
    df=df.rename(columns={"primary_exchange": "exchange_name"})
    df=df.drop_duplicates(subset=["exchange_id"])
    file_path= f"backend/data/processed/transformed_to_db_exchanges/exchanges_transformed_to_db_{get_current_date()}.parquet"
    write_to_parquet(df, file_path)
def transform_to_database_region(df):
    
    col=["region_id","region","local_open","local_close"]
    df=df[col]
    df.rename(columns={
        "region": "region_name",
        "local_open": "region_local_open",
        "local_close": "region_local_close"
    }, inplace=True)
    
    df=df.drop_duplicates(subset=["region_id"])
    df[["region_local_open", "region_local_close"]] = df[["region_local_open", "region_local_close"]].apply(pd.to_datetime, errors='coerce')
    file_path= f"backend/data/processed/transformed_to_db_regions/regions_transformed_to_db_{get_current_date()}.parquet"
    write_to_parquet(df, file_path)