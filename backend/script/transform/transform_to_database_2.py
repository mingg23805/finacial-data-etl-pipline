from configfile.logger_utils import setup_logger
from configfile.io_utils import write_to_parquet
import pandas as pd
import datetime
def get_current_date():
    """
    Get the current date in YYYY-MM-DD format.

    Returns:
        str: The current date as a string.
    """
    return datetime.datetime.now().strftime('%Y-%m-%d')
logger= setup_logger(__name__)
def transform_to_database_industries(df):
    
    col=["industry_id","industry","sector"]
    industries_df=df[col]
    industries_df=industries_df.drop_duplicates(subset=["industry_id"])[col]
    industries_df=industries_df.reset_index(drop=True)
    industries_df.rename(columns={
        "industry": "industry_name",
        "sector": "industry_sector"
    }, inplace=True)
    industries_df.dropna(subset=["industry_id"], inplace=True)
    file_path= f"backend/data/processed/transformed_to_db_industries/industries_transformed_to_db_{get_current_date()}.parquet"
    write_to_parquet(industries_df, file_path)
def transform_to_database_sic(df):
    
    col=["sic","sicIndustry","sicSector"]
    sicindustries_df=df[col]
    sicindustries_df=sicindustries_df.drop_duplicates(subset=["sic"])[col]
    sicindustries_df=sicindustries_df.reset_index(drop=True)
    sicindustries_df.rename(columns={
        "sic": "sic_id",
        "sicIndustry": "sic_industry",
        "sicSector": "sic_sector"
    }, inplace=True)
    file_path = f"backend/data/processed/transformed_to_db_sic/sic_transformed_to_db_{get_current_date()}.parquet"
    write_to_parquet(sicindustries_df, file_path)
