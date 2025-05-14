from configfile.logger_utils import setup_logger
from configfile.io_utils import write_to_parquet
import pandas as pd
logger= setup_logger(__name__)
def transform_to_database_industries(df,file_path):
    
    col=["industry_id","industry","sector"]
    industries_df=df[col]
    industries_df=industries_df.drop_duplicates(subset=["industry_id"])[col]
    industries_df=industries_df.reset_index(drop=True)
    
    write_to_parquet(industries_df, file_path)
def transform_to_database_sic(df,file_path):
    
    col=["sic","sicIndustry","sicSector"]
    sicindustries_df=df[col]
    sicindustries_df=sicindustries_df.drop_duplicates(subset=["sic"])[col]
    sicindustries_df=sicindustries_df.reset_index(drop=True)
    
    write_to_parquet(sicindustries_df, file_path)
