from configfile.logger_utils import setup_logger
from backend.script.transform.json_to_df import  companies_df, market_df
from configfile.io_utils import read_json_file, write_json_file
import pandas as pd
import json
import numpy as np
import os
import datetime
logger= setup_logger(__name__)
def transform_to_database_sic():
    company_df=companies_df()
    col=["industry_id","industry","sector"]
    industries_df=company_df[col]
    industries_df=industries_df.drop_duplicates(subset=["industry_id"])[col]
    industries_df=industries_df.reset_index(drop=True)
    
    return industries_df.head(10)
def transform_to_database_sic():
    company_df=companies_df()
    col=["sic","sicIndustry","sicSector"]
    sicindustries_df=company_df[col]
    sicindustries_df=sicindustries_df.drop_duplicates(subset=["sic"])[col]
    sicindustries_df=sicindustries_df.reset_index(drop=True)
    
    return sicindustries_df.head(10)
