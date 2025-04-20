from configfile.logger_utils import setup_logger
import os
import sys
from configfile.io_utils import read_json_file
import pandas as pd
import json
import numpy as np
logger = setup_logger(__name__)
def companies_df():
    company_df = read_json_file('backend/data/raw/companies.json')
    
    industry_df = read_json_file('backend/data/raw/IndustryCode.json')
    industry_df.columns = industry_df.columns.str.strip().str.lower()
    df = company_df.merge(
    industry_df[["industry", "sector", "code"]],
    on=['industry', 'sector'],
    how='left'  
)   
    df['code'] = df['code'].fillna(0).astype('Int64')
    df[['industry', 'sector', 'code']] = df[['industry', 'sector', 'code']].replace('', np.nan)
    df.loc[df['code'].isna() & df['industry'].notna()& df['sector'].notna(), 'code'] = 999
    return df
def markert_df():
    df = read_json_file('backend/data/raw/market_status.json')
    df.columns = df.columns.str.strip().str.lower()

    # df = df.explode("primary_exchanges")
    
    return df
