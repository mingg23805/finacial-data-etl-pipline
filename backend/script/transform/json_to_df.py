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
def market_df():
    market_df=read_json_file('backend/data/raw/market_status.json').get("markets",[])
    market_df=pd.json_normalize(market_df)
    
    market_df = market_df.rename(columns={"primary_exchanges": "primary_exchange"})

    market_df.columns = market_df.columns.str.strip().str.lower()

    market_df["primary_exchange"] = market_df["primary_exchange"].str.split(r",\s*")
    market_df = market_df.explode("primary_exchange").reset_index(drop=True)
    
    market_df=market_df.replace('', np.nan)

    exchanges_df = read_json_file('backend/data/raw/ExchangeAndRegion.json')
    exchanges_df=exchanges_df[["ExchangeName", "ExchangeCode", "RegionName", "RegionCode"]]
    exchanges_df = exchanges_df.rename(columns={
    "ExchangeName": "primary_exchange",
    "ExchangeCode": "exchange_id",
    "RegionName": "region",
    "RegionCode": "region_id"
})
    market_df = market_df.merge(exchanges_df[["primary_exchange", "region", "exchange_id", "region_id"]],
                            on=["primary_exchange", "region"],
                            how="left")
    market_df = market_df.drop(columns=["notes",'market_type'], axis=1)
    return market_df
def region_and_exchange_mapping():
    exchanges_df = read_json_file('backend/data/raw/ExchangeAndRegion.json')
    exchanges_df=exchanges_df[["ExchangeName", "ExchangeCode", "RegionName", "RegionCode"]]
    name_to_code = dict(zip(exchanges_df["ExchangeName"], exchanges_df["ExchangeCode"]))
    region_to_code = dict(zip(exchanges_df["RegionName"], exchanges_df["RegionCode"]))
    return name_to_code, region_to_code
def industry_mapping():
    industry_df = read_json_file('backend/data/raw/IndustryCode.json')
    industry_df.columns = industry_df.columns.str.strip().str.lower()
    industry_mapping = dict(zip([industry_df["Industry"],industry_df["Sector"]], industry_df["Code"]))
    return industry_mapping
