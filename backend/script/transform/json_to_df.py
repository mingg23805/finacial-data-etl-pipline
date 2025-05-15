from configfile.logger_utils import setup_logger
import os
import sys
from configfile.io_utils import read_json_file
import pandas as pd
import json
import numpy as np
logger = setup_logger(__name__)

def map_industry_code(company_df, industry_dict):
    """
    Gán mã ngành (code) cho DataFrame dựa trên industry và sector
    Parameters:
        df: DataFrame chứa các cột 'industry' và 'sector'
        industry_mapping: dict với key là (industry, sector), value là code
    Returns:
        DataFrame với cột 'code' được cập nhật
    """
    # Ánh xạ code từ industry_mapping
    company_df["industry_id"] = company_df.apply(
    lambda row: industry_dict.get((row["industry"], row["sector"]), pd.NA),
    axis=1
)
    company_df[["industry", "sector", "industry_id"]] = company_df[["industry", "sector", "industry_id"]].replace('', np.nan)

    company_df["industry_id"] = company_df["industry_id"].astype("Int64")
    

    # Nếu industry & sector không rỗng nhưng industry_id không tìm thấy → gán 999
    company_df.loc[
        company_df["industry_id"].isna() &
        company_df["industry"].notna() &
        company_df["sector"].notna(),
        "industry_id"
    ] = 999

    return company_df
def companies_df():
    company_df = read_json_file('backend/data/raw/companies.json')
    company_df["sic"] = company_df["sic"].astype("Int64")
    industry_dict=industry_map()
    company_df=map_industry_code(company_df, industry_dict)
    
    exchange_mapping, region_mapping = region_and_exchange_map()
    company_df["exchange_id"] = company_df["exchange"].map(exchange_mapping)
    return company_df 
def markets_df():
    market_df=read_json_file('backend/data/raw/market_status.json').get("markets",[])
    market_df=pd.json_normalize(market_df)
    market_df=market_df.drop(columns=["market_type","notes"])
    market_df = market_df.rename(columns={"primary_exchanges": "primary_exchange"})

    market_df.columns = market_df.columns.str.strip().str.lower()

    market_df["primary_exchange"] = market_df["primary_exchange"].str.split(r",\s*")
    market_df = market_df.explode("primary_exchange").reset_index(drop=True)
    
    market_df=market_df.replace('', np.nan)
    
    exchange_mapping, region_mapping = region_and_exchange_map()
    market_df["exchange_id"] = market_df["primary_exchange"].map(exchange_mapping)
    market_df["region_id"] = market_df["region"].map(region_mapping)

    
    return market_df
def region_and_exchange_map():
    exchanges_df = read_json_file('backend/data/raw/ExchangeAndRegion.json')
    exchanges_df=exchanges_df[["ExchangeName", "ExchangeCode", "RegionName", "RegionCode"]]
    exchange_to_code = dict(zip(exchanges_df["ExchangeName"], exchanges_df["ExchangeCode"]))
    region_to_code = dict(zip(exchanges_df["RegionName"], exchanges_df["RegionCode"]))
    return exchange_to_code, region_to_code

def industry_map():
    industry_df = read_json_file('backend/data/raw/IndustryCode.json')
    industry_df.columns = industry_df.columns.str.strip().str.lower()
    industry_mapping = dict(zip(
    zip(industry_df["industry"], industry_df["sector"]),  # tạo tuple (industry, sector)
    industry_df["code"]
))
    return industry_mapping
