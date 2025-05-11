from configfile.logger_utils import setup_logger
from backend.script.transform.json_to_df import companies_df, market_df, region_and_exchange_map, industry_map
from backend.script.exract.extract import extract_to_json
import pandas as pd
import json
import numpy as np
from configfile.io_utils import read_json_file, write_json_file
logger =setup_logger(__name__)
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
def transform_to_database_companies():
    df=companies_df() 
    
    industry_dict=industry_map()
    df=map_industry_code(df, industry_dict)
    
    exchange_mapping, region_mapping = region_and_exchange_map()
    df["exchange_id"] = df["exchange"].map(exchange_mapping)
    
    df = df[[
    "id",       # mã công ty
    "exchange_id",      # mã sàn giao dịch
    "industry_id",      # mã ngành
    "sic",              # mã SIC
    "name",             # tên công ty
    "ticker",           # mã chứng khoán
    "isDelisted",      # đã hủy niêm yết hay chưa
    "category",          # loại công ty
    "currency",         # đơn vị tiền tệ
    "location"          # vị trí
]]
 
    
    
    return df