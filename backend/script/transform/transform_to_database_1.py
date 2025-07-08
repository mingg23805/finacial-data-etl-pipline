from configfile.logger_utils import setup_logger
from configfile.io_utils import write_to_parquet

logger =setup_logger(__name__)
def transform_to_database_companies(df,file_path): 
    
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
    df = df.rename(columns={
        "id": "company_id",
        "exchange_id": "company_exchange_id",
        "industry_id": "company_industry_id",
        "sic": "company_sic_id",
        "name": "company_name",
        "ticker": "company_ticker",
        "isDelisted": "company_is_delisted",
        "category": "company_category",
        "currency": "company_currency",
        "location": "company_location"
    })
    write_to_parquet(df, file_path)
    
    