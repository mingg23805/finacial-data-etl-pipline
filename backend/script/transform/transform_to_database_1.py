from configfile.logger_utils import setup_logger
from configfile.io_utils import write_to_parquet
import datetime
logger =setup_logger(__name__)
def get_current_date():
    """
    Get the current date in YYYY-MM-DD format.

    Returns:
        str: The current date as a string.
    """
    return datetime.datetime.now().strftime('%Y-%m-%d')
def transform_to_database_companies(df): 
    
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
    file_path= f"backend/data/processed/transformed_to_db_companies/companies_transformed_to_db_{get_current_date()}.parquet"
    write_to_parquet(df, file_path)
    
    