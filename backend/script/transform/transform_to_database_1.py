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
    write_to_parquet(df, file_path)
    
    