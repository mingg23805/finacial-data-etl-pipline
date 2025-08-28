import psycopg2
import pandas as pd
import io
from configfile.io_utils import read_parquet_file
from configfile.logger_utils import setup_logger
from configfile.config import Config
from configfile.file_config import get_latest_file_in_directory
logger=setup_logger(__name__)

def upsert_parquet_to_postgres(file_path, table_name, columns, conflict_cols, db_params):
    # 1. Đọc parquet chỉ lấy đúng các cột được truyền
    df = pd.read_parquet(file_path, columns=columns)
    df = df.drop_duplicates(subset=conflict_cols, keep="last")
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    col_list = ", ".join(columns)
    excluded_updates = ", ".join([f"{col}=EXCLUDED.{col}" for col in columns if col not in conflict_cols])
    
    staging_table = f"{table_name}_staging"

    # 2. Tạo bảng staging với đúng các cột cần thiết
    cur.execute(f"DROP TABLE IF EXISTS {staging_table};")
    cur.execute(f"""
        CREATE TEMP TABLE {staging_table} (LIKE {table_name} INCLUDING DEFAULTS)
    """)
    
    # 3. Copy data vào staging
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    print("Buffer content:")
    print(buffer.getvalue())
    cur.copy_from(buffer, staging_table, sep=",", columns=columns)

    # 4. Insert từ staging vào bảng chính
    excluded_updates = ", ".join([
        f"{col}=EXCLUDED.{col}" 
        for col in columns if col not in conflict_cols 
    ])
    
    merge_sql = f"""
    INSERT INTO {table_name} ({col_list})
    SELECT {col_list}
    FROM {staging_table}
    ON CONFLICT ({", ".join(conflict_cols)}) DO UPDATE SET
        {excluded_updates};
    """
    cur.execute(merge_sql)

    conn.commit()
    cur.close()
    conn.close()
def load_companies_to_db():
    last_parquet_file= get_latest_file_in_directory("backend/data/processed/transformed_to_db_companies",".parquet")
    db_params= {
        "dbname":Config.DB_NAME,
        "user":Config.DB_USER,
        "password":Config.DB_PASSWORD,
        "host":Config.DB_HOST,
        "port":Config.DB_PORT
    }
    upsert_parquet_to_postgres( last_parquet_file,"companies"
                            ,columns=[
                            "company_exchange_id",
                            "company_industry_id",
                            "company_sic_id",
                            "company_name",
                            "company_ticker",
                            "company_is_delisted",
                            "company_category",
                            "company_currency",
                            "company_location"],
                            conflict_cols=['company_ticker', 'company_is_delisted'],
                            db_params=db_params)
def load_industries_to_db():
    last_parquet_file= get_latest_file_in_directory("backend/data/processed/transformed_to_db_industries",".parquet")
    db_params= {
        "dbname":Config.DB_NAME,
        "user":Config.DB_USER,
        "password":Config.DB_PASSWORD,
        "host":Config.DB_HOST,
        "port":Config.DB_PORT
    }
    upsert_parquet_to_postgres( last_parquet_file,"industries"
                            ,columns=["industry_name",
                            "industry_id",
                            "industry_sector"],
                            conflict_cols=['industry_id'],
                            db_params=db_params)
def load_sic_to_db():
    last_parquet_file= get_latest_file_in_directory("backend/data/processed/transformed_to_db_sic",".parquet")
    db_params= {
        "dbname":Config.DB_NAME,
        "user":Config.DB_USER,
        "password":Config.DB_PASSWORD,
        "host":Config.DB_HOST,
        "port":Config.DB_PORT
    }
    upsert_parquet_to_postgres( last_parquet_file,"sicindustries"
                            ,columns=['sic_id', 'sic_industry', 'sic_sector'],
        
                            conflict_cols=['sic_id'],
                            db_params=db_params)
def load_region_to_db():
    last_parquet_file= get_latest_file_in_directory("backend/data/processed/transformed_to_db_regions",".parquet")
    db_params= {
        "dbname":Config.DB_NAME,
        "user":Config.DB_USER,
        "password":Config.DB_PASSWORD,
        "host":Config.DB_HOST,
        "port":Config.DB_PORT
    }
    upsert_parquet_to_postgres( last_parquet_file
                            ,"regions"
                            ,columns=['region_id','region_name', 'region_local_open', 'region_local_close']
                            ,conflict_cols=['region_name'],
                            db_params=db_params)
def load_exchange_to_db():
    last_parquet_file= get_latest_file_in_directory("backend/data/processed/transformed_to_db_exchanges",".parquet")
    db_params= {
        "dbname":Config.DB_NAME,
        "user":Config.DB_USER,
        "password":Config.DB_PASSWORD,
        "host":Config.DB_HOST,
        "port":Config.DB_PORT
    }
    upsert_parquet_to_postgres( last_parquet_file
                            ,"exchanges"
                            ,columns=['exchange_id', 'exchange_region_id', 'exchange_name']
                            ,conflict_cols=['exchange_name'],
                            db_params=db_params)