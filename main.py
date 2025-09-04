from backend.script.exract.extract import extract_to_json

from configfile.config import Config
from configfile.logger_utils import setup_logger
from backend.script.load.load_parquet_to_database_1 import load_companies_to_db,load_exchange_to_db,load_industries_to_db,load_region_to_db,load_sic_to_db
from backend.script.transform.json_to_df import markets_df,companies_df
from backend.script.transform.transform_to_database_1 import transform_to_database_companies
from backend.script.transform.transform_to_database_2 import transform_to_database_industries, transform_to_database_sic
from backend.script.transform.transform_to_database_3 import transform_to_database_exchange, transform_to_database_region

from elt.scripts.load.load_db_to_parquet import load_db_to_dl
logger=setup_logger(__name__)
def main():
        # engine= create_engine(
        # f"postgresql://{Config.DB_USER}:{Config.DB_PASSWORD}@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}"
        # )
        #save_data_to_from_polygon_json()
        #company_df=companies_df()
        # # market_df=markets_df()
        #transform_to_database_companies(company_df)
        # transform_to_database_industries(company_df)
        #transform_to_database_sic(company_df)
        # transform_to_database_exchange(market_df)
        # transform_to_database_region(market_df)
        #load_region_to_db()
        #load_industries_to_db()
        #load_sic_to_db()
        #load_exchange_to_db()
        #load_companies_to_db()
        
        load_db_to_dl()
        
        
        
        
if __name__ == "__main__":
        main()  