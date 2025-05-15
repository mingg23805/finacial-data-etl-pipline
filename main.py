from backend.script.transform.json_to_df import companies_df, markets_df
from backend.script.transform.transform_to_database_1 import transform_to_database_companies
from backend.script.transform.transform_to_database_2 import transform_to_database_sic, transform_to_database_industries
from backend.script.transform.transform_to_database_3 import transform_to_database_region, transform_to_database_exchange
from configfile.config import Config

from sqlalchemy import create_engine
def main():
        engine = create_engine(Config.DATABASE_URL)
        
if __name__ == "__main__":
        main()  