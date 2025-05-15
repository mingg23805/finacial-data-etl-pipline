
from sqlalchemy import create_engine
from configfile.io_utils import read_parquet

def load_company_data_to_database(engine, file_path):
    """
    Load company data to the database.
    """
    companies_df.to_sql('companies', con=engine, if_exists='replace', index=False)
    print("Company data loaded to database.")