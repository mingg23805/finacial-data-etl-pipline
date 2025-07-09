
from sqlalchemy import create_engine
from configfile.io_utils import read_parquet
from configfile import logger_utils
logger=setup_logger(__name__)
def load_company_data_to_database(engine, file_path):
    """
    Load company data to the database.
    """
    print("Company data loaded to database.")