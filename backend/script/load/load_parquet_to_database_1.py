from sqlalchemy import create_engine
import pandas as pd

from configfile.io_utils import read_parquet_file
from configfile.logger_utils import setup_logger
logger=setup_logger(__name__)
def load_company_data_to_database(engine, file_path):
    """
    Load company data to the database.
    """
    file_path = "backend/data/processed/" + file_path
    logger.info(f"Loading company data from {file_path} to database.")
    df = read_parquet_file(file_path)
    df.to_sql("company_data", con=engine, if_exists="replace", index=False)
    logger.info("Company data loaded to database.")
