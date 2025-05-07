from configfile.logger_utils import setup_logger
from backend.script.transform.json_to_df import companies_df, market_df
from backend.script.exract.extract import extract_to_json
import pandas as pd
import json
import numpy as np
from configfile.io_utils import read_json_file, write_json_file
logger =setup_logger(__name__)
def transform_to_database_companies():
    company_df=companies_df() 
    compa
    return 