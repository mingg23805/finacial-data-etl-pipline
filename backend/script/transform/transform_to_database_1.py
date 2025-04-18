from configfile.logger_utils import setup_logger
import os
import sys
import pandas as pd
import json
import numpy as np
from configfile.io_utils import read_json_file, write_json_file
logger =setup_logger(__name__)
def transform_to_database_companies():
    df=read_json_file('backend/data/raw/companies.json')
    df['name'] = df['name'].str.replace('\'', '')
    df=df['id','exchange','industry','sic',
          'name','ticker','isDelisted','category','currency','location']
    df.columns = ['company_id','company_exchange','industry','sic',
                  'name','ticker','isDelisted','category','currency','location']
