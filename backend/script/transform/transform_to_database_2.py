from configfile.logger_utils import setup_logger
from backend.script.transform.json_to_df import companies_df, market_df
from configfile.io_utils import read_json_file, write_json_file
import pandas as pd
import json
import numpy as np
import os
import datetime