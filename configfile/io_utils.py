from configfile.logger_utils import setup_logger
import os
import sys
import pandas as pd
import json
logger=setup_logger(__name__)
def read_json_file(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        return df
    except FileNotFoundError:
        logger.error(f"File {file_path} not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        logger.error("Error decoding JSON from the file.")
        sys.exit(1) 


def write_json_file(data, file_path, indent=4):
    logger.info(f"Ghi dữ liệu ra file JSON: {file_path}")
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)
        logger.info(f"data saved to file {file_path}")