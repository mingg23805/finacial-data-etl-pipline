import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os
import json

def get_latest_file(directory,extension):
    """
    Get the most recent file in the specified directory.
    """
    try:
        files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(extension)]
        if not files:
            print(f"No {extension} files found in the directory.")
            return None
        latest_file = max(files, key=os.path.getmtime)
        print(f"Latest file found: {latest_file}")
        return latest_file
    except Exception as e:
        print(f"Error getting last file: {e}")
        return None
def load_json_from_file(file_path):
    """
    Load a JSON file into a Python object (dict or list).
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            print(type(data))
        return data
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return None
    except json.JSONDecodeError as e:
        print(f"Error reading JSON file: {e}")
        return None
def save_to_parquet(data, parquet_file):
    """
    Convert raw data to a DataFrame and save it in Parquet format.
    """
    try:
        # Validate data format for DataFrame conversion
        if not (isinstance(data, list) and all(isinstance(item, dict) for item in data)) and not (isinstance(data, dict) and all(isinstance(v, list) for v in data.values())):
            raise ValueError("Data must be a list of dicts or a dict of lists for DataFrame conversion.")
        df=pd.DataFrame(data)   
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_file)
        print(f"Converted DataFrame to {parquet_file}")
    except Exception as e:
        print(f"Error converting DataFrame to Parquet: {e}")
def load_db_to_dl(input_directory, output_directory):
    """
    Load data from a directory and save it to Parquet format.
    """
    latest_file = get_latest_file(input_directory, '.json')
    if not latest_file:
        print("No JSON files found to convert.")
        return
    else:
        data= load_json_from_file(latest_file)
        print(f"Read file: {latest_file}")
        filename = os.path.basename(latest_file).replace('.json', ".parquet")
        output_filepath = os.path.join(output_directory, filename)
        
        save_to_parquet(data,output_filepath)
        print(f"Saved Parquet file: {output_filepath}")
def convert_news_to_parquet():
    input_directory = r'elt/data/raw/news'
    output_directory = r'elt/data/completed/load_api_news_to_dl'
    load_db_to_dl(input_directory, output_directory)
def convert_ohlcs_to_parquet():
    input_directory = r'elt/data/raw/ohlcs'
    output_directory = r'elt/data/completed/load_api_ohlcs_to_dl'
    load_db_to_dl(input_directory, output_directory)
    convert_news_to_parquet()
convert_ohlcs_to_parquet()
