import requests
import json
import datetime
from configfile.config import Config
from configfile.logger_utils import setup_logger
logger = setup_logger(__name__)
def  fetch_data_from_polygon(api_key=Config.POLYGON_API_KEY):
    """
    Fetch data from the Polygon API using the provided API key.

    Parameters:
        api_key (str): The API key for the Polygon API.

    Returns:
        dict: The JSON response from the Polygon API.
    """
    url = f'https://api.polygon.io/v2/reference/news?apiKey={api_key}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        company_data = response.json()
        return company_data
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        raise
    except Exception as err:
        logger.error(f"An error occurred: {err}")
        raise
def get_current_date():
    """
    Get the current date in YYYY-MM-DD format.

    Returns:
        str: The current date as a string.
    """
    return datetime.datetime.now().strftime('%Y-%m-%d')
def save_data_to_from_polygon_json(data=fetch_data_from_polygon()):
    """
    Save the provided data to a JSON file.

    Parameters:
        data (dict): The data to save.
    """
    file_path = "elt/data/raw/ohlcs/" + f"ohlcs_{get_current_date()}.json"
    try:
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
    except Exception as e:
        logger.error(f"Error saving data to JSON: {e}")
        raise