import requests
import json
import datetime
from configfile.config import Config

def get_data_by_time_range(time_zone):
    """Get data based on the current time zone range."""
    yesterday = (datetime.date.today() - datetime.timedelta(days=1))
    if time_zone == 1:
        time_from = yesterday.strftime("%Y%m%dT"+"0000")
        time_to = yesterday.strftime("%Y%m%dT"+"0929")
    elif time_zone == 2:
        time_from = yesterday.strftime("%Y%m%dT"+"0930")
        time_to = yesterday.strftime("%Y%m%dT"+"1600")
    else:
        time_from = yesterday.strftime("%Y%m%dT"+"1601")
        time_to = yesterday.strftime("%Y%m%dT"+"2359")
    return time_from, time_to

def fetch_data_from_alpha(api_key=Config.ALPHA_VANTAGE_API_KEY):
    """
    Fetch data from the Alpha Vantage API using the provided API key.

    Parameters:
        api_key (str): The API key for the Alpha Vantage API.

    Returns:
        dict: The JSON response from the Alpha Vantage API.
    """
    limit="1000"
    json_object=[]
    for time_zone in [1,2,3]:
        time_from, time_to = get_data_by_time_range(time_zone)
        url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&time_from={time_from}&time_to={time_to}&limit={limit}&apikey={api_key}'
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()["feed"]
            json_object+=data
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            raise
        except Exception as err:
            print(f"An error occurred: {err}")
            raise
    return json_object
def get_current_date():
    """
    Get the current date in YYYY-MM-DD format.

    Returns:
        str: The current date as a string.
    """
    return datetime.datetime.now().strftime('%Y-%m-%d')
def save_data_to_from_alpha_json(data=fetch_data_from_alpha()):
    """
    Save the provided data to a JSON file.

    Parameters:
        data (dict): The data to save.
        file_path (str): The path to the JSON file.
    """
    file_path = "elt/data/raw/news/" + f"news_{get_current_date()}.json"
    try:
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
            print(f"Data saved to file {file_path}")
    except Exception as e:
        print(f"Error saving data to JSON: {e}")
        raise
save_data_to_from_alpha_json()