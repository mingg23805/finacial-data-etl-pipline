from configfile.config import Config
from configfile.logger_utils import setup_logger
from configfile.io_utils import write_json_file
import requests
import json
import sys
import os

logger = setup_logger(__name__)
def fetch_data_from_sec(api_key=Config.SEC_API_KEY):
    """
    Fetch data from the SEC API using the provided API key.
    
    Parameters:
        api_key (str): The API key for the SEC API.
    
    Returns:
        dict: The JSON response from the SEC API.
    """
    url = f"https://api.sec-api.io/mapping/exchange/nasdaq?token={api_key}"
    try :
        response = requests.get(url)
        response.raise_for_status() 
        company_data = response.json()
        return company_data
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        sys.exit(1)

    except Exception as err:
        logger.error(f"An error occurred: {err}")
        sys.exit(1)
        
def fetch_data_for_market_status_from_alpha_vantage(api_key=Config.ALPHA_VANTAGE_API_KEY):
    url=f'https://www.alphavantage.co/query?function=MARKET_STATUS&apikey={api_key}'
    try :
        response = requests.get(url)
        response.raise_for_status() 
        market_status_data = response.json()
        return market_status_data
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        sys.exit(1)

    except Exception as err:
        logger.error(f"An error occurred: {err}")
        sys.exit(1)
def extract_to_json():
    logger.info("Fetching data from SEC API...")
    company_data = fetch_data_from_sec()
    logger.info("Data fetched successfully from SEC API.")
    
    logger.info("Fetching market status data from Alpha Vantage...")
    market_status_data = fetch_data_for_market_status_from_alpha_vantage()
    logger.info("Market status data fetched successfully from Alpha Vantage.")
    
    
    write_json_file(company_data, 'backend/data/raw/companies.json')
    write_json_file(market_status_data, 'backend/data/raw/market_status.json')  