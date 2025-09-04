import os
from dotenv import load_dotenv

# Load environment variables from .env file
try:
    load_dotenv()
except Exception as e:
    print(f"Error loading .env file: {e}")
class Config:
    SEC_API_KEY = os.getenv("SEC_API_KEY")
    ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
    
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")





