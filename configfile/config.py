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
    DATABASE_URL = os.getenv("DATABASE_URL")







