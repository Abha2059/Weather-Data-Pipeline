import os     #Allows Python to work with environment variables.
from dotenv import load_dotenv     #Imports the function that reads our .env file.
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(ENV_FILE)   #This loads the values stored in .env. 
    

WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")      #This stores the API key from the environment.  

if not WEATHER_API_KEY:
    raise ValueError("WEATHER_API_KEY is not set in the .env file")