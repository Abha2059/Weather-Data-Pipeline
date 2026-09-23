import os     #Allows Python to work with environment variables.

# Critical macOS fork safety settings for background processes and Airflow workers
os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
os.environ["no_proxy"] = "*"
os.environ["NO_PROXY"] = "*"

from dotenv import load_dotenv     #Imports the function that reads our .env file.
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(ENV_FILE)   #This loads the values stored in .env. 
    

WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")      #This stores the API key from the environment.  

if not WEATHER_API_KEY:
    raise ValueError("WEATHER_API_KEY is not set in the .env file")

# AWS S3 Configuration
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "weather-data-pipeline-abhay")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

# Database Configuration (Local MySQL and AWS RDS)
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "weather_db")