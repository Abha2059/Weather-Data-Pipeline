import json                                                              #Python needs this to save our dictionary as a JSON file.
from pathlib import Path                                                 #Path helps Python work with folders and file paths.
from datetime import datetime


import requests                                                          #requests allows Python to communicate with the REST API using HTTP.

from config import WEATHER_API_KEY, PROJECT_ROOT


BASE_URL = "https://api.openweathermap.org/data/2.5/weather"             # This help python program to communicate with the OpenWeatherMap API and get weather details.


CITIES = [
    "Delhi",
    "Mumbai",
    "Bangalore",
    "Kolkata",
    "Chennai"
]


def fetch_weather(city):                                            # Create the API request
    params = {                                                      # This creates the API query.
        "q": city,
        "appid": WEATHER_API_KEY,
        "units": "metric"                                           # means temperature is returned in Celsius
    }

    response = requests.get(                                        # Python sends a GET request.
        BASE_URL,
        params=params,
        timeout=10                                                 # means Python won't wait indefinitely if the API doesn't respond.
    )

    response.raise_for_status()                                   #Check API response-If there's an HTTP error, raises an exception

    return response.json()                                         #The API returns JSON. Python converts the JSON response into a Python dictionary.


def save_raw_data(city, data):
    today = datetime.now().strftime("%Y-%m-%d")

    raw_directory = PROJECT_ROOT / "data/raw" / today

    raw_directory.mkdir(                                                #doesn't exist folder, Python creates it.
        parents=True,
        exist_ok=True
    )

    file_name = city.lower().replace(" ", "_") + ".json"

    file_path = raw_directory / file_name                                 #EX - data/raw/2026-09-04/delhi.json

    with open(file_path, "w") as file:                                   #This writes the API response to the file
        json.dump(data, file, indent=4)                                  #indent - instead of putting everything on one line.

    print(f"Saved raw data: {file_path}")

    try:
        from s3_utils import upload_json_to_bronze
        upload_json_to_bronze(city, data, today)
    except Exception as s3_err:
        pass


if __name__ == "__main__":

    for city in CITIES:

        try:                                                             #We don't want 1 city failure to stop the entire pipeline.
            data = fetch_weather(city)

            save_raw_data(city, data)

        except requests.exceptions.RequestException as error:
            print(f"Failed to fetch weather for {city}: {error}")