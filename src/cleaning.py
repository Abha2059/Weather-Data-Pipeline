import json
from pathlib import Path
from datetime import datetime

from config import PROJECT_ROOT


def clean_weather_data(data):
    """
    Extract and clean useful fields from one raw weather API response.
    """

    timestamp = data.get("dt")                                             #The API gives a Unix timestamp such as: 1609459200 - Later we convert it into proper date and time.

    if timestamp:
        date_time = datetime.fromtimestamp(timestamp)
        weather_date = date_time.date().isoformat()
    else:
        date_time = None
        weather_date = None

    cleaned_data = {
        "city": data.get("name"),                                           # we are using get() method to avoid KeyError if the key doesn't exist in the dictionary it will return None.
        "country": data.get("sys", {}).get("country"),                      # If sys doesn't exist, use an empty dictionary {} instead of crashing
        "latitude": data.get("coord", {}).get("lat"),
        "longitude": data.get("coord", {}).get("lon"),
        "temperature": data.get("main", {}).get("temp"),
        "feels_like": data.get("main", {}).get("feels_like"),
        "humidity": data.get("main", {}).get("humidity"),
        "pressure": data.get("main", {}).get("pressure"),
        "wind_speed": data.get("wind", {}).get("speed"),
        "precipitation_1h": data.get("rain", {}).get("1h"),
        "weather_condition": data.get("weather", [{}])[0].get("main"),          #[0] Get the first element of the list. 
        "weather_description": data.get("weather", [{}])[0].get("description"),
        "timestamp": timestamp,                                             #The API gives a Unix timestamp such as: 1609459200 -Later we convert it in proper(Y M D)transformation stage.
        "weather_date": weather_date
    }

    return cleaned_data


def clean_raw_files():

    raw_directory = PROJECT_ROOT / "data/raw"

    json_files = list(raw_directory.glob("**/*.json"))

    if not json_files:
        print("No JSON files found in data/raw/")
        return

    cleaned_records = []

    for file_path in json_files:

        print(f"Cleaning: {file_path}")

        try:

            with open(file_path, "r") as file:
                data = json.load(file)

            cleaned_data = clean_weather_data(data)

            cleaned_records.append(cleaned_data)

        except json.JSONDecodeError:
            print(f"Invalid JSON file: {file_path}")

        except Exception as error:
            print(f"Error cleaning {file_path}: {error}")


    processed_directory = PROJECT_ROOT / "data/processed"               # tells Python where we want our cleaned data

    processed_directory.mkdir(                                  # creates the folder if it doesn't already exist
        parents=True,
        exist_ok=True
    )

   
    output_file = processed_directory / "cleaned_weather.json"          # creates the name of the output file after cleaning all the raw files. EX - data/processed/cleaned_weather.json

    with open(output_file, "w") as file:
        json.dump(cleaned_records, file, indent=4)

    print("\n==============================")
    print("CLEANING SUMMARY")
    print("==============================")
    print(f"Records cleaned: {len(cleaned_records)}")
    print(f"Saved cleaned data: {output_file}")


if __name__ == "__main__":
    clean_raw_files()