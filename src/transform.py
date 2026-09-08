import json
from pathlib import Path
from datetime import datetime
from config import PROJECT_ROOT


def transform_weather_data(data):
    """
    Transform cleaned weather data into analysis-ready format.
    """

    timestamp = data.get("timestamp")                       # Convert timestamp from cleaned data into readable date and time

    if timestamp:
        date_time = datetime.fromtimestamp(timestamp).strftime(             # This takes the timestamp number and converts it into a Python datetime object
            "%Y-%m-%d %H:%M:%S"                                             # strftime()- Format a date/time into a string according to a specified format(%Y-%m-%d %H:%M:%S).
        )
    else:
        date_time = None

    temperature = data.get("temperature")                   # Get temperature

    if temperature is None:                                 # Create temperature category
        temperature_category = "Unknown"
    elif temperature < 15:
        temperature_category = "Cold"
    elif temperature < 25:
        temperature_category = "Moderate"
    elif temperature < 35:
        temperature_category = "Warm"
    else:
        temperature_category = "Hot"

    transformed_data = {
        "city": data.get("city", "").title(),                           #title()-Delhi
        "country": data.get("country"),

        "latitude": round(data.get("latitude"), 4)
        if data.get("latitude") is not None else None,

        "longitude": round(data.get("longitude"), 4)
        if data.get("longitude") is not None else None,

        "temperature": round(temperature, 2)                            #If: temp=31.56789 Round(2):31.57 
        if temperature is not None else None,

        "feels_like": round(data.get("feels_like"), 2)
        if data.get("feels_like") is not None else None,

        "humidity": data.get("humidity"),

        "pressure": data.get("pressure"),

        "wind_speed": round(data.get("wind_speed"), 2)
        if data.get("wind_speed") is not None else None,

        "precipitation_1h": round(data.get("precipitation_1h"), 2)
        if data.get("precipitation_1h") is not None else None,

        "weather_condition": data.get("weather_condition"),

        "weather_description": data.get("weather_description"),

        "timestamp": timestamp,

        "date_time": date_time,

        "weather_date": data.get("weather_date"),

        "temperature_category": temperature_category
    }

    return transformed_data


def transform_processed_data():

    input_file = PROJECT_ROOT / "data/processed/cleaned_weather.json"

    if not input_file.exists():
        print("Cleaned weather file not found.")
        print("Please run cleaning.py first.")
        return

    with open(input_file, "r") as file:                       # Read cleaned data
        cleaned_data = json.load(file)

    transformed_records = []

    for record in cleaned_data:                               # Transform each record

        transformed_record = transform_weather_data(record)

        transformed_records.append(transformed_record)


    output_file = PROJECT_ROOT / "data/processed/transformed_weather.json"           # Save transformed data

    with open(output_file, "w") as file:
        json.dump(transformed_records, file, indent=4)

    print("==============================")
    print("TRANSFORMATION SUMMARY")
    print("==============================")
    print(f"Records transformed: {len(transformed_records)}")
    print(f"Saved transformed data: {output_file}")


if __name__ == "__main__":
    transform_processed_data()