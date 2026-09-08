import json
from pathlib import Path

from config import PROJECT_ROOT


def validate_weather_data(data):
    """
    Validate a single weather API response.
    Returns True if the data is valid, otherwise False.
    """

    # Check city name
    if not data.get("name"):
        print("Validation failed: city name is missing")
        return False

    # Check main weather information
    if "main" not in data:
        print("Validation failed: main weather data is missing")
        return False

    main = data["main"]

    # Check temperature
    if "temp" not in main:
        print("Validation failed: temperature is missing")
        return False

    # Check humidity
    if "humidity" not in main:
        print("Validation failed: humidity is missing")
        return False

    humidity = main["humidity"]

    if not 0 <= humidity <= 100:
        print(f"Validation failed: invalid humidity: {humidity}")
        return False

    # Check pressure
    if "pressure" not in main:
        print("Validation failed: pressure is missing")
        return False

    pressure = main["pressure"]

    if pressure <= 0:                               #Pressure cant be negative or zero.
        print(f"Validation failed: invalid pressure: {pressure}")
        return False

    # Check wind information
    if "wind" not in data:
        print("Validation failed: wind data is missing")
        return False

    if "speed" not in data["wind"]:
        print("Validation failed: wind speed is missing")
        return False

    wind_speed = data["wind"]["speed"]

    if wind_speed < 0:
        print(f"Validation failed: invalid wind speed: {wind_speed}")
        return False

    return True


def validate_raw_files():
    """
    Read all raw JSON files and validate them.
    """

    raw_directory = PROJECT_ROOT / "data/raw"

    json_files = list(raw_directory.glob("**/*.json"))              # ("**/*.json") -Find JSON files inside data/raw folder and its subfolders

    if not json_files:
        print("No JSON files found in data/raw/")
        return

    valid_count = 0
    invalid_count = 0

    for file_path in json_files:

        print(f"\nChecking: {file_path}")

        try:
            with open(file_path, "r") as file:                      # with - "Python automatically closes the file after the indented code finishes."
                data = json.load(file)                              # Read JSON data from an opened file and convert it into a Python dictionary/object, and store the result in data.

            if validate_weather_data(data):
                print("Status: VALID")
                valid_count += 1
            else:
                print("Status: INVALID")
                invalid_count += 1

        except json.JSONDecodeError:
            print("Status: INVALID - JSON file is corrupted")
            invalid_count += 1

        except Exception as error:
            print(f"Error reading file: {error}")
            invalid_count += 1

    print("\n==============================")
    print("VALIDATION SUMMARY")
    print("==============================")
    print(f"Valid files: {valid_count}")
    print(f"Invalid files: {invalid_count}")
    print(f"Total files: {valid_count + invalid_count}")


if __name__ == "__main__":
    validate_raw_files()