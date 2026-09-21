"""
Script to download a large authentic historical weather dataset.
Source: Open-Meteo Historical Weather Archive (ECMWF ERA5 Reanalysis).
Coverage: Delhi, Mumbai, Bangalore, Kolkata, Chennai (2010-01-01 to 2024-12-31).
Records: ~657,000 real hourly weather observations.
"""

import csv
import sys
import time
from pathlib import Path
import requests

# Resolve project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
HISTORICAL_DIR = PROJECT_ROOT / "data" / "historical"
OUTPUT_FILE = HISTORICAL_DIR / "weather_historical.csv"

# Target cities matching the Phase 1 pipeline
CITIES = [
    {"name": "Delhi", "lat": 28.6139, "lon": 77.2090, "country": "IN"},
    {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777, "country": "IN"},
    {"name": "Bangalore", "lat": 12.9716, "lon": 77.5946, "country": "IN"},
    {"name": "Kolkata", "lat": 22.5726, "lon": 88.3639, "country": "IN"},
    {"name": "Chennai", "lat": 13.0827, "lon": 80.2707, "country": "IN"},
]

ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"
START_DATE = "2010-01-01"
END_DATE = "2024-12-31"

FIELDNAMES = [
    "timestamp",
    "city",
    "country",
    "latitude",
    "longitude",
    "temperature",
    "humidity",
    "pressure",
    "wind_speed",
    "precipitation",
    "weather_code"
]


def get_existing_cities():
    """
    Checks if the output CSV already exists and which cities are present.
    Allows resuming interrupted downloads without re-downloading existing cities.
    """
    if not OUTPUT_FILE.exists():
        return set(), 0

    existing_cities = set()
    total_lines = 0
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_lines += 1
            city = row.get("city")
            if city:
                existing_cities.add(city)

    return existing_cities, total_lines


def fetch_city_data(city_info, max_retries: int = 5):
    """
    Fetches hourly weather data with retry and exponential backoff for rate limits.
    """
    params = {
        "latitude": city_info["lat"],
        "longitude": city_info["lon"],
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "surface_pressure",
            "wind_speed_10m",
            "precipitation",
            "weather_code"
        ],
        "timezone": "Asia/Kolkata"
    }

    delay = 10
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(ARCHIVE_API_URL, params=params, timeout=60)
            if response.status_code == 429:
                print(f"  [Rate Limit] 429 received. Backing off for {delay}s (attempt {attempt}/{max_retries})...")
                time.sleep(delay)
                delay *= 2
                continue

            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            if attempt == max_retries:
                raise e
            print(f"  [Network Warning] {e}. Retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2

    raise RuntimeError(f"Exceeded max retries for {city_info['name']}")


def download_historical_weather():
    """
    Downloads historical hourly weather data for all configured cities
    and writes them into a unified CSV file with resume capability.
    """
    HISTORICAL_DIR.mkdir(parents=True, exist_ok=True)

    existing_cities, existing_count = get_existing_cities()

    print("==================================================")
    print("HISTORICAL WEATHER DATASET DOWNLOADER")
    print("==================================================")
    print(f"Source: Open-Meteo Historical Weather Archive (ERA5)")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Cities: {', '.join(c['name'] for c in CITIES)}")
    print(f"Output File: {OUTPUT_FILE}")
    if existing_cities:
        print(f"Already downloaded cities: {', '.join(sorted(existing_cities))} ({existing_count:,} records)")
    print("==================================================\n")

    file_exists = OUTPUT_FILE.exists() and existing_count > 0

    # Open in append mode if file already exists with headers, otherwise write mode
    mode = "a" if file_exists else "w"

    with open(OUTPUT_FILE, mode, newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()

        running_total = existing_count

        for city_info in CITIES:
            city_name = city_info["name"]
            lat = city_info["lat"]
            lon = city_info["lon"]
            country = city_info["country"]

            if city_name in existing_cities:
                print(f"Skipping {city_name} (already present in dataset).")
                continue

            print(f"Fetching data for {city_name} (lat: {lat}, lon: {lon})...")

            data = fetch_city_data(city_info)

            hourly = data.get("hourly", {})
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            humidities = hourly.get("relative_humidity_2m", [])
            pressures = hourly.get("surface_pressure", [])
            wind_speeds = hourly.get("wind_speed_10m", [])
            precipitations = hourly.get("precipitation", [])
            weather_codes = hourly.get("weather_code", [])

            city_records = len(times)
            print(f"  -> Received {city_records:,} hourly records for {city_name}.")

            # Write in chunks to keep memory usage low
            batch = []
            for i in range(city_records):
                row = {
                    "timestamp": times[i],
                    "city": city_name,
                    "country": country,
                    "latitude": lat,
                    "longitude": lon,
                    "temperature": temps[i],
                    "humidity": humidities[i],
                    "pressure": pressures[i],
                    "wind_speed": wind_speeds[i],
                    "precipitation": precipitations[i],
                    "weather_code": weather_codes[i]
                }
                batch.append(row)

                if len(batch) >= 10000:
                    writer.writerows(batch)
                    batch = []

            if batch:
                writer.writerows(batch)

            running_total += city_records
            print(f"  -> Successfully written to CSV. Running total: {running_total:,} records.")

            # Polite pause to avoid hitting rate limits
            time.sleep(3)

    print("\n==================================================")
    print("DOWNLOAD COMPLETE")
    print("==================================================")
    print(f"Total authentic records available: {running_total:,}")
    print(f"Saved at: {OUTPUT_FILE}")
    print("==================================================")


if __name__ == "__main__":
    download_historical_weather()
