import csv                                                                  # csv allows Python to create a CSV file
import os                                                                   # os reads .env variables
import mysql.connector                                                      # allows Python to communicate with MySQL

from pathlib import Path
from dotenv import load_dotenv

from config import PROJECT_ROOT                                             # config.py already knows where your project root is


load_dotenv(PROJECT_ROOT / ".env")                                          # loads .env file


def connect_to_database():                                                  # create connection to mySQL to weather_db.
    """
    Connect Python to MySQL database.
    """

    connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )

    return connection


def export_weather_data():


    connection = connect_to_database()

    cursor = connection.cursor(dictionary=True)

    # Fetch all weather records
    query = """
        SELECT
            id,
            city,
            country,
            latitude,
            longitude,
            temperature,
            feels_like,
            humidity,
            pressure,
            wind_speed,
            precipitation_1h,
            weather_condition,
            weather_description,
            timestamp,
            date_time,
            temperature_category,
            weather_date
        FROM weather_data
        ORDER BY weather_date, city
    """

    cursor.execute(query)

    weather_data = cursor.fetchall()

    
    output_directory = PROJECT_ROOT / "data/powerbi"

    output_directory.mkdir(
        parents=True,
        exist_ok=True
    )


    output_file = output_directory / "weather_data.csv"

    # Write data to CSV
    if weather_data:

        fieldnames = weather_data[0].keys()

        with open(output_file, "w", newline="") as file:

            writer = csv.DictWriter(
                file,
                fieldnames=fieldnames
            )

            writer.writeheader()                                            # writes-id,city,country,latitude,...
            writer.writerows(weather_data)                                  # writes all mysql data to CSV file

    else:

        print("No weather records found in MySQL.")

    cursor.close()
    connection.close()

    print("==============================")
    print("POWER BI EXPORT SUMMARY")
    print("==============================")
    print(f"Records exported: {len(weather_data)}")
    print(f"CSV file: {output_file}")


if __name__ == "__main__":
    export_weather_data()