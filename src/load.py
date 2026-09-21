import json
from pathlib import Path
from config import PROJECT_ROOT
import mysql.connector                              # It allows Python to communicate with MySQL.
from dotenv import load_dotenv
import os


load_dotenv(PROJECT_ROOT / ".env")                                       # Its tells python to Read the .env file and make its variables available to my program


def connect_to_database():                          # Connect Python to our MySQL database
    """
    Connect to MySQL database.
    """
    host = os.getenv("MYSQL_HOST", "localhost")
    port = int(os.getenv("MYSQL_PORT", 3306))
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "")
    database = os.getenv("MYSQL_DATABASE", "weather_db")

    try:
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL database at {host}:{port}: {err}")
        raise


def load_weather_data():

    # Path to transformed JSON file
    input_file = PROJECT_ROOT / "data/processed/transformed_weather.json"

    # Check if file exists
    if not input_file.exists():
        print("Transformed weather file not found.")
        print("Please run transform.py first.")
        return

    # Read transformed data
    with open(input_file, "r") as file:
        weather_data = json.load(file)                          # Convert JSON to Python data

    connection = None
    cursor = None

    try:
        connection = connect_to_database()                          #  Again Connect to MySQL after return previously.

        cursor = connection.cursor()                                # A cursor allows Python to send SQL commands to MySQL

        # SQL INSERT query
        insert_query = """
            INSERT IGNORE INTO weather_data (
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
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
     
        inserted_count = 0
        skipped_count = 0

        # Insert each weather record
        for record in weather_data:

            values = (
                record.get("city"),
                record.get("country"),
                record.get("latitude"),
                record.get("longitude"),
                record.get("temperature"),
                record.get("feels_like"),
                record.get("humidity"),
                record.get("pressure"),
                record.get("wind_speed"),
                record.get("precipitation_1h"),
                record.get("weather_condition"),
                record.get("weather_description"),
                record.get("timestamp"),
                record.get("date_time"),
                record.get("temperature_category"),
                record.get("weather_date")
            )

            cursor.execute(insert_query, values)                        #Python takes insert_query , values and send them to MySQL.

            if cursor.rowcount == 1:
                inserted_count += 1
            else:
                skipped_count += 1

        connection.commit()                                             #Save these INSERT operations permanently.

        print("==============================")
        print("DATABASE LOAD SUMMARY")
        print("==============================")
        print(f"Records received: {len(weather_data)}")
        print(f"Records inserted: {inserted_count}")
        print(f"Records skipped: {skipped_count}")

    except Exception as error:
        if connection and connection.is_connected():
            connection.rollback()
        print(f"Database operation failed: {error}")
        raise
    finally:
        # Close connection
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


if __name__ == "__main__":
    load_weather_data()