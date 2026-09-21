import os
import sys
import argparse
from pathlib import Path
import mysql.connector
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def get_db_config():
    return {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", 3306)),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "database": os.getenv("MYSQL_DATABASE", "weather_db")
    }


def test_connection():
    config = get_db_config()
    host = config["host"]
    port = config["port"]
    user = config["user"]

    print(f"Testing MySQL connection to {host}:{port} as user '{user}'...")
    try:
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=config["password"]
        )
        cursor = connection.cursor()
        cursor.execute("SELECT VERSION();")
        version = cursor.fetchone()[0]
        cursor.close()
        connection.close()

        print(f"SUCCESS: Connected to MySQL host '{host}:{port}'. Server version: {version}")
        return True
    except mysql.connector.Error as err:
        print(f"FAILURE: Unable to connect to MySQL host '{host}:{port}': {err}")
        return False


def init_database_and_schema():
    config = get_db_config()
    host = config["host"]
    port = config["port"]
    user = config["user"]
    database = config["database"]

    print(f"Initializing database and tables on {host}:{port}...")
    try:
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=config["password"]
        )
        cursor = connection.cursor()

        # Create database if not exists
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database};")
        print(f"Database '{database}' verified.")
        cursor.execute(f"USE {database};")

        # Read create_tables.sql
        sql_file = PROJECT_ROOT / "sql/create_tables.sql"
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")

        with open(sql_file, "r") as f:
            sql_content = f.read()

        # Execute statements
        statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]
        for stmt in statements:
            cursor.execute(stmt)

        connection.commit()

        # Verify table exists
        cursor.execute("SHOW TABLES LIKE 'weather_data';")
        result = cursor.fetchone()
        if result:
            print("SUCCESS: Table 'weather_data' created and verified.")
        else:
            print("WARNING: Table 'weather_data' not found after schema execution.")

        cursor.close()
        connection.close()
        return True
    except mysql.connector.Error as err:
        print(f"FAILURE: Database initialization failed: {err}")
        return False


def run_sql_analytics():
    config = get_db_config()
    host = config["host"]
    port = config["port"]
    user = config["user"]
    database = config["database"]

    print(f"\n==============================")
    print(f"RUNNING SQL ANALYTICS ON {host}")
    print(f"==============================")

    try:
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=config["password"],
            database=database
        )
        cursor = connection.cursor()

        # 1. Total records count
        cursor.execute("SELECT COUNT(*) FROM weather_data;")
        total_records = cursor.fetchone()[0]
        print(f"\n--- 1. Total Records Count: {total_records} ---")

        if total_records == 0:
            print("No records found in weather_data yet. Run the ETL pipeline to load data.")
            cursor.close()
            connection.close()
            return True

        # 2. Average temperature by city
        print("\n--- 2. Average Temperature by City ---")
        cursor.execute("""
            SELECT city, ROUND(AVG(temperature), 2) AS avg_temp
            FROM weather_data
            GROUP BY city
            ORDER BY avg_temp DESC;
        """)
        for city, avg_temp in cursor.fetchall():
            print(f"  {city:<15}: {avg_temp}°C")

        # 3. Maximum and minimum temperature by city
        print("\n--- 3. Temperature Extremes by City ---")
        cursor.execute("""
            SELECT city, MAX(temperature) AS max_temp, MIN(temperature) AS min_temp
            FROM weather_data
            GROUP BY city;
        """)
        for city, max_t, min_t in cursor.fetchall():
            print(f"  {city:<15}: Max {max_t}°C | Min {min_t}°C")

        # 4. Average humidity by city
        print("\n--- 4. Average Humidity by City ---")
        cursor.execute("""
            SELECT city, ROUND(AVG(humidity), 2) AS avg_humidity
            FROM weather_data
            GROUP BY city
            ORDER BY avg_humidity DESC;
        """)
        for city, avg_h in cursor.fetchall():
            print(f"  {city:<15}: {avg_h}%")

        # 5. Average wind speed by city
        print("\n--- 5. Average Wind Speed by City ---")
        cursor.execute("""
            SELECT city, ROUND(AVG(wind_speed), 2) AS avg_wind
            FROM weather_data
            GROUP BY city
            ORDER BY avg_wind DESC;
        """)
        for city, avg_w in cursor.fetchall():
            print(f"  {city:<15}: {avg_w} m/s")

        # 6. Duplicate check (city, weather_date)
        print("\n--- 6. Duplicate Verification (city, weather_date) ---")
        cursor.execute("""
            SELECT city, weather_date, COUNT(*)
            FROM weather_data
            GROUP BY city, weather_date
            HAVING COUNT(*) > 1;
        """)
        duplicates = cursor.fetchall()
        if not duplicates:
            print("  SUCCESS: Zero duplicate (city, weather_date) records found.")
        else:
            print(f"  WARNING: Found {len(duplicates)} duplicate records: {duplicates}")

        cursor.close()
        connection.close()
        print(f"\n==============================")
        print("SQL ANALYTICS COMPLETED")
        print(f"==============================\n")
        return True
    except mysql.connector.Error as err:
        print(f"FAILURE: Error running analytics: {err}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database initialization and analytics verification utility")
    parser.add_argument("--test-connection", action="store_true", help="Test connection to the configured MySQL database")
    parser.add_argument("--init-schema", action="store_true", help="Create database and initialize table schema")
    parser.add_argument("--run-analytics", action="store_true", help="Run SQL analytics queries against the database")
    parser.add_argument("--all", action="store_true", help="Run test, schema init, and analytics in sequence")

    args = parser.parse_args()

    if not any([args.test_connection, args.init_schema, args.run_analytics, args.all]):
        test_connection()
        init_database_and_schema()
    else:
        if args.test_connection or args.all:
            test_connection()
        if args.init_schema or args.all:
            init_database_and_schema()
        if args.run_analytics or args.all:
            run_sql_analytics()
