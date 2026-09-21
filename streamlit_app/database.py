import os
import sys
import logging
from pathlib import Path
import mysql.connector
from mysql.connector import Error as MySQLError
import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

logger = logging.getLogger("streamlit_weather_db")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s in %(module)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def _get_secret(key: str, default: str = "") -> str:
    """
    Safely retrieves a configuration value from Streamlit secrets or environment variables.
    Supports both flat keys (e.g. MYSQL_HOST) and nested tables (e.g. [mysql] host).
    """
    try:
        import streamlit as st
        if hasattr(st, "secrets"):
            if key in st.secrets:
                return str(st.secrets[key])
            if "mysql" in st.secrets:
                lower_key = key.replace("MYSQL_", "").lower()
                if lower_key in st.secrets["mysql"]:
                    return str(st.secrets["mysql"][lower_key])
    except Exception:
        pass
    return os.getenv(key, default)


def get_db_config():
    """
    Retrieves database connection configuration from Streamlit secrets or environment.
    Supports AWS RDS MySQL and local MySQL fallback.
    """
    host = _get_secret("MYSQL_HOST", "localhost")
    port_str = _get_secret("MYSQL_PORT", "3306")
    user = _get_secret("MYSQL_USER", "root")
    password = _get_secret("MYSQL_PASSWORD", "")
    database = _get_secret("MYSQL_DATABASE", "weather_db")

    try:
        port = int(port_str)
    except (ValueError, TypeError):
        port = 3306

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
        "connect_timeout": 10
    }


def get_connection():
    """
    Establishes and returns a fresh MySQL connection using environment settings.
    """
    config = get_db_config()
    try:
        connection = mysql.connector.connect(**config)
        return connection
    except MySQLError as err:
        logger.error(f"Database connection failed to host {config['host']}:{config['port']} (user: {config['user']}): {err}")
        raise


def test_db_connection():
    """
    Verifies database connectivity with a lightweight ping query.
    Returns (success: bool, message: str, version: str).
    """
    config = get_db_config()
    connection = None
    cursor = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT VERSION();")
        version_row = cursor.fetchone()
        version = version_row[0] if version_row else "Unknown"
        return True, f"Connected to {config['host']}", version
    except Exception as err:
        return False, str(err), "N/A"
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def run_query(query: str, params: tuple | list | None = None) -> pd.DataFrame:
    """
    Executes a parameterized SQL query against MySQL and returns the results
    as a clean Pandas DataFrame. Ensures connections are strictly closed.
    """
    connection = None
    cursor = None
    try:
        connection = get_connection()
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, params or ())
        records = cursor.fetchall()
        df = pd.DataFrame(records)
        return df
    except MySQLError as err:
        logger.error(f"SQL execution error: {err}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
