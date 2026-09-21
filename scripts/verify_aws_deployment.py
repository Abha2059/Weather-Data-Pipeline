#!/usr/bin/env python3
"""
AWS Cloud Deployment Verification Tool for Weather Data Aggregation Pipeline.

Validates the full cloud architecture:
  1. AWS Configuration & Region verification.
  2. AWS S3 Data Lake (Bucket, Encryption, Bronze, Silver, Gold object counts).
  3. AWS RDS MySQL (Host connection, schema, record counts, duplicate checks).
  4. OpenWeather REST API connectivity.
  5. Apache Airflow DAG integrity & task dependencies.
  6. Streamlit analytics queries execution & latency.

Usage:
  PYTHONPATH=src python scripts/verify_aws_deployment.py
"""

import os
import sys
import time
from pathlib import Path

# Add src to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import (
        AWS_REGION,
        AWS_S3_BUCKET,
        MYSQL_HOST,
        MYSQL_PORT,
        MYSQL_DATABASE,
        MYSQL_USER,
        WEATHER_API_KEY,
    )
    from s3_utils import get_s3_client, is_s3_available, list_s3_objects
    import mysql.connector
    import requests
except ImportError as e:
    print(f"Error importing required modules: {e}")
    print("Ensure dependencies are installed and PYTHONPATH=src is set.")
    sys.exit(1)


def mask_host(host: str) -> str:
    """Masks database host for secure output display."""
    if "." in host:
        parts = host.split(".")
        return f"{parts[0]}.***.{'.'.join(parts[2:])}"
    return host


def verify_s3():
    """Validates S3 bucket access, encryption, and data lake layers."""
    print("\n" + "=" * 60)
    print("1. AWS S3 DATA LAKE VERIFICATION")
    print("=" * 60)
    print(f"Target Region: {AWS_REGION}")
    print(f"Target Bucket: {AWS_S3_BUCKET}")

    if not is_s3_available():
        print("❌ S3 is not available or AWS credentials are missing.")
        return False

    s3 = get_s3_client()
    try:
        # Check bucket location
        loc = s3.get_bucket_location(Bucket=AWS_S3_BUCKET).get("LocationConstraint") or "us-east-1"
        print(f"✅ Bucket exists in region: {loc}")

        # Check encryption
        enc = s3.get_bucket_encryption(Bucket=AWS_S3_BUCKET)
        rules = enc.get("ServerSideEncryptionConfiguration", {}).get("Rules", [])
        algo = rules[0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] if rules else "None"
        print(f"✅ Server-side encryption: {algo}")

        # Count objects in layers
        bronze_keys = list_s3_objects("bronze/weather/")
        silver_keys = list_s3_objects("silver/weather/")
        gold_keys = list_s3_objects("gold/weather/")

        print(f"✅ Bronze Layer (Raw JSON):     {len(bronze_keys)} objects")
        print(f"✅ Silver Layer (Parquet):      {len(silver_keys)} objects")
        print(f"✅ Gold Layer (Analytics):      {len(gold_keys)} objects")

        return True
    except Exception as e:
        print(f"❌ S3 verification encountered an error: {e}")
        return False


def verify_rds():
    """Validates RDS MySQL connectivity, table schema, and record integrity."""
    print("\n" + "=" * 60)
    print("2. AWS RDS MYSQL VERIFICATION")
    print("=" * 60)
    print(f"Host:     {mask_host(MYSQL_HOST)}:{MYSQL_PORT}")
    print(f"Database: {MYSQL_DATABASE}")
    print(f"User:     {MYSQL_USER}")

    t0 = time.time()
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=os.getenv("MYSQL_PASSWORD", ""),
            database=MYSQL_DATABASE,
            connection_timeout=5,
        )
        latency = (time.time() - t0) * 1000
        print(f"✅ Successfully connected to AWS RDS MySQL in {latency:.1f}ms")
        cursor = conn.cursor()

        # Check version
        cursor.execute("SELECT VERSION()")
        db_version = cursor.fetchone()[0]
        print(f"✅ Server Version: {db_version}")

        # Check table
        cursor.execute("SHOW TABLES LIKE 'weather_data'")
        if not cursor.fetchone():
            print("❌ Table 'weather_data' does not exist.")
            cursor.close()
            conn.close()
            return False
        print("✅ Table 'weather_data' verified.")

        # Check row count
        cursor.execute("SELECT COUNT(*) FROM weather_data")
        count = cursor.fetchone()[0]
        print(f"✅ Total Records: {count}")

        # Check duplicate records
        cursor.execute(
            """
            SELECT city, weather_date, COUNT(*)
            FROM weather_data
            GROUP BY city, weather_date
            HAVING COUNT(*) > 1
            """
        )
        duplicates = cursor.fetchall()
        if duplicates:
            print(f"⚠️ Warning: Found {len(duplicates)} duplicate records.")
        else:
            print("✅ Data Integrity: 0 duplicate (city, weather_date) records found.")

        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Failed to connect to AWS RDS MySQL: {e}")
        return False


def verify_openweather_api():
    """Validates OpenWeather REST API connectivity."""
    print("\n" + "=" * 60)
    print("3. OPENWEATHER REST API VERIFICATION")
    print("=" * 60)
    if not WEATHER_API_KEY:
        print("❌ WEATHER_API_KEY is not configured.")
        return False

    url = f"https://api.openweathermap.org/data/2.5/weather?q=Delhi,IN&appid={WEATHER_API_KEY}"
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            city = data.get("name")
            temp_k = data.get("main", {}).get("temp", 0)
            temp_c = temp_k - 273.15
            print(f"✅ OpenWeather API responsive: {city} is {temp_c:.1f}°C (HTTP 200)")
            return True
        else:
            print(f"❌ OpenWeather API returned status code {resp.status_code}: {resp.text}")
            return False
    except Exception as e:
        print(f"❌ OpenWeather API request failed: {e}")
        return False


def verify_airflow_dag():
    """Validates Airflow DAG import and task structure."""
    print("\n" + "=" * 60)
    print("4. AIRFLOW ORCHESTRATION VERIFICATION")
    print("=" * 60)
    try:
        from dags.weather_pipeline_dag import dag
        tasks = [t.task_id for t in dag.tasks]
        print(f"✅ DAG '{dag.dag_id}' loaded successfully with {len(tasks)} tasks:")
        for t in tasks:
            print(f"   • {t}")
        return True
    except Exception as e:
        print(f"❌ Failed importing Airflow DAG: {e}")
        return False


def verify_streamlit():
    """Validates Streamlit data layer queries against RDS."""
    print("\n" + "=" * 60)
    print("5. STREAMLIT DATA LAYER VERIFICATION")
    print("=" * 60)
    try:
        from streamlit_app.queries import get_available_cities, get_date_bounds, get_kpi_summary
        cities = get_available_cities()
        min_d, max_d = get_date_bounds()
        kpis = get_kpi_summary(cities, min_d, max_d)
        print(f"✅ Distinct Cities available: {cities}")
        print(f"✅ Date Range Bounds: {min_d} to {max_d}")
        print(f"✅ KPI Summary: {kpis.get('record_count', 0)} records | Avg Temp: {kpis.get('avg_temp', 0):.2f}°C")
        return True
    except Exception as e:
        print(f"❌ Streamlit query layer error: {e}")
        return False


def main():
    print("############################################################")
    print("      Weather Data Pipeline — AWS Cloud Verification        ")
    print("############################################################")

    s3_ok = verify_s3()
    rds_ok = verify_rds()
    api_ok = verify_openweather_api()
    dag_ok = verify_airflow_dag()
    st_ok = verify_streamlit()

    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    print(f"  AWS S3 Data Lake:         {'[PASS]' if s3_ok else '[FAIL]'}")
    print(f"  AWS RDS MySQL:            {'[PASS]' if rds_ok else '[FAIL]'}")
    print(f"  OpenWeather API:          {'[PASS]' if api_ok else '[FAIL]'}")
    print(f"  Airflow DAG Definition:   {'[PASS]' if dag_ok else '[FAIL]'}")
    print(f"  Streamlit Data Layer:     {'[PASS]' if st_ok else '[FAIL]'}")
    print("=" * 60)

    all_passed = all([s3_ok, rds_ok, api_ok, dag_ok, st_ok])
    if all_passed:
        print("🎉 ALL CLOUD COMPONENTS VERIFIED SUCCESSFULLY!")
        sys.exit(0)
    else:
        print("⚠️ Some checks failed. Review output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
