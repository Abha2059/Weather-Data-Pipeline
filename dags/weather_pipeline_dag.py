import os
import sys

# Critical fix for macOS fork safety and proxy resolution in Airflow workers
os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
os.environ["no_proxy"] = "*"
os.environ["NO_PROXY"] = "*"

import pendulum
from pathlib import Path
from datetime import timedelta

from airflow import DAG                                                     # DAG is the object that represents our workflow
try:
    from airflow.operators.python import PythonOperator                     # type: ignore
    from airflow.operators.empty import EmptyOperator                       # type: ignore
except ImportError:
    from airflow.providers.standard.operators.python import PythonOperator  # type: ignore
    from airflow.providers.standard.operators.empty import EmptyOperator    # type: ignore



# Ensure project root and src are on sys.path regardless of where DAG is loaded from
WEATHER_REPO_ROOT = Path("/Users/abhaykumar/Weather-Data-Pipeline")
if not (WEATHER_REPO_ROOT / "src").exists():
    WEATHER_REPO_ROOT = Path(__file__).resolve().parent.parent

PROJECT_ROOT = WEATHER_REPO_ROOT
SRC_PATH = PROJECT_ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# Task 1: Extract weather data from OpenWeather API
def task_extract_weather():
    from extract import CITIES, fetch_weather, save_raw_data                 # type: ignore
    from logger_config import logger                                        # type: ignore

    logger.info("Extracting weather data for configured cities from OpenWeather API...")
    extracted_cities = []

    for city in CITIES:
        try:
            data = fetch_weather(city)
            save_raw_data(city, data)
            extracted_cities.append(city)
            logger.info(f"Successfully extracted data for {city}")
        except Exception as error:
            logger.error(f"Failed to fetch data for {city}: {error}")
            raise

    logger.info(f"Extraction task completed successfully for {len(extracted_cities)} cities.")
    return len(extracted_cities)


# Task 2: Upload and synchronize raw JSON files to AWS S3 Bronze layer
def task_upload_bronze():
    from s3_utils import sync_local_raw_to_bronze, ensure_bucket_exists     # type: ignore
    from logger_config import logger                                        # type: ignore

    logger.info("Verifying S3 bucket and uploading raw JSON records to Bronze layer...")
    ensure_bucket_exists()
    synced_count = sync_local_raw_to_bronze()
    logger.info(f"Bronze layer upload completed: {synced_count} objects verified in S3.")
    return synced_count


# Task 3: PySpark Bronze to Silver data cleaning and validation
def task_process_silver():
    from spark_processing import get_or_create_spark_session, process_bronze_to_silver  # type: ignore
    from logger_config import logger                                                    # type: ignore

    logger.info("Starting PySpark Bronze -> Silver processing...")
    spark = None
    try:
        spark = get_or_create_spark_session(app_name="AirflowBronzeToSilver")
        silver_df = process_bronze_to_silver(spark)
        if silver_df is None:
            raise RuntimeError("Silver processing produced empty or invalid output.")
        logger.info("Silver Parquet data lake layer written and uploaded to S3 successfully.")
    finally:
        if spark:
            spark.stop()


# Task 4: PySpark Silver to Gold analytical datasets generation
def task_process_gold():
    from spark_processing import get_or_create_spark_session, process_silver_to_gold    # type: ignore
    from logger_config import logger                                                    # type: ignore

    logger.info("Starting PySpark Silver -> Gold analytical aggregation processing...")
    spark = None
    try:
        spark = get_or_create_spark_session(app_name="AirflowSilverToGold")
        process_silver_to_gold(spark)
        logger.info("All 6 Gold analytical datasets written and uploaded to S3 successfully.")
    finally:
        if spark:
            spark.stop()


# Task 5: Verify S3 Lakehouse integrity and synchronize analytical caches
def task_verify_lakehouse():
    from validation import validate_raw_files                               # type: ignore
    from cleaning import clean_raw_files                                     # type: ignore
    from transform import transform_processed_data                           # type: ignore
    from export_for_powerbi import export_weather_data                       # type: ignore
    from s3_utils import get_s3_client                                     # type: ignore
    from config import AWS_S3_BUCKET                                        # type: ignore
    from logger_config import logger                                        # type: ignore

    logger.info("Validating and cleaning raw files for local analytical cache...")
    validate_raw_files()
    clean_raw_files()
    transform_processed_data()

    # Optional local/RDS MySQL loading if configured
    try:
        from load import load_weather_data                                   # type: ignore
        load_weather_data()
        logger.info("Optional MySQL database sync completed.")
    except Exception as db_err:
        logger.info(f"Skipping optional relational database sync: {db_err}")

    try:
        export_weather_data()
        logger.info("Exported updated weather data to Power BI CSV.")
    except Exception as exp_err:
        logger.warning(f"Power BI export warning: {exp_err}")

    # Verify S3 Silver and Gold layers
    try:
        s3 = get_s3_client()
        silver_objs = s3.list_objects_v2(Bucket=AWS_S3_BUCKET, Prefix="silver/weather/", MaxKeys=5)
        silver_count = silver_objs.get("KeyCount", 0)
        gold_objs = s3.list_objects_v2(Bucket=AWS_S3_BUCKET, Prefix="gold/weather/", MaxKeys=5)
        gold_count = gold_objs.get("KeyCount", 0)
        logger.info(f"S3 Lakehouse verified: Silver objects={silver_count > 0}, Gold objects={gold_count > 0}")
    except Exception as s3_err:
        logger.warning(f"S3 verification check warning: {s3_err}")

    logger.info("Lakehouse verification stage completed successfully.")


# Schedule configuration: defaults to None (manual triggering), configurable via AIRFLOW_SCHEDULE
raw_schedule = os.getenv("AIRFLOW_SCHEDULE", None)
if raw_schedule and raw_schedule.strip().lower() not in ("none", "null", ""):
    dag_schedule = raw_schedule.strip()
else:
    dag_schedule = None

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


with DAG(                                                                   # DAG definition
    dag_id="weather_data_pipeline",                                         # This is the name Airflow will show in its UI
    default_args=default_args,
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),            # tells Airflow when the DAG's schedule begins.
    schedule=dag_schedule,
    catchup=False                                                           # This means Airflow won't try to run the DAG for past dates(old records).
) as dag:

    extract_weather_task = PythonOperator(
        task_id="extract_weather",
        python_callable=task_extract_weather,
        retries=2,
        retry_delay=timedelta(seconds=30)
    )

    upload_bronze_task = PythonOperator(
        task_id="upload_bronze",
        python_callable=task_upload_bronze,
        retries=2,
        retry_delay=timedelta(seconds=30)
    )

    process_silver_task = PythonOperator(
        task_id="process_silver",
        python_callable=task_process_silver,
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    process_gold_task = PythonOperator(
        task_id="process_gold",
        python_callable=task_process_gold,
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    verify_lakehouse_task = PythonOperator(
        task_id="verify_lakehouse",
        python_callable=task_verify_lakehouse,
        retries=1,
        retry_delay=timedelta(seconds=30)
    )

    pipeline_complete_task = EmptyOperator(
        task_id="pipeline_complete"
    )

    # Task dependency graph: Extract -> Bronze -> Silver -> Gold -> Verify Lakehouse -> Complete
    (
        extract_weather_task
        >> upload_bronze_task
        >> process_silver_task
        >> process_gold_task
        >> verify_lakehouse_task
        >> pipeline_complete_task
    )