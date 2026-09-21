"""
PySpark processing pipeline for the Data Lake architecture.
Transforms Bronze raw weather JSON into cleaned Silver Parquet data,
and aggregates Silver data into analytical Gold Parquet datasets.
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

from config import PROJECT_ROOT, AWS_S3_BUCKET
from logger_config import logger
from s3_utils import (
    is_s3_available,
    ensure_bucket_exists,
    upload_directory,
    list_s3_objects,
    read_json_from_s3,
    sync_local_raw_to_bronze
)

# Local staging paths
LOCAL_RAW_DIR = PROJECT_ROOT / "data" / "raw"
LOCAL_SILVER_DIR = PROJECT_ROOT / "data" / "processed" / "silver_weather"
LOCAL_GOLD_DIR = PROJECT_ROOT / "data" / "processed" / "gold_weather"


def get_or_create_spark_session(app_name: str = "WeatherDataLakeProcessing"):
    """
    Initializes a local SparkSession optimized for macOS and data lake operations.
    """
    from pyspark.sql import SparkSession

    jvm_opens = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.extraJavaOptions", jvm_opens)
        .config("spark.executor.extraJavaOptions", jvm_opens)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def collect_bronze_records() -> List[Dict[str, Any]]:
    """
    Collects raw JSON records from S3 Bronze if available,
    falling back to or combining with local data/raw JSON files.
    """
    records: List[Dict[str, Any]] = []

    # Check S3 Bronze first if accessible
    s3_bronze_keys = list_s3_objects("bronze/weather/")
    if s3_bronze_keys:
        logger.info(f"Found {len(s3_bronze_keys)} Bronze records in S3.")
        for key in s3_bronze_keys:
            if key.endswith(".json"):
                data = read_json_from_s3(key)
                if data:
                    records.append(data)

    # If no records in S3 or offline, read local raw JSON directory
    if not records and LOCAL_RAW_DIR.exists():
        logger.info("Reading Bronze data from local data/raw directory...")
        for json_file in LOCAL_RAW_DIR.glob("**/*.json"):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    records.append(data)
            except Exception as e:
                logger.error(f"Error reading {json_file}: {e}")

    logger.info(f"Total Bronze records collected: {len(records)}")
    return records


def parse_raw_record(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extracts and standardizes useful fields from a raw OpenWeatherMap API dictionary.
    """
    if not raw or not isinstance(raw, dict):
        return None

    city = raw.get("name")
    if not city:
        return None

    dt_timestamp = raw.get("dt")
    if dt_timestamp:
        dt_obj = datetime.fromtimestamp(dt_timestamp)
        date_time_str = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
        weather_date_str = dt_obj.strftime("%Y-%m-%d")
        year_val = dt_obj.year
        month_val = dt_obj.month
        day_val = dt_obj.day
    else:
        date_time_str = None
        weather_date_str = None
        year_val = None
        month_val = None
        day_val = None

    temp = raw.get("main", {}).get("temp")
    if temp is None:
        temp_cat = "Unknown"
    elif temp < 15:
        temp_cat = "Cold"
    elif temp < 25:
        temp_cat = "Moderate"
    elif temp < 35:
        temp_cat = "Warm"
    else:
        temp_cat = "Hot"

    return {
        "city": str(city).title(),
        "country": raw.get("sys", {}).get("country"),
        "latitude": float(raw.get("coord", {}).get("lat")) if raw.get("coord", {}).get("lat") is not None else None,
        "longitude": float(raw.get("coord", {}).get("lon")) if raw.get("coord", {}).get("lon") is not None else None,
        "temperature": float(temp) if temp is not None else None,
        "feels_like": float(raw.get("main", {}).get("feels_like")) if raw.get("main", {}).get("feels_like") is not None else None,
        "humidity": int(raw.get("main", {}).get("humidity")) if raw.get("main", {}).get("humidity") is not None else None,
        "pressure": int(raw.get("main", {}).get("pressure")) if raw.get("main", {}).get("pressure") is not None else None,
        "wind_speed": float(raw.get("wind", {}).get("speed")) if raw.get("wind", {}).get("speed") is not None else None,
        "precipitation_1h": float(raw.get("rain", {}).get("1h", 0.0)) if raw.get("rain") else 0.0,
        "weather_condition": raw.get("weather", [{}])[0].get("main") if raw.get("weather") else None,
        "weather_description": raw.get("weather", [{}])[0].get("description") if raw.get("weather") else None,
        "timestamp": int(dt_timestamp) if dt_timestamp else None,
        "date_time": date_time_str,
        "weather_date": weather_date_str,
        "year": year_val,
        "month": month_val,
        "day": day_val,
        "temperature_category": temp_cat
    }


def process_bronze_to_silver(spark) -> Optional[Any]:
    """
    Reads Bronze records, performs PySpark transformations and validation,
    and writes partitioned Parquet data to the Silver layer (local + S3).
    """
    from pyspark.sql.functions import col, to_date, to_timestamp, round as spark_round
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, IntegerType, LongType
    )

    logger.info("Starting Bronze -> Silver PySpark processing...")

    bronze_records = collect_bronze_records()
    if not bronze_records:
        logger.warning("No Bronze records found to process into Silver.")
        return None

    parsed_records = []
    for raw in bronze_records:
        parsed = parse_raw_record(raw)
        if parsed:
            parsed_records.append(parsed)

    if not parsed_records:
        logger.warning("No valid records could be parsed from Bronze data.")
        return None

    schema = StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("feels_like", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("precipitation_1h", DoubleType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("weather_description", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("date_time", StringType(), True),
        StructField("weather_date", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("temperature_category", StringType(), True),
    ])

    df = spark.createDataFrame(parsed_records, schema=schema)

    # Cast date and timestamp fields to formal temporal types
    df = (
        df
        .withColumn("date_time", to_timestamp(col("date_time"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("weather_date", to_date(col("weather_date"), "yyyy-MM-dd"))
        .withColumn("temperature", spark_round(col("temperature"), 2))
        .withColumn("feels_like", spark_round(col("feels_like"), 2))
        .withColumn("wind_speed", spark_round(col("wind_speed"), 2))
        .withColumn("precipitation_1h", spark_round(col("precipitation_1h"), 2))
    )

    # Deduplication
    initial_count = df.count()
    silver_df = df.dropDuplicates(["city", "weather_date"])
    dedup_count = silver_df.count()
    duplicates_removed = initial_count - dedup_count
    logger.info(f"Deduplication: {initial_count} -> {dedup_count} ({duplicates_removed} duplicates removed)")

    # Data quality validation
    silver_df = silver_df.filter(
        (col("city").isNotNull()) &
        (col("temperature").isNotNull()) &
        (col("weather_date").isNotNull()) &
        (col("humidity") >= 0) & (col("humidity") <= 100) &
        (col("pressure") > 0) &
        (col("wind_speed") >= 0)
    )

    final_silver_count = silver_df.count()
    logger.info(f"Silver records after quality validation: {final_silver_count}")

    # Write locally partitioned by year, month, day
    LOCAL_SILVER_DIR.mkdir(parents=True, exist_ok=True)
    (
        silver_df
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(str(LOCAL_SILVER_DIR))
    )
    logger.info(f"Silver Parquet written to {LOCAL_SILVER_DIR}")

    # Upload to S3 Silver layer
    upload_directory(LOCAL_SILVER_DIR, "silver/weather")

    return silver_df


def process_silver_to_gold(spark, silver_df=None):
    """
    Reads Silver Parquet data and creates 6 analytical Gold Parquet datasets.
    """
    from pyspark.sql.functions import (
        col, avg, max as spark_max, min as spark_min, sum as spark_sum,
        coalesce, lit, round as spark_round
    )

    logger.info("Starting Silver -> Gold PySpark processing...")

    if silver_df is None:
        if not LOCAL_SILVER_DIR.exists():
            logger.warning(f"Silver path {LOCAL_SILVER_DIR} not found. Run Silver processing first.")
            return
        silver_df = spark.read.parquet(str(LOCAL_SILVER_DIR))

    LOCAL_GOLD_DIR.mkdir(parents=True, exist_ok=True)

    # 1. City Temperature Summary
    gold_city_temp = (
        silver_df
        .groupBy("city")
        .agg(
            spark_round(avg("temperature"), 2).alias("average_temperature"),
            spark_round(spark_max("temperature"), 2).alias("maximum_temperature"),
            spark_round(spark_min("temperature"), 2).alias("minimum_temperature")
        )
        .orderBy("city")
    )
    path_city_temp = LOCAL_GOLD_DIR / "city_temperature"
    gold_city_temp.write.mode("overwrite").parquet(str(path_city_temp))
    upload_directory(path_city_temp, "gold/weather/city_temperature")
    logger.info("Gold dataset created: city_temperature")

    # 2. Monthly Temperature Trend
    gold_monthly_temp = (
        silver_df
        .groupBy("year", "month", "city")
        .agg(
            spark_round(avg("temperature"), 2).alias("average_temperature"),
            spark_round(spark_max("temperature"), 2).alias("maximum_temperature"),
            spark_round(spark_min("temperature"), 2).alias("minimum_temperature")
        )
        .orderBy("year", "month", "city")
    )
    path_monthly_temp = LOCAL_GOLD_DIR / "monthly_temperature"
    gold_monthly_temp.write.mode("overwrite").parquet(str(path_monthly_temp))
    upload_directory(path_monthly_temp, "gold/weather/monthly_temperature")
    logger.info("Gold dataset created: monthly_temperature")

    # 3. City Weather Summary
    gold_city_weather = (
        silver_df
        .groupBy("city")
        .agg(
            spark_round(avg("temperature"), 2).alias("average_temperature"),
            spark_round(avg("humidity"), 2).alias("average_humidity"),
            spark_round(avg("wind_speed"), 2).alias("average_wind_speed"),
            spark_round(spark_sum(coalesce(col("precipitation_1h"), lit(0.0))), 2).alias("total_precipitation")
        )
        .orderBy("city")
    )
    path_city_weather = LOCAL_GOLD_DIR / "city_weather_summary"
    gold_city_weather.write.mode("overwrite").parquet(str(path_city_weather))
    upload_directory(path_city_weather, "gold/weather/city_weather_summary")
    logger.info("Gold dataset created: city_weather_summary")

    # 4. Humidity Trends
    gold_humidity = (
        silver_df
        .groupBy("city", "weather_date")
        .agg(
            spark_round(avg("humidity"), 2).alias("average_humidity")
        )
        .orderBy("weather_date", "city")
    )
    path_humidity = LOCAL_GOLD_DIR / "humidity_trends"
    gold_humidity.write.mode("overwrite").parquet(str(path_humidity))
    upload_directory(path_humidity, "gold/weather/humidity_trends")
    logger.info("Gold dataset created: humidity_trends")

    # 5. Wind Trends
    gold_wind = (
        silver_df
        .groupBy("city", "weather_date")
        .agg(
            spark_round(avg("wind_speed"), 2).alias("average_wind_speed")
        )
        .orderBy("weather_date", "city")
    )
    path_wind = LOCAL_GOLD_DIR / "wind_trends"
    gold_wind.write.mode("overwrite").parquet(str(path_wind))
    upload_directory(path_wind, "gold/weather/wind_trends")
    logger.info("Gold dataset created: wind_trends")

    # 6. Rainfall / Precipitation Trends
    gold_rainfall = (
        silver_df
        .groupBy("city", "weather_date")
        .agg(
            spark_round(spark_sum(coalesce(col("precipitation_1h"), lit(0.0))), 2).alias("total_precipitation")
        )
        .orderBy("weather_date", "city")
    )
    path_rainfall = LOCAL_GOLD_DIR / "rainfall_trends"
    gold_rainfall.write.mode("overwrite").parquet(str(path_rainfall))
    upload_directory(path_rainfall, "gold/weather/rainfall_trends")
    logger.info("Gold dataset created: rainfall_trends")

    logger.info("All 6 Gold analytical datasets successfully processed.")


def run_spark_pipeline():
    """
    Orchestrates the complete PySpark Bronze -> Silver -> Gold pipeline.
    """
    logger.info("==============================")
    logger.info("DATA LAKE PYSPARK JOB STARTED")
    logger.info("==============================")

    # Verify or initialize S3 bucket
    ensure_bucket_exists()

    # Sync any existing local raw JSON files to S3 Bronze
    sync_local_raw_to_bronze()

    spark = get_or_create_spark_session()

    try:
        silver_df = process_bronze_to_silver(spark)
        if silver_df is not None:
            process_silver_to_gold(spark, silver_df)
        logger.info("==============================")
        logger.info("DATA LAKE PYSPARK JOB COMPLETED")
        logger.info("==============================")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_spark_pipeline()
