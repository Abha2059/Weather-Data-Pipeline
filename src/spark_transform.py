"""
Phase 2: Big Data Processing with PySpark.
-----------------------------------------
This module performs distributed cleaning, transformation, and partitioning
of large historical weather datasets using Apache PySpark.

Output: High-performance columnar Parquet files partitioned by year and month.
Location: data/processed/weather_parquet/
"""

import os
import sys
from pathlib import Path

# Set up project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = PROJECT_ROOT / "data" / "historical" / "weather_historical.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "weather_parquet"


def get_or_create_spark_session(app_name: str = "WeatherHistoricalBigData"):
    """
    Creates and configures a local PySpark session.
    Configures JVM memory, local parallelism, and Java options for macOS.
    """
    # Prefer Java 17 if available on arm64/x86, otherwise let system JAVA_HOME resolve
    java_17 = "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
    if "JAVA_HOME" not in os.environ and os.path.isdir(java_17):
        # Only set if architecture compatible or default
        pass

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
        .master("local[*]")  # Utilize all available CPU cores on local machine
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.extraJavaOptions", jvm_opens)
        .config("spark.executor.extraJavaOptions", jvm_opens)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    # Set logging level to WARN to minimize verbose Spark internals in console
    spark.sparkContext.setLogLevel("WARN")

    return spark


def process_weather_data():
    """
    Main PySpark ETL routine:
    1. Reads large historical CSV
    2. Cleans nulls, casts data types, and deduplicates records
    3. Transforms metrics (temperature category, dates, wind speed)
    4. Validates data quality
    5. Writes partitioned Parquet files
    """
    print("==================================================")
    print("PHASE 2: PYSPARK HISTORICAL WEATHER PROCESSING")
    print("==================================================")

    if not INPUT_PATH.exists():
        print(f"[ERROR] Historical dataset not found at: {INPUT_PATH}")
        print("Please run 'python src/download_historical_data.py' first.")
        sys.exit(1)

    spark = get_or_create_spark_session()
    print(f"SparkSession created successfully (Spark version: {spark.version})")
    print(f"Reading input data from: {INPUT_PATH}\n")

    from pyspark.sql.functions import (
        col,
        to_timestamp,
        to_date,
        year,
        month,
        dayofmonth,
        round as spark_round,
        when,
        count,
        min as spark_min,
        max as spark_max,
        countDistinct
    )
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType
    )

    # Step 1: Read raw CSV using Spark DataFrame API
    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")  # Read as string first to safely clean and cast
        .csv(str(INPUT_PATH))
    )

    total_input_records = raw_df.count()
    print(f"Total Raw Records Ingested: {total_input_records:,}")

    print("\n--- Original Schema ---")
    raw_df.printSchema()

    print("\n--- Raw Sample (First 5 Rows) ---")
    raw_df.show(5, truncate=False)

    # Step 2: Data Cleaning & Type Casting
    # Cast fields to their accurate physical data types
    typed_df = (
        raw_df
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm"))
        .withColumn("temperature", col("temperature").cast(DoubleType()))
        .withColumn("humidity", col("humidity").cast(IntegerType()))
        .withColumn("pressure", col("pressure").cast(DoubleType()))
        .withColumn("wind_speed", col("wind_speed").cast(DoubleType()))
        .withColumn("precipitation", col("precipitation").cast(DoubleType()))
        .withColumn("latitude", col("latitude").cast(DoubleType()))
        .withColumn("longitude", col("longitude").cast(DoubleType()))
        .withColumn("weather_code", col("weather_code").cast(IntegerType()))
        .withColumn("city", col("city").cast(StringType()))
        .withColumn("country", col("country").cast(StringType()))
    )

    # Step 3: Handle Missing Values (Null filtering)
    # Remove records missing critical identification or physical measurements
    null_count = typed_df.filter(
        col("timestamp").isNull() |
        col("city").isNull() |
        col("temperature").isNull()
    ).count()

    cleaned_df = typed_df.filter(
        col("timestamp").isNotNull() &
        col("city").isNotNull() &
        col("temperature").isNotNull()
    )

    # Default missing numeric metrics where appropriate (e.g. precipitation null -> 0.0)
    cleaned_df = cleaned_df.na.fill({
        "precipitation": 0.0,
        "wind_speed": 0.0
    })

    # Step 4: Deduplication
    # A single city cannot have two different readings at the exact same timestamp
    distinct_df = cleaned_df.dropDuplicates(["city", "timestamp"])
    duplicates_removed = cleaned_df.count() - distinct_df.count()

    # Step 5: Transformations & Feature Engineering
    # - Format weather_date (YYYY-MM-DD)
    # - Extract year, month, day for partitioning and time-series analysis
    # - Round decimal fields for consistency
    # - Categorize temperature: Cold (<15C), Moderate (15-24.9C), Warm (25-34.9C), Hot (>=35C)
    # - Convert wind speed from km/h to m/s for standard meteorological unit alignment
    transformed_df = (
        distinct_df
        .withColumn("weather_date", to_date(col("timestamp")))
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
        .withColumn("temperature", spark_round(col("temperature"), 2))
        .withColumn("pressure", spark_round(col("pressure"), 2))
        .withColumn("wind_speed_kmh", spark_round(col("wind_speed"), 2))
        .withColumn("wind_speed_ms", spark_round(col("wind_speed") / 3.6, 2))
        .withColumn("precipitation", spark_round(col("precipitation"), 2))
        .withColumn(
            "temperature_category",
            when(col("temperature") < 15.0, "Cold")
            .when((col("temperature") >= 15.0) & (col("temperature") < 25.0), "Moderate")
            .when((col("temperature") >= 25.0) & (col("temperature") < 35.0), "Warm")
            .otherwise("Hot")
        )
        .drop("wind_speed")  # Replaced with explicit wind_speed_kmh and wind_speed_ms
    )

    # Step 6: Data Quality Validation
    print("\n--- Running Data Quality Validation Checks ---")
    invalid_humidity = transformed_df.filter(
        (col("humidity") < 0) | (col("humidity") > 100)
    ).count()
    invalid_pressure = transformed_df.filter(col("pressure") <= 0).count()
    invalid_wind = transformed_df.filter(col("wind_speed_kmh") < 0).count()

    print(f"  [Validation] Invalid humidity count (outside 0-100%): {invalid_humidity}")
    print(f"  [Validation] Invalid pressure count (<= 0 hPa): {invalid_pressure}")
    print(f"  [Validation] Invalid wind speed count (< 0 km/h): {invalid_wind}")

    if invalid_humidity > 0 or invalid_pressure > 0 or invalid_wind > 0:
        print("  [Warning] Filtering out anomalous sensor records exceeding physical bounds...")
        transformed_df = transformed_df.filter(
            (col("humidity") >= 0) & (col("humidity") <= 100) &
            (col("pressure") > 0) &
            (col("wind_speed_kmh") >= 0)
        )

    final_records_count = transformed_df.count()

    # Step 7: Aggregations for Quality Summary
    summary_stats = transformed_df.select(
        spark_min(col("timestamp")).alias("min_time"),
        spark_max(col("timestamp")).alias("max_time"),
        countDistinct(col("city")).alias("distinct_cities")
    ).first()

    min_date = summary_stats["min_time"]
    max_date = summary_stats["max_time"]
    distinct_cities = summary_stats["distinct_cities"]

    # Step 8: Write to Parquet with Partitioning
    print(f"\nWriting {final_records_count:,} records to Parquet format...")
    print(f"Destination: {OUTPUT_PATH}")
    print("Partitioning Strategy: year, month")

    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    (
        transformed_df
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(str(OUTPUT_PATH))
    )

    print("Parquet write completed successfully!")

    # Step 9: Processing Summary Output
    print("\n==================================================")
    print("PYSPARK PROCESSING SUMMARY")
    print("==================================================")
    print(f"Input records:           {total_input_records:,}")
    print(f"Nulls handled/removed:   {null_count:,}")
    print(f"Duplicates removed:      {duplicates_removed:,}")
    print(f"Final output records:    {final_records_count:,}")
    print(f"Distinct cities:         {distinct_cities}")
    print(f"Date range:              {min_date} to {max_date}")
    print(f"Output format:           Parquet (Snappy compressed)")
    print(f"Partition keys:          year, month")
    print(f"Output location:         {OUTPUT_PATH}")
    print("==================================================\n")

    # Step 10: Verify the Parquet Output by Reading Back
    print("--- Verifying Parquet Output ---")
    parquet_verify_df = spark.read.parquet(str(OUTPUT_PATH))
    print(f"Verified Record Count from Parquet: {parquet_verify_df.count():,}")
    print("Sample Records from Parquet Store:")
    parquet_verify_df.select(
        "city",
        "weather_date",
        "temperature",
        "temperature_category",
        "humidity",
        "pressure",
        "wind_speed_ms",
        "year",
        "month"
    ).show(5, truncate=False)

    spark.stop()
    print("SparkSession stopped cleanly.")


if __name__ == "__main__":
    process_weather_data()
