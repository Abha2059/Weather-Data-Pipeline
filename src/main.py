from extract import fetch_weather, save_raw_data, CITIES                    #Take the fetch_weather() function from extract.py and use it here
from validation import validate_raw_files
from cleaning import clean_raw_files
from transform import transform_processed_data
from load import load_weather_data
from logger_config import logger


def run_main():                                                             #This function contains full ETL pipeline.
    logger.info("==============================")
    logger.info("WEATHER ETL PIPELINE STARTED")
    logger.info("==============================")

    # Extract
    logger.info("STEP 1: Extracting weather data")

    for city in CITIES:

        try:
            logger.info(f"Fetching weather data for {city}")
            data = fetch_weather(city)                                      #This calls the function from extract.py
            save_raw_data(city, data)
            logger.info(f"Successfully extracted data for {city}")

        except Exception as error:
            logger.error(
                f"Failed to fetch data for {city}: {error}"
            )

    # Validation
    logger.info("STEP 2: Validating raw data")

    try:
        validate_raw_files()
        logger.info("Validation completed")

    except Exception as error:
        logger.error(
            f"Validation failed: {error}"
        )
        return

    # Cleaning
    logger.info("STEP 3: Cleaning data")

    try:
        clean_raw_files()
        logger.info("Cleaning completed")

    except Exception as error:
        logger.error(
            f"Cleaning failed: {error}"
        )
        return

    # Transformation
    logger.info("STEP 4: Transforming data")

    try:
        transform_processed_data()
        logger.info("Transformation completed")

    except Exception as error:
        logger.error(
            f"Transformation failed: {error}"
        )
        return

    #  Loading
    logger.info("STEP 5: Loading data into MySQL")

    try:
        load_weather_data()
        logger.info("Database loading completed")

    except Exception as error:
        logger.error(
            f"Database loading failed: {error}"
        )
        return

    # Data Lake Processing
    logger.info("STEP 6: Processing Data Lake with PySpark (Silver & Gold Parquet)")

    try:
        from spark_processing import run_spark_pipeline
        run_spark_pipeline()
        logger.info("Data Lake PySpark processing completed")

    except Exception as error:
        logger.error(
            f"Data Lake PySpark processing failed: {error}"
        )

    logger.info("==============================")
    logger.info("WEATHER ETL PIPELINE COMPLETED")
    logger.info("==============================")


if __name__ == "__main__":                                                  #execute this part of the program only when this Python file is run directly, not when it is imported
    run_main()                                                              #If I directly run main.py, execute run_main()