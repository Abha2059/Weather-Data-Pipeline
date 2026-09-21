# Weather Data Aggregation Pipeline

An end-to-end Data Engineering project that collects weather data from the OpenWeather API, validates and transforms the data, stores historical records in MySQL, orchestrates the ETL pipeline using Apache Airflow, performs SQL analytics, and exports data for Power BI visualization.

---

## 📌 Project Overview

The **Weather Data Aggregation Pipeline** is an ETL-based Data Engineering project designed to collect and analyze weather information for multiple cities.

The pipeline retrieves weather data from the **OpenWeather REST API**, stores the raw API responses as JSON files, validates and cleans the data, transforms it into an analysis-ready format, and loads it into a **local MySQL database**.

The stored weather data can then be analyzed using SQL and exported to CSV for visualization in **Microsoft Power BI**.

Apache Airflow is used to orchestrate the complete ETL workflow and can be triggered manually.

---

## 🎯 Problem Statement

Weather APIs provide current weather information, but raw API responses are not directly suitable for historical analysis and visualization.

This project solves this problem by creating an automated ETL pipeline that:

- Retrieves weather data from a public API.
- Collects data for multiple cities.
- Stores raw API responses.
- Validates incoming data.
- Cleans and standardizes the data.
- Transforms the data into an analysis-ready format.
- Stores historical weather records in MySQL.
- Prevents duplicate city/date records.
- Performs analytical queries using SQL.
- Exports data for Power BI.
- Uses Apache Airflow for workflow orchestration.

---

## 🎯 Objectives

1. Build an end-to-end ETL pipeline using Python.
2. Integrate a public REST weather API.
3. Collect weather data for multiple cities.
4. Store raw API responses for historical reference.
5. Validate incoming weather data.
6. Clean and standardize weather data.
7. Transform data into an analysis-ready structure.
8. Store historical weather records in MySQL.
9. Handle API failures gracefully.
10. Prevent duplicate records.
11. Perform weather analytics using SQL.
12. Export weather data for Power BI.
13. Orchestrate the ETL workflow using Apache Airflow.

---

# 🏗️ Architecture

```text
                    OpenWeather API
                          |
                          v
                  +---------------+
                  | Python ETL    |
                  +---------------+
                          |
                          v
                    Raw JSON Data
                  data/raw/YYYY-MM-DD
                          |
                          v
                     Validation
                          |
                          v
                       Cleaning
                          |
                          v
                    Transformation
                          |
                          v
                    Local MySQL
                    weather_db
                          |
                          v
                    SQL Analytics
                          |
                          v
              export_for_powerbi.py
                          |
                          v
             weather_data.csv
                          |
                          v
                      Power BI
```

### Airflow Orchestration

```text
                 Apache Airflow
                       |
                       v
             weather_data_pipeline
                       |
                       v
                run_weather_pipeline
                       |
                       v
                 main.run_main()
                       |
                       v
        +-------------------------------+
        |                               |
        v                               v
     Extract                         Validate
        |                               |
        +---------------+---------------+
                        |
                        v
                     Clean
                        |
                        v
                   Transform
                        |
                        v
                      Load
                        |
                        v
                     MySQL
```

---

# 🔄 Data Flow

```text
OpenWeather API
       ↓
Extract Weather Data
       ↓
Save Raw JSON
       ↓
Validate Data
       ↓
Clean Data
       ↓
Transform Data
       ↓
Load into MySQL
       ↓
Run SQL Analytics
       ↓
Export CSV
       ↓
Power BI Dashboard
```

---

# 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.11 | ETL development |
| OpenWeather API | Weather data source |
| REST API | API communication |
| Requests | Sending API requests |
| python-dotenv | Managing environment variables |
| MySQL | Historical data storage |
| mysql-connector-python | Connecting Python with MySQL |
| Apache Airflow 3.1.0 | Workflow orchestration |
| SQL | Data analysis |
| Power BI | Data visualization |
| Git | Version control |
| GitHub | Source code management |

---

# 📁 Project Structure

```text
Weather-Data-Pipeline/
│
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── extract.py
│   ├── validation.py
│   ├── cleaning.py
│   ├── transform.py
│   ├── load.py
│   ├── logger_config.py
│   ├── main.py
│   └── export_for_powerbi.py
│
├── dags/
│   └── weather_pipeline_dag.py
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── powerbi/
│       └── weather_data.csv
│
├── logs/
│   └── pipeline.log
│
├── sql/
│   ├── create_tables.sql
│   └── analytics.sql
│
├── .env
├── .gitignore
├── requirements.txt
└── README.md
```

---

# 📄 File Descriptions

| File | Description |
|---|---|
| `config.py` | Loads project paths and environment variables |
| `extract.py` | Retrieves weather data from OpenWeather API |
| `validation.py` | Validates raw weather data |
| `cleaning.py` | Extracts and cleans useful fields |
| `transform.py` | Converts cleaned data into analysis-ready data |
| `load.py` | Loads transformed data into MySQL |
| `main.py` | Runs the complete ETL pipeline |
| `logger_config.py` | Configures application logging |
| `export_for_powerbi.py` | Exports MySQL data to CSV |
| `weather_pipeline_dag.py` | Defines the Airflow workflow |
| `create_tables.sql` | Creates the MySQL database table |
| `analytics.sql` | Contains SQL analytics queries |
| `.env` | Stores API and database configuration |
| `.gitignore` | Prevents sensitive/unnecessary files from Git |
| `requirements.txt` | Contains Python dependencies |
| `README.md` | Project documentation |

---

# 🌦️ Weather Data Collected

The pipeline collects weather information for multiple cities.

Current cities:

```text
Delhi
Mumbai
Bangalore
Kolkata
Chennai
```

The OpenWeather API provides information such as:

- Temperature
- Feels-like temperature
- Humidity
- Atmospheric pressure
- Wind speed
- Precipitation
- Weather condition
- Weather description
- Latitude
- Longitude
- Country
- Timestamp

---

# 🔧 ETL Pipeline

The pipeline consists of five major ETL stages:

```text
Extract
   ↓
Validate
   ↓
Clean
   ↓
Transform
   ↓
Load
```

---

# 1️⃣ Extraction

File:

```text
src/extract.py
```

The extraction stage connects to the OpenWeather API and retrieves weather information for the configured cities.

The API request uses metric units so temperature values are returned in Celsius.

### API Endpoint

```text
https://api.openweathermap.org/data/2.5/weather
```

### Example Request Parameters

```text
city
API key
units=metric
```

### Raw Data Storage

The original API response is stored as JSON.

Example:

```text
data/raw/
└── 2026-09-09/
    ├── delhi.json
    ├── mumbai.json
    ├── bangalore.json
    ├── kolkata.json
    └── chennai.json
```

Keeping raw API responses allows the original source data to be preserved for debugging and future processing.

### API Failure Handling

The extraction stage uses:

- Request timeout
- Exception handling
- HTTP status validation

If one city fails, the error is reported and extraction continues for the remaining cities.

---

# 2️⃣ Validation

File:

```text
src/validation.py
```

The validation stage checks whether the raw API response contains valid weather information.

### Validation Checks

The pipeline checks:

- City name exists.
- Main weather data exists.
- Temperature exists.
- Humidity exists.
- Humidity is between 0 and 100.
- Pressure exists.
- Pressure is greater than zero.
- Wind data exists.
- Wind speed exists.
- Wind speed is not negative.
- JSON file is readable.

### Example

```text
Checking: data/raw/2026-09-09/delhi.json

Status: VALID
```

Invalid records are identified and reported.

---

# 3️⃣ Cleaning

File:

```text
src/cleaning.py
```

The cleaning stage extracts useful information from the nested JSON response.

For example:

```text
main.temp
main.feels_like
main.humidity
main.pressure
wind.speed
sys.country
coord.lat
coord.lon
weather[0].main
weather[0].description
```

The cleaning process also safely handles missing nested fields.

### Timestamp Conversion

The API provides a Unix timestamp such as:

```text
1609459200
```

The pipeline converts this timestamp into a readable date.

### Output

```text
data/processed/cleaned_weather.json
```

---

# 4️⃣ Transformation

File:

```text
src/transform.py
```

The transformation stage converts cleaned records into an analysis-ready format.

### Transformations

The pipeline performs:

- City name standardization.
- Latitude rounding.
- Longitude rounding.
- Temperature rounding.
- Feels-like temperature rounding.
- Wind speed rounding.
- Precipitation rounding.
- Unix timestamp conversion.
- Date/time formatting.
- Temperature categorization.

---

## 🌡️ Temperature Categories

The pipeline creates a `temperature_category` field.

| Temperature | Category |
|---|---|
| Less than 15°C | Cold |
| 15°C to less than 25°C | Moderate |
| 25°C to less than 35°C | Warm |
| 35°C or above | Hot |

Example:

```text
Temperature = 31.57
Category = Warm
```

### Output

```text
data/processed/transformed_weather.json
```

---

# 5️⃣ Loading

File:

```text
src/load.py
```

The loading stage inserts transformed weather data into MySQL.

The process is:

```text
Read transformed JSON
        ↓
Connect to MySQL
        ↓
Insert records
        ↓
Check duplicates
        ↓
Commit transaction
        ↓
Close connection
```

---

# 🗄️ Database Design

Database:

```text
weather_db
```

Table:

```text
weather_data
```

### Database Schema

| Column | Data Type | Description |
|---|---|---|
| `id` | INT | Primary key |
| `city` | VARCHAR(100) | City name |
| `country` | VARCHAR(10) | Country code |
| `latitude` | DECIMAL(10,6) | Latitude |
| `longitude` | DECIMAL(10,6) | Longitude |
| `temperature` | DECIMAL(5,2) | Temperature |
| `feels_like` | DECIMAL(5,2) | Feels-like temperature |
| `humidity` | INT | Humidity percentage |
| `pressure` | INT | Atmospheric pressure |
| `wind_speed` | DECIMAL(5,2) | Wind speed |
| `precipitation_1h` | DECIMAL(6,2) | One-hour precipitation |
| `weather_condition` | VARCHAR(50) | Main weather condition |
| `weather_description` | VARCHAR(100) | Weather description |
| `timestamp` | BIGINT | Unix timestamp |
| `date_time` | DATETIME | Readable date/time |
| `temperature_category` | VARCHAR(20) | Temperature category |
| `weather_date` | DATE | Weather observation date |

---

# 🔐 Duplicate Handling

The database contains a unique constraint:

```text
unique_city_date
```

which is based on:

```text
city + weather_date
```

This means that the same city cannot have multiple records for the same weather date.

The loader uses:

```sql
INSERT IGNORE
```

Therefore, when the pipeline runs again:

```text
New record
     ↓
Inserted

Existing city/date
     ↓
Skipped
```

This prevents duplicate historical records.

---

# 📊 SQL Analytics

SQL queries are stored in:

```text
sql/analytics.sql
```

The project supports analysis such as:

### Temperature Analysis

- Average temperature
- Maximum temperature
- Minimum temperature
- Average temperature by city
- Temperature trends over time

### Humidity Analysis

- Average humidity
- Average humidity by city
- Humidity trends

### Wind Analysis

- Average wind speed
- Wind speed comparison by city

### Weather Analysis

- Weather condition distribution
- Temperature category distribution

### Historical Analysis

- Daily temperature trends
- Monthly temperature trends
- City comparisons
- Rainfall/precipitation trends

---

# 📈 Power BI

The project provides a CSV export for Power BI.

File:

```text
src/export_for_powerbi.py
```

Output:

```text
data/powerbi/weather_data.csv
```

Run:

```bash
python src/export_for_powerbi.py
```

The exported CSV can be imported into Power BI/Fabric for visualization.

---

# 📊 Power BI Dashboard

The dashboard can contain the following analytics.

### Temperature

- Average temperature
- Maximum temperature
- Minimum temperature
- Average temperature by city
- Temperature over time

### Humidity

- Average humidity
- Average humidity by city
- Humidity trends

### Wind

- Average wind speed
- Wind speed by city

### Weather

- Weather condition distribution
- Temperature category distribution

### Historical Trends

- Temperature trends by date
- Monthly temperature trends
- City comparisons
- Precipitation trends

---

# ✈️ Apache Airflow

Apache Airflow is used to orchestrate the ETL workflow.

DAG file:

```text
dags/weather_pipeline_dag.py
```

DAG ID:

```text
weather_data_pipeline
```

The DAG calls:

```text
main.run_main()
```

which executes the complete ETL pipeline.

---

## Airflow Workflow

```text
Airflow DAG
      ↓
run_weather_pipeline
      ↓
main.run_main()
      ↓
Extract
      ↓
Validate
      ↓
Clean
      ↓
Transform
      ↓
Load
```

---

## Manual Airflow Execution

The current project uses:

```python
schedule=None
```

This means the DAG is intentionally configured for **manual execution**.

Trigger the pipeline manually using:

```bash
airflow dags trigger weather_data_pipeline
```

Check the DAG runs:

```bash
airflow dags list-runs weather_data_pipeline
```

A successful run should show:

```text
success
```

---

# 💻 Prerequisites

Before running the project, install/configure:

- Python 3.11
- MySQL Server
- MySQL Client or MySQL Workbench
- OpenWeather API account
- OpenWeather API key
- Apache Airflow 3.1.0
- Git
- Power BI/Fabric for visualization

---

# ⚙️ Installation

## Step 1: Clone the Repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
```

Navigate to the project:

```bash
cd Weather-Data-Pipeline
```

---

## Step 2: Create Virtual Environment

```bash
python3 -m venv venv
```

Activate it:

```bash
source venv/bin/activate
```

For Windows:

```bash
venv\Scripts\activate
```

---

## Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Main Dependencies

```text
requests
python-dotenv
mysql-connector-python
apache-airflow==3.1.0
```

---

# 🔑 Configuration

Create a `.env` file in the project root.

Example:

```env
WEATHER_API_KEY=your_openweather_api_key

MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=
MYSQL_DATABASE=weather_db
```

If MySQL has a password:

```env
MYSQL_PASSWORD=your_mysql_password
```

The `.env` file should never be uploaded to GitHub.

---

# 🗄️ MySQL Setup

Start your MySQL server.

Create the database:

```sql
CREATE DATABASE weather_db;
```

Select the database:

```sql
USE weather_db;
```

Create the required table using:

```text
sql/create_tables.sql
```

You can run:

```bash
mysql -u root -p weather_db < sql/create_tables.sql
```

If your MySQL root account has no password:

```bash
mysql -u root weather_db < sql/create_tables.sql
```

Verify the table:

```sql
SHOW TABLES;
```

Expected:

```text
weather_data
```

---

# ▶️ Running the Complete Pipeline

Navigate to the project:

```bash
cd ~/Documents/Weather-Data-Pipeline
```

Activate the virtual environment:

```bash
source venv/bin/activate
```

Run:

```bash
python src/main.py
```

The complete pipeline will execute:

```text
Extract
   ↓
Validate
   ↓
Clean
   ↓
Transform
   ↓
Load
```

---

# 🧩 Running Individual Components

Each Python module can also be executed separately.

### Extract

```bash
python src/extract.py
```

### Validation

```bash
python src/validation.py
```

### Cleaning

```bash
python src/cleaning.py
```

### Transformation

```bash
python src/transform.py
```

### Loading

```bash
python src/load.py
```

### Power BI Export

```bash
python src/export_for_powerbi.py
```

For normal use, run:

```bash
python src/main.py
```

---

# ✈️ Running Airflow

Activate the virtual environment:

```bash
source venv/bin/activate
```

Navigate to the project:

```bash
cd ~/Documents/Weather-Data-Pipeline
```

Set Airflow home:

```bash
export AIRFLOW_HOME="$PWD/airflow"
```

Set the DAG folder:

```bash
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
```

Initialize/migrate the Airflow database:

```bash
airflow db migrate
```

Check available DAGs:

```bash
airflow dags list
```

Trigger the weather pipeline:

```bash
airflow dags trigger weather_data_pipeline
```

Check the run:

```bash
airflow dags list-runs weather_data_pipeline
```

---

# 🔍 Verifying the Pipeline

## Check Raw JSON Files

```bash
find data/raw -type f -name "*.json"
```

---

## Check Processed Files

```bash
ls data/processed
```

Expected:

```text
cleaned_weather.json
transformed_weather.json
```

---

## Check Power BI CSV

```bash
ls -lh data/powerbi/weather_data.csv
```

---

## Check MySQL Records

Connect to MySQL:

```bash
mysql -u root
```

Then:

```sql
USE weather_db;
```

Check number of records:

```sql
SELECT COUNT(*) FROM weather_data;
```

View all records:

```sql
SELECT * FROM weather_data;
```

View latest records:

```sql
SELECT *
FROM weather_data
ORDER BY weather_date DESC, city;
```

---

# 🧪 Pipeline Validation

The project can be validated by running the complete pipeline directly:

```bash
python src/main.py
```

After execution, verify the database:

```sql
SELECT COUNT(*) FROM weather_data;
```

Also verify the Power BI export:

```bash
python src/export_for_powerbi.py
```

Then check:

```text
data/powerbi/weather_data.csv
```

Airflow can be validated with:

```bash
airflow dags trigger weather_data_pipeline
```

and:

```bash
airflow dags list-runs weather_data_pipeline
```

---

# 🚨 Error Handling

The project includes error handling at multiple stages.

## API Errors

API request failures are caught and displayed.

## Timeout

API requests use a timeout so the pipeline does not wait indefinitely.

## Invalid JSON

Corrupted JSON files are detected during validation.

## Missing Fields

Required weather fields are checked before processing.

## Invalid Humidity

Humidity must be between:

```text
0 and 100
```

## Invalid Pressure

Pressure must be greater than zero.

## Invalid Wind Speed

Wind speed cannot be negative.

## Duplicate Records

Duplicate city/date records are ignored by MySQL.

---

# 🔒 Security

Sensitive information is stored inside:

```text
.env
```

Examples:

```text
WEATHER_API_KEY
MYSQL_PASSWORD
```

The `.env` file should never be committed to GitHub.

The `.gitignore` file contains:

```gitignore
venv/
.env
__pycache__/
*.pyc
data/raw/
data/processed/
logs/
airflow/
```

---

# 📝 Logging

The project contains:

```text
src/logger_config.py
```

and:

```text
logs/pipeline.log
```

Logging can be used to record pipeline events, errors, and execution information.

---

# 🧪 Testing Approach

The current project does not use a separate `tests/` directory.

Instead, the pipeline is validated through direct execution and verification.

Run:

```bash
python src/main.py
```

Then verify:

```sql
SELECT COUNT(*) FROM weather_data;
```

Also verify:

```text
data/raw/
data/processed/
data/powerbi/weather_data.csv
```

Airflow execution can also be verified through:

```bash
airflow dags list-runs weather_data_pipeline
```

---

# 📌 Current Implementation

The project is currently designed as a **local/manual Data Engineering pipeline**.

### Current architecture

```text
OpenWeather API
       ↓
Python ETL
       ↓
Local MySQL
       ↓
SQL Analytics
       ↓
CSV Export
       ↓
Power BI
```

Apache Airflow is included for workflow orchestration and manual triggering.

The current implementation does not depend on:

- Cloud databases
- Cloud data warehouses
- Cloud dataflows
- Cloud lakehouses
- Automatic cloud scheduling

---

# 🚀 Future Improvements

The following features can be added in future versions:

1. Incremental ETL processing.
2. Process only newly extracted raw files.
3. Automated daily Airflow scheduling.
4. More cities.
5. Historical weather API integration.
6. Improved data-quality checks.
7. Stronger schema validation.
8. API retry and rate-limit handling.
9. Database indexing optimization.
10. Unit and integration testing.
11. Docker containerization.
12. GitHub Actions CI/CD.
13. Advanced Power BI dashboard.
14. Additional weather metrics.
15. Weather forecasting using Machine Learning.

---

# 🛠️ Troubleshooting

## Problem: `ModuleNotFoundError`

Activate the virtual environment:

```bash
source venv/bin/activate
```

Then install dependencies:

```bash
pip install -r requirements.txt
```

---

## Problem: Weather API Key Error

Check that `.env` exists in the project root:

```text
Weather-Data-Pipeline/.env
```

Make sure it contains:

```env
WEATHER_API_KEY=your_api_key
```

---

## Problem: MySQL Connection Error

Check:

```env
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=
MYSQL_DATABASE=weather_db
```

Also make sure MySQL Server is running.

---

## Problem: Database Does Not Exist

Create it:

```sql
CREATE DATABASE weather_db;
```

---

## Problem: Airflow Cannot Find DAG

Set:

```bash
export AIRFLOW_HOME="$PWD/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
```

Then:

```bash
airflow dags list
```

---

## Problem: Duplicate Records

Duplicate records are expected to be skipped when the same city/date already exists.

The database uses:

```text
UNIQUE(city, weather_date)
```

---

# 📊 Example SQL Queries

## Average Temperature

```sql
SELECT AVG(temperature) AS average_temperature
FROM weather_data;
```

## Maximum Temperature

```sql
SELECT MAX(temperature) AS maximum_temperature
FROM weather_data;
```

## Minimum Temperature

```sql
SELECT MIN(temperature) AS minimum_temperature
FROM weather_data;
```

## Average Temperature by City

```sql
SELECT
    city,
    AVG(temperature) AS average_temperature
FROM weather_data
GROUP BY city;
```

## Average Humidity by City

```sql
SELECT
    city,
    AVG(humidity) AS average_humidity
FROM weather_data
GROUP BY city;
```

## Weather Conditions

```sql
SELECT
    weather_condition,
    COUNT(*) AS total_records
FROM weather_data
GROUP BY weather_condition
ORDER BY total_records DESC;
```

## Temperature Categories

```sql
SELECT
    temperature_category,
    COUNT(*) AS total_records
FROM weather_data
GROUP BY temperature_category;
```

## Historical Temperature

```sql
SELECT
    weather_date,
    city,
    temperature
FROM weather_data
ORDER BY weather_date, city;
```

---

# 📈 Example End-to-End Execution

```bash
# 1. Go to project
cd ~/Documents/Weather-Data-Pipeline

# 2. Activate virtual environment
source venv/bin/activate

# 3. Run complete ETL pipeline
python src/main.py

# 4. Export MySQL data for Power BI
python src/export_for_powerbi.py
```

For Airflow:

```bash
# Set Airflow environment
export AIRFLOW_HOME="$PWD/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"

# Trigger manually
airflow dags trigger weather_data_pipeline

# Check status
airflow dags list-runs weather_data_pipeline
```

---

# 📦 Output Files

The pipeline produces the following types of output:

### Raw Data

```text
data/raw/YYYY-MM-DD/*.json
```

### Cleaned Data

```text
data/processed/cleaned_weather.json
```

### Transformed Data

```text
data/processed/transformed_weather.json
```

### Power BI Data

```text
data/powerbi/weather_data.csv
```

### Logs

```text
logs/pipeline.log
```

### Database

```text
MySQL
└── weather_db
    └── weather_data
```

### Parquet Big Data (Phase 2)

```text
data/processed/weather_parquet/
├── year=2010/
│   ├── month=1/
│   └── ...
└── year=2024/
    ├── month=12/
```

---

# ⚡ Big Data Processing with PySpark (Phase 2)

In **Phase 2**, we implemented large-scale historical weather data processing using **PySpark** and stored the processed data in **Parquet** format.

### 🎯 Why Historical Data Was Added
While the Phase 1 REST API pipeline collects real-time daily weather snapshots (dozens to hundreds of records), evaluating long-term climate trends and stress-testing data pipelines requires **deep historical data**. We added 15 years of authentic hourly weather observations (2010–2024) across 5 major Indian cities (Delhi, Mumbai, Bangalore, Kolkata, Chennai) from the open ECMWF ERA5 reanalysis archive.

### 📊 Dataset Size
- **Total Records:** 657,480 authentic hourly observations
- **Date Range:** 2010-01-01 to 2024-12-31
- **Locations:** 5 distinct major cities (Delhi, Mumbai, Bangalore, Kolkata, Chennai)
- **Raw CSV Size:** ~44 MB
- **Processed Parquet Size:** ~28 MB (Snappy compressed)

### 🚀 Why PySpark Was Introduced
- Standard single-threaded Python loops and dictionaries are not designed for processing hundreds of thousands or millions of records efficiently.
- Pandas is bound to a single CPU core and loads entire datasets into memory, risking `OutOfMemory` errors.
- PySpark provides distributed, parallel in-memory computing across all available CPU cores on local machines (`local[*]`), building optimized execution DAGs before performing transformations.

### 🛠️ What Processing PySpark Performs
1. **Schema Standardization:** Casts raw string fields into exact physical datatypes (`DoubleType`, `IntegerType`, `TimestampType`, `DateType`).
2. **Missing Value & Outlier Handling:** Filters invalid/null records, defaults missing sensor values safely (e.g. precipitation nulls -> 0.0), and enforces physical atmospheric bounds.
3. **Deduplication:** Drops duplicate observations for identical city-timestamp pairs (`dropDuplicates(["city", "timestamp"])`).
4. **Feature Engineering:**
   - Categorizes temperature into `Cold` (<15°C), `Moderate` (15–24.9°C), `Warm` (25–34.9°C), and `Hot` (>=35°C).
   - Converts wind speed from km/h to m/s.
   - Extracts time-dimension partition keys: `year`, `month`, and `day`.
5. **Quality Verification:** Reads back and validates the generated Parquet datasets to ensure zero schema drift.

### 📦 Why Parquet Was Selected
- **Columnar Storage:** Unlike row-oriented CSV/JSON, Parquet stores values column-by-column.
- **High Compression:** Built-in Snappy compression saves ~35-40% storage space compared to raw CSV.
- **Predicate Pushdown:** Downstream analytical engines only read the specific columns and partitions requested by queries, drastically reducing disk I/O.

### 🗂️ Partitioning Strategy
Data is partitioned by **`year`** and **`month`**:
```text
data/
└── processed/
    └── weather_parquet/
        ├── year=2010/
        │   ├── month=1/
        │   └── ...
        └── year=2024/
            ├── month=1/
            ├── month=2/
            └── ...
```
Partitioning enables queries like *"Find average temperature in May 2024"* to skip reading files from all other years and months entirely (partition pruning).

### 📍 Input and Output Locations
- **Input Raw Historical Data:** `data/historical/weather_historical.csv`
- **Output Processed Parquet Data:** `data/processed/weather_parquet/`

### 💻 How to Run the PySpark Job

```bash
# 1. Activate virtual environment
source venv/bin/activate

# 2. Download authentic historical dataset (~657K records, if not already present)
python src/download_historical_data.py

# 3. Run the PySpark distributed transformation job
python src/spark_transform.py
```

### 📋 Example Processing Summary Output

```text
==================================================
PYSPARK PROCESSING SUMMARY
==================================================
Input records:           657,480
Nulls handled/removed:   0
Duplicates removed:      0
Final output records:    657,480
Distinct cities:         5
Date range:              2010-01-01 00:00:00 to 2024-12-31 23:00:00
Output format:           Parquet (Snappy compressed)
Partition keys:          year, month
Output location:         data/processed/weather_parquet
==================================================
```

---

# ☁️ AWS S3 Data Lake Architecture (Phase 3)

In **Phase 3**, we introduced an **Amazon Web Services (AWS) S3 Data Lake** supporting **Bronze**, **Silver**, and **Gold** medallion architecture layers processed via **PySpark**.

```text
                    OpenWeather API
                          ↓
                       Python
                          ↓
                    AWS S3 Bronze
                    Raw JSON Data
                          ↓
                       PySpark
                          ↓
                    AWS S3 Silver
                  Cleaned Parquet Data
                          ↓
                       PySpark
                          ↓
                    AWS S3 Gold
                Analytics Parquet Data
                          ↓
                        MySQL
                          ↓
                       Power BI
```

### 🎯 Why S3 is Used
- **Scalability & Durability**: Amazon S3 provides 99.999999999% (11 9's) data durability and virtually unlimited storage capacity.
- **Cost Efficiency**: Storing raw data and compressed Parquet files in S3 costs pennies per gigabyte, avoiding costly local or database disk bloat.
- **Separation of Compute and Storage**: Decouples data storage (S3) from compute processing engines (PySpark, SQL queries), enabling independent scaling.

### 🏛️ What is a Data Lake?
A centralized repository allowing you to store all your structured, semi-structured, and raw data at any scale. The pipeline organizes data into the standard **Medallion Architecture**:
1. **Bronze Layer (Raw)**:
   - Preserves source API responses in their raw, unaltered JSON format.
   - Preserved for auditability, debugging, and historical reprocessing.
   - S3 Path: `s3://<bucket-name>/bronze/weather/year=YYYY/month=MM/day=DD/<city>.json`
2. **Silver Layer (Cleaned & Standardized)**:
   - Validated, deduplicated, and typed data transformed by PySpark.
   - Enforces physical sensor constraints and extracts time dimensions (`year`, `month`, `day`).
   - Stored in columnar **Parquet** format.
   - S3 Path: `s3://<bucket-name>/silver/weather/year=YYYY/month=MM/day=DD/*.parquet`
3. **Gold Layer (Analytics-Ready Aggregations)**:
   - Business-level aggregations designed for fast analytical queries and BI dashboards.
   - Stored in columnar **Parquet** format.
   - S3 Paths:
     - `s3://<bucket-name>/gold/weather/city_temperature/` (Avg, Min, Max temperatures by city)
     - `s3://<bucket-name>/gold/weather/monthly_temperature/` (Monthly temperature trends)
     - `s3://<bucket-name>/gold/weather/city_weather_summary/` (Overall city KPI summary)
     - `s3://<bucket-name>/gold/weather/humidity_trends/` (Daily humidity trends)
     - `s3://<bucket-name>/gold/weather/wind_trends/` (Daily wind speed trends)
     - `s3://<bucket-name>/gold/weather/rainfall_trends/` (Daily precipitation totals)

### 🚀 Why PySpark is Used
- Distributed in-memory data processing across CPU cores.
- Handles parsing nested JSON, schema casting, and complex aggregations for Data Lake layers.

### 📦 Why Parquet is Used
- **Columnar Format**: Queries scanning specific columns (e.g. `temperature`) only read those column blocks from S3.
- **Snappy Compression**: Reduces file size by 35-40% compared to raw text.
- **Partition Pruning**: Queries filtering by date skip reading non-matching partition folders.

### 🗂️ S3 Partitioning Strategy
- **Bronze**: `year=YYYY/month=MM/day=DD/<city>.json`
- **Silver**: `year=YYYY/month=MM/day=DD/*.parquet`
- **Gold**: Partitioned by analytical topic domain

### 🔐 AWS Authentication & Configuration
The project uses standard `boto3` authentication without hard-coded secrets.

Configure credentials in `.env`:
```env
AWS_REGION=ap-south-1
AWS_S3_BUCKET=your-unique-bucket-name
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
```
Or use the AWS CLI:
```bash
aws configure
```

### 🛡️ IAM Least Privilege Permissions
The IAM policy requires only minimal S3 permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::your-unique-bucket-name",
        "arn:aws:s3:::your-unique-bucket-name/*"
      ]
    }
  ]
}
```

### 💻 How to Run the End-to-End Pipeline
```bash
# 1. Activate virtual environment
source venv/bin/activate

# 2. Run the complete pipeline (Extract -> S3 Bronze -> PySpark Silver/Gold -> Local MySQL)
python src/main.py

# Or run the PySpark Data Lake processing independently:
python src/spark_processing.py
```

### 🔍 How to Verify S3
```bash
# Verify Bronze
aws s3 ls s3://your-bucket-name/bronze/weather/ --recursive

# Verify Silver
aws s3 ls s3://your-bucket-name/silver/weather/ --recursive

# Verify Gold
aws s3 ls s3://your-bucket-name/gold/weather/ --recursive
```

### 🔗 Integration with Existing MySQL Pipeline
The existing MySQL database loading (`src/load.py`) and Power BI export (`src/export_for_powerbi.py`) remain completely connected and functional. Data flows smoothly from the API into S3 and MySQL simultaneously.

### 💰 Cost & Free-Tier Safety
- Storage uses standard S3 standard tier (~$0.023/GB/month).
- File sizes for raw JSON and Parquet are well within AWS Free Tier limits (5 GB standard storage, 20,000 GET requests, 2,000 PUT requests).
- No continuous or duplicate uploads of identical data.
- S3 bucket remains private by default (Block Public Access enabled).

---

# ☁️ Cloud Relational Database — AWS RDS for MySQL

This project extends the relational database layer from a local MySQL instance to **AWS Relational Database Service (RDS) for MySQL**, providing a managed cloud database for production storage, SQL analytics, and business intelligence.

### 📐 Target Architecture

```text
                 OpenWeather REST API
                          ↓
                      Python ETL
                          ↓
                   AWS S3 Bronze (Raw JSON)
                          ↓
                       PySpark
                          ↓
                   AWS S3 Silver (Parquet)
                          ↓
                       PySpark
                          ↓
                    AWS S3 Gold (Aggregations)
                          ↓
                 AWS RDS MySQL (weather_db)
                          ↓
                   SQL Analytics
                          ↓
                     Power BI
```

### 🔄 Local MySQL vs. AWS RDS MySQL

| Feature | Local MySQL | AWS RDS for MySQL |
| :--- | :--- | :--- |
| **Hosting** | Local developer machine | AWS Cloud (`ap-south-1`) |
| **Availability** | Accessible only while local service runs | Cloud-accessible 24/7 |
| **Backups** | Manual mysqldump backups | Automated snapshots & point-in-time recovery |
| **Scalability** | Limited to local machine resources | Scalable compute and storage tiers |
| **Maintenance** | Manual OS and engine updates | Managed engine patches and maintenance windows |
| **Pipeline Role** | Local development and offline testing | Production cloud database target |

### ⚙️ Configuration-Driven Switching

The pipeline connects to the database via environment variables defined in `.env`. Switching between Local MySQL and AWS RDS requires **zero code changes**:

**Local Development:**
```env
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=
MYSQL_DATABASE=weather_db
```

**AWS RDS Cloud Deployment:**
```env
MYSQL_HOST=weather-db.xxxxxxxxxxxx.ap-south-1.rds.amazonaws.com
MYSQL_PORT=3306
MYSQL_USER=admin
MYSQL_PASSWORD=your_secure_password
MYSQL_DATABASE=weather_db
```

### 🗄️ Database Schema & Integrity

The relational schema defined in `sql/create_tables.sql` is identical across Local MySQL and AWS RDS:

```sql
CREATE DATABASE IF NOT EXISTS weather_db;
USE weather_db;

CREATE TABLE IF NOT EXISTS weather_data (
    id INT NOT NULL AUTO_INCREMENT,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(10),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    temperature DECIMAL(5,2),
    feels_like DECIMAL(5,2),
    humidity INT,
    pressure INT,
    wind_speed DECIMAL(5,2),
    precipitation_1h DECIMAL(6,2),
    weather_condition VARCHAR(50),
    weather_description VARCHAR(100),
    timestamp BIGINT,
    date_time DATETIME,
    temperature_category VARCHAR(20),
    weather_date DATE NOT NULL,

    PRIMARY KEY (id),
    UNIQUE KEY unique_city_date (city, weather_date)
);
```

- **Primary Key**: `id` provides an auto-incrementing surrogate key.
- **Uniqueness Guarantee**: `UNIQUE KEY unique_city_date (city, weather_date)` paired with `INSERT IGNORE` ensures idempotent loads with zero duplicate records across repeated pipeline runs.

### 🔒 Networking & Security Group Configuration

- **Restricted Access**: Port 3306 is strictly restricted to the developer's public IP address (`<IP>/32`) in the VPC Security Group (`weather-rds-sg`).
- **Never Open to 0.0.0.0/0**: Inbound MySQL access is never open to the public internet.
- **Credential Protection**: Master passwords and endpoints are stored exclusively in local `.env` files and never committed to version control.

### 🛠️ Database Management Utility (`src/db_init.py`)

A dedicated helper script manages database connectivity, initialization, and verification:

```bash
# 1. Test database connection (SELECT 1)
python src/db_init.py --test-connection

# 2. Initialize weather_db and weather_data table
python src/db_init.py --init-schema

# 3. Execute SQL analytics against the database
python src/db_init.py --run-analytics

# 4. Run test, schema initialization, and analytics in sequence
python src/db_init.py --all
```

### 📊 Connecting Power BI to AWS RDS MySQL

To connect Microsoft Power BI directly to the AWS RDS database:
1. Open **Power BI Desktop**.
2. Click **Get Data** $\rightarrow$ **MySQL Database** $\rightarrow$ **Connect**.
3. **Server**: Enter the RDS endpoint (e.g., `weather-db.xxxxxxxxxxxx.ap-south-1.rds.amazonaws.com:3306`).
4. **Database**: `weather_db`.
5. **Data Connectivity Mode**: Select **Import** (for high performance) or **DirectQuery** (for live updates).
6. **Authentication**: Select **Database** tab and enter your RDS username (`admin`) and password.
7. Select the `weather_data` table and click **Load**.

### 💰 Cost Management & Resource Safety

- **Free Tier Eligibility**: AWS provides 750 hours/month of `db.t3.micro` or `db.t4g.micro` Single-AZ with 20 GiB General Purpose SSD storage for the first 12 months on eligible accounts.
- **Stopping RDS**: When not in active development, the RDS instance can be stopped in the AWS Console to halt hourly compute charges.
  > [!NOTE]
  > Stopping an instance pauses compute charges; however, provisioned storage (20 GiB gp3 $\approx$ ~$2.30/month) remains allocated and billed until deleted.
- **Deleting RDS**: To permanently eliminate all charges after project evaluation:
  1. Open RDS Console $\rightarrow$ Select `weather-db`.
  2. Actions $\rightarrow$ **Delete**.
  3. Deselect "Create final snapshot" and confirm deletion.

---

# 🌪️ Workflow Orchestration — Apache Airflow

This project utilizes **Apache Airflow** to orchestrate and coordinate the end-to-end Data Engineering pipeline, managing task dependencies, execution order, retries, and data lake synchronization.

### 📐 Orchestration Architecture & Workflow

```text
                     OpenWeather REST API
                              ↓
                      [extract_weather]
                              ↓
                       [upload_bronze]
                              ↓
                       [process_silver]
                              ↓
                        [process_gold]
                              ↓
                          [load_rds]
                              ↓
                     [pipeline_complete]
```

### 🧩 Component Responsibilities

| Component | Primary Responsibility |
| :--- | :--- |
| **Apache Airflow** | Workflow orchestration, task sequencing, retries, failure handling, and execution state |
| **Python** | REST API extraction and modular pipeline helper logic |
| **AWS S3** | Cloud Data Lake storage across Bronze, Silver, and Gold layers |
| **PySpark** | Large-scale distributed data processing, validation, deduplication, and aggregation |
| **AWS RDS MySQL** | Production cloud relational database storing analysis-ready records |
| **SQL** | Analytical queries, aggregations, and business metrics |
| **Power BI** | Interactive business intelligence dashboards and visualizations |
| **Streamlit Dashboard** | Real-time interactive web analytics application with dynamic slicing, KPI cards, and S3 Gold benchmark |

### 📋 Airflow Task Breakdown

1. **`extract_weather`**:
   - Fetches current weather data for all configured cities from the OpenWeather REST API.
   - Saves raw JSON responses locally into `data/raw/YYYY-MM-DD/<city>.json`.
   - **Retries**: 2 retries with 30s delay for transient network/API timeouts.

2. **`upload_bronze`**:
   - Synchronizes raw JSON records to the AWS S3 Bronze layer (`s3://<bucket>/bronze/weather/year=YYYY/month=MM/day=DD/<city>.json`).
   - Ensures immutable raw data storage.
   - **Retries**: 2 retries with 30s delay.

3. **`process_silver`**:
   - Initializes a PySpark session and reads raw Bronze data.
   - Performs data quality validation, casts data types, validates physical sensor bounds, and removes duplicate records.
   - Writes Snappy-compressed Parquet files partitioned by `year`, `month`, and `day` to `s3://<bucket>/silver/weather/`.
   - **Retries**: 1 retry with 1m delay.

4. **`process_gold`**:
   - Reads Silver Parquet data and creates 6 analytical aggregation datasets:
     - `city_temperature` (Avg, Max, Min temperatures per city)
     - `monthly_temperature` (Monthly trends)
     - `city_weather_summary` (Multi-metric KPIs)
     - `humidity_trends` (Daily humidity trends)
     - `wind_trends` (Daily wind speed trends)
     - `rainfall_trends` (Daily precipitation totals)
   - Writes Snappy Parquet files to `s3://<bucket>/gold/weather/`.
   - **Retries**: 1 retry with 1m delay.

5. **`load_rds`**:
   - Cleans and transforms records, then loads them into AWS RDS MySQL (`weather_db.weather_data`) using `mysql-connector-python`.
   - Enforces uniqueness on `(city, weather_date)` with `INSERT IGNORE` for idempotent execution.
   - Updates local Power BI CSV export at `data/powerbi/weather_data.csv`.
   - **Retries**: 1 retry with 30s delay.

6. **`pipeline_complete`**:
   - Final `EmptyOperator` downstream anchor confirming successful end-to-end execution.

### ⚙️ DAG Configuration & Scheduling

- **DAG ID**: `weather_data_pipeline`
- **Schedule**: Defaults to `None` for safe manual execution during development.
- **Future Scheduling**: Configurable via the `AIRFLOW_SCHEDULE` environment variable in `.env`.
  - To enable a daily schedule at midnight IST:
    ```env
    AIRFLOW_SCHEDULE="0 0 * * *"
    ```
  - To disable automatic scheduling:
    ```env
    AIRFLOW_SCHEDULE="None"
    ```
- **Timezone**: Timezone-aware using `Asia/Kolkata` (`pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata")`).
- **Catchup**: `catchup=False` to prevent unintentional backfilling of historical dates.

### 🚀 Running & Verifying the Airflow DAG

Execute the DAG locally using the Airflow CLI:

```bash
# Set Airflow environment variables
export AIRFLOW_HOME="$PWD/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"

# 1. Verify DAG import without errors
airflow dags list-import-errors

# 2. Inspect DAG task details and dependencies
airflow dags details weather_data_pipeline

# 3. Test end-to-end DAG execution
airflow dags test weather_data_pipeline
```

---

# 📊 Interactive Weather Analytics Dashboard (Streamlit)

A modern, responsive web application built with **Streamlit** and **Plotly** to provide interactive ad-hoc data analysis, dynamic multi-dimensional filtering, KPI metric tracking, and cross-layer benchmarking against the AWS S3 Gold Data Lakehouse.

```text
               +--------------------------------------------+
               |            Streamlit Web Dashboard         |
               +--------------------------------------------+
                             ▲                 ▲
                             │                 │
              (Dynamic Slicing & Queries)   (Pre-Aggregated Benchmarks)
                             │                 │
                             │                 │
                  +--------------------+  +--------------------+
                  |    AWS RDS MySQL   |  |    AWS S3 Gold     |
                  |     (weather_db)   |  |   (Parquet Files)  |
                  +--------------------+  +--------------------+
```

---

### 🌟 Core Capabilities & Dashboard Views

The dashboard is structured into high-impact visual sections and dedicated analytical tabs:

1. **Interactive Sidebar Controls**:
   - **City Multi-Select**: Dynamic selection across all ingested cities (`Bengaluru`, `Chennai`, `Delhi`, `Kolkata`, `Mumbai`) with a one-click "Select All" toggle.
   - **Date Range Filter**: Dynamically bounded by the minimum and maximum `weather_date` available in the database.
   - **Weather Condition Filter**: Filter by observed meteorological conditions (`Clear`, `Clouds`, `Haze`, `Rain`, etc.).
   - **Cache Management**: One-click "Refresh Data & Clear Cache" button leveraging `@st.cache_data(ttl=300)`.

2. **Top-Level KPI Summary Cards**:
   - **Total Records**: Filtered record count in scope.
   - **Avg Temperature**: Filtered mean temperature (°C).
   - **Peak Temperature**: Maximum temperature recorded with city attribution.
   - **Avg Humidity**: Filtered mean relative humidity (%).
   - **Avg Wind Speed**: Filtered mean wind speed (km/h).
   - **Total Rain (1h)**: Sum of recorded 1-hour precipitation events (mm).

3. **Dedicated Analytical Tabs**:
   - 📈 **Temperature Analytics**:
     - Daily temperature trend lines by city over time.
     - Temperature extremes comparison (Min vs Avg vs Max vs Feels Like).
   - 💧 **Humidity & Wind**:
     - Daily humidity trends across cities.
     - City-level average humidity distribution bar chart.
     - Daily wind speed (km/h) trends over time.
   - 🌧️ **Precipitation (1h)**:
     - 1-hour recorded precipitation events by city.
     - *Data Disclosure Note*: Accurately explains that values represent 1-hour snapshot precipitation rates from OpenWeather API observations.
   - 🏙️ **City Comparison**:
     - Cross-city comparative bar charts for temperature, humidity, and wind speed.
     - Tabular city benchmark matrix with min/avg/max temperature, humidity, wind, and prevailing condition.
   - 📅 **Monthly Trends**:
     - Monthly aggregated temperature trends (`YYYY-MM`) comparing average, minimum, and maximum values.
   - 🏆 **S3 Gold Lakehouse**:
     - Reads pre-aggregated PySpark Parquet tables directly from `s3://<bucket>/gold/weather/` (or local lakehouse mirror).
     - Allows direct benchmark comparison between real-time operational RDS MySQL records and PySpark batch gold aggregations.
   - 📋 **Data Explorer**:
     - Paginated, searchable data table displaying all filtered records.
     - Instant **"Download Filtered Data as CSV"** button for offline reporting.

---

### 🔄 Coexistence with Power BI

The Streamlit dashboard does **not** replace the existing Power BI reports; rather, both tools serve complementary roles:

| Dimension | Streamlit Dashboard | Power BI Dashboard |
| :--- | :--- | :--- |
| **Primary Audience** | Data engineers, analysts, operational teams | Business stakeholders, executives, management |
| **Data Source** | Live queries to AWS RDS MySQL & AWS S3 Gold Parquet | Scheduled refresh via `weather_data.csv` export / RDS |
| **Interactivity** | Low-latency ad-hoc SQL slicing, live cache invalidation | Drag-and-drop DAX visualizations and cross-filtering |
| **Deployment** | Python web application running locally or via container | Microsoft Power BI Service / Power BI Desktop (.pbix) |
| **Extensibility** | Direct Python/Plotly code customization | Enterprise business intelligence workspace sharing |

---

### 🛡️ Security & Connection Best Practices

- **Zero Hardcoded Credentials**: Database host, user, password, and port are retrieved dynamically from the local `.env` configuration file.
- **Connection Isolation**: Every database interaction uses a `try...finally` block to strictly close cursor and connection objects, preventing connection leaks on AWS RDS.
- **SQL Injection Prevention**: All dynamic SQL queries use parameterized queries (`%s` placeholders with value tuples).
- **Sensitive Data Masking**: Dashboard footer displays active RDS host metadata while masking internal passwords and usernames.

---

### 📁 Streamlit Directory Structure

```text
streamlit_app/
├── __init__.py           # Package initializer
├── app.py                # Main Streamlit dashboard application
├── database.py           # Safe RDS MySQL connection & ping tester
├── queries.py            # Parameterized SQL query builders
├── charts.py             # Reusable Plotly chart generators
├── gold_lakehouse.py     # AWS S3 Gold Parquet dataset reader
└── README.md             # Streamlit quickstart guide
```

---

### 🚀 Running the Streamlit Dashboard

1. **Activate the Virtual Environment**:
   ```bash
   cd /Users/abhaykumar/Weather-Data-Pipeline
   source venv/bin/activate
   ```

2. **Verify Database Connectivity**:
   Ensure `.env` contains valid AWS RDS credentials:
   ```env
   DB_HOST=weather-db.c9qcuiuy6x3m.ap-south-1.rds.amazonaws.com
   DB_PORT=3306
   DB_USER=admin
   DB_PASSWORD=your_secure_password
   DB_NAME=weather_db
   ```

3. **Launch the Dashboard**:
   ```bash
   streamlit run streamlit_app/app.py
   ```

4. **Access the Dashboard**:
   Open your browser and navigate to `http://localhost:8501`.

---

# 🐳 Docker Containerization

The project includes production-ready **Docker** and **Docker Compose** configurations to package the ETL pipeline, Apache Airflow orchestrator, PySpark distributed engine, and Streamlit analytics dashboard into isolated, reproducible containers.

```text
                                  +---------------------------------------+
                                  |         Host Machine (Browser)        |
                                  +---------------------------------------+
                                        │ (Port 8080)            │ (Port 8501)
                                        ▼                        ▼
+-----------------------------------------------------------------------------------------+
| Docker Network: weather_pipeline_network                                                |
|                                                                                         |
|  +-------------------------------------+        +------------------------------------+  |
|  |     Service: airflow                |        |     Service: streamlit             |  |
|  |     (weather_airflow)               |        |     (weather_streamlit)            |  |
|  |                                     |        |                                    |  |
|  |  - Python 3.11                      |        |  - Python 3.11                     |  |
|  |  - OpenJDK 17                       |        |  - Streamlit Web Server            |  |
|  |  - Apache Airflow 3.1.0 Standalone  |        |  - Plotly Interactive Visuals      |  |
|  |  - PySpark 4.2.0 Engine             |        |  - PyArrow Parquet Reader          |  |
|  |  - S3 Sync & RDS Loaders            |        |  - Parameterized Query Engine      |  |
|  +-------------------------------------+        +------------------------------------+  |
|                  │                                                 │                    |
+──────────────────┼─────────────────────────────────────────────────┼────────────────────+
                   │                                                 │
                   ▼                                                 ▼
     +───────────────────────────+                     +───────────────────────────+
     |       AWS S3 Bucket       |                     |     AWS RDS for MySQL     |
     |   (Bronze / Silver / Gold)|                     |   (weather_db.weather_data|
     +───────────────────────────+                     +───────────────────────────+
```

---

### 🎯 Why Docker is Used
- **Environment Consistency**: Eliminates "works on my machine" issues by packaging exact versions of Python 3.11, OpenJDK 17, Apache Airflow 3.1.0, and PySpark 4.2.0.
- **Dependency Isolation**: Prevents dependency conflicts between Airflow's internal libraries, PySpark JVM bindings, and web dashboard packages.
- **Simplified Deployment**: Replaces multi-step local environment setups with single-command startup via `docker compose up -d`.
- **Zero Local Database Bloat**: Connects directly to existing AWS RDS MySQL and AWS S3 infrastructure without requiring redundant local database containers.

---

### 📦 Services Architecture

1. **`airflow` (`weather_airflow`)**:
   - **Image Definition**: [`Dockerfile.airflow`](file:///Users/abhaykumar/Weather-Data-Pipeline/Dockerfile.airflow) (based on `python:3.11-slim` with `openjdk-17-jre-headless`).
   - **Responsibilities**: Orchestrates the 6-stage ETL workflow (`extract_weather` $\rightarrow$ `upload_bronze` $\rightarrow$ `process_silver` $\rightarrow$ `process_gold` $\rightarrow$ `load_rds` $\rightarrow$ `pipeline_complete`).
   - **Execution Mode**: Runs `airflow standalone`, serving the scheduler, API server, and DAG processor.
   - **Ports**: Exposes `8080:8080` for the Airflow 3.x UI and API.
   - **Volumes**:
     - `./dags:/app/dags` (live DAG synchronization)
     - `./src:/app/src` (pipeline logic)
     - `./data:/app/data` (raw, silver, gold, and Power BI CSV staging)
     - `./logs:/app/logs` (task execution logs)
     - `weather_airflow_metadata:/app/airflow` (persistent Airflow SQLite metadata volume)

2. **`streamlit` (`weather_streamlit`)**:
   - **Image Definition**: [`Dockerfile.streamlit`](file:///Users/abhaykumar/Weather-Data-Pipeline/Dockerfile.streamlit) (based on `python:3.11-slim`).
   - **Responsibilities**: Serves the interactive weather analytics dashboard.
   - **Ports**: Exposes `8501:8501` mapped directly to host browser.
   - **Volumes**:
     - `./streamlit_app:/app/streamlit_app` (live dashboard code hot-reloading)
     - `./src:/app/src` (shared configuration)
     - `./data:/app/data` (lakehouse data mirror fallback)

---

### 📁 Docker Configuration Files

| File | Purpose |
| :--- | :--- |
| [`.dockerignore`](file:///Users/abhaykumar/Weather-Data-Pipeline/.dockerignore) | Excludes `.env`, virtualenv, git history, cache files, and runtime logs from the build context |
| [`Dockerfile.airflow`](file:///Users/abhaykumar/Weather-Data-Pipeline/Dockerfile.airflow) | Multi-stage image packaging Python 3.11, OpenJDK 17, Airflow 3.1.0, and PySpark 4.2.0 |
| [`Dockerfile.streamlit`](file:///Users/abhaykumar/Weather-Data-Pipeline/Dockerfile.streamlit) | Lightweight image packaging Streamlit, Plotly, MySQL Connector, and PyArrow |
| [`docker-compose.yml`](file:///Users/abhaykumar/Weather-Data-Pipeline/docker-compose.yml) | Orchestration manifest defining services, networks, volumes, port bindings, and environment passing |

---

### 📋 Prerequisites & Docker Desktop Setup

To run the containerized pipeline, Docker must be installed and running on your machine:

1. **Install Docker Desktop on macOS**:
   Using Homebrew:
   ```bash
   brew install --cask docker
   ```
   Or download directly from [Docker Official Website](https://www.docker.com/products/docker-desktop/).

2. **Start Docker Desktop**:
   Open Docker from your Applications folder. Verify installation:
   ```bash
   docker --version
   docker compose version
   ```

---

### 🚀 Running the Pipeline with Docker Compose

1. **Verify Environment Variables**:
   Ensure your `.env` file exists in the project root with valid credentials:
   ```env
   OPENWEATHER_API_KEY=your_api_key
   AWS_ACCESS_KEY_ID=your_aws_access_key
   AWS_SECRET_ACCESS_KEY=your_aws_secret_key
   AWS_REGION=ap-south-1
   AWS_S3_BUCKET=weather-data-pipeline-abhay-699258776334
   DB_HOST=weather-db.c9qcuiuy6x3m.ap-south-1.rds.amazonaws.com
   DB_PORT=3306
   DB_USER=admin
   DB_PASSWORD=your_db_password
   DB_NAME=weather_db
   ```

2. **Build Docker Images**:
   ```bash
   docker compose build
   ```

3. **Start All Services in the Background**:
   ```bash
   docker compose up -d
   ```

4. **Verify Running Containers**:
   ```bash
   docker compose ps
   ```

5. **Access Application UIs**:
   - **Streamlit Weather Dashboard**: Open [http://localhost:8501](http://localhost:8501)
   - **Apache Airflow Orchestrator UI**: Open [http://localhost:8080](http://localhost:8080)

6. **View Service Logs**:
   ```bash
   # Stream all logs
   docker compose logs -f

   # View Airflow orchestrator logs
   docker compose logs -f airflow

   # View Streamlit dashboard logs
   docker compose logs -f streamlit
   ```

7. **Stop All Services**:
   ```bash
   docker compose down
   ```

8. **Rebuild a Specific Service**:
   ```bash
   docker compose build streamlit
   docker compose up -d streamlit
   ```

---

### 🔧 Troubleshooting & Common Issues

- **Port Already in Use (`8080` or `8501`)**:
  If port 8501 is currently occupied by a local process:
  ```bash
  # Check which process is listening
  lsof -i :8501
  # Stop local Streamlit before launching the containerized service
  ```
- **AWS S3 / RDS Connection in Docker**:
  Containers share the host network stack through the bridge network `weather_pipeline_network` and communicate directly with AWS endpoints using DNS resolution. Ensure your AWS RDS Security Group allows inbound MySQL traffic (port 3306) from your external IP address.
- **Airflow Standalone Password**:
  When Airflow starts for the first time in Docker, it generates a default admin password located inside the container at `/app/airflow/standalone_admin_password.txt`. View it using:
  ```bash
  docker compose exec airflow cat /app/airflow/standalone_admin_password.txt
  ```

---

# 🎓 Data Engineering Concepts Demonstrated

This project demonstrates several important Data Engineering concepts:

- ETL Pipeline
- REST API Integration
- Data Extraction
- Raw Data Storage
- Data Validation
- Data Cleaning
- Data Transformation
- Data Loading
- Relational Database
- MySQL
- SQL Analytics
- Data Deduplication
- Error Handling
- Environment Variables
- Logging
- Workflow Orchestration
- Apache Airflow
- CSV Data Export
- Business Intelligence
- Power BI
- Historical Data Storage
- Interactive Web Dashboards
- Streamlit Analytics
- Plotly Data Visualization
- Hybrid Lakehouse & Database Serving Layer
- Containerization
- Docker & Docker Compose
- Multi-Container Orchestration
- Reproducible Data Engineering Environments

---

# 📚 Learning Outcomes

After completing this project, the following concepts are demonstrated:

### Python

- File handling
- JSON processing
- API requests
- Exception handling
- Environment variables
- Modular programming

### SQL

- SELECT
- WHERE
- GROUP BY
- ORDER BY
- Aggregate functions
- Data analysis
- Database constraints

### Data Engineering

- ETL architecture
- Data pipelines
- Data quality
- Historical storage
- Duplicate handling
- Pipeline orchestration

### Airflow

- DAG creation
- PythonOperator
- Task execution
- Manual DAG triggering
- Workflow monitoring

### Business Intelligence & Dashboards

- CSV data preparation
- Power BI data import
- KPI visualization
- Trend analysis
- City comparison
- Streamlit interactive web applications
- Plotly charts & time-series visualizations
- Live multi-dimensional filtering & dynamic queries
- S3 Gold Data Lakehouse benchmarking

### Containerization & DevOps

- Dockerfile multi-stage and runtime design
- Docker Compose multi-service coordination
- Persistent volume mounting & environment passing
- Cross-platform OpenJDK and PySpark container configuration
- Port mapping & custom bridge network architecture

---

# ☁️ AWS Cloud Deployment

This section provides complete documentation for deploying the **Weather Data Aggregation Pipeline** to Amazon Web Services (AWS), utilizing **Amazon S3** for the Lakehouse storage layer, **Amazon RDS for MySQL** for relational serving, and containerized compute via **Docker Compose** on **Amazon EC2**.

---

## 🏗️ AWS Cloud Architecture

```text
                                  AWS Cloud (Region: ap-south-1)
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                                 │
│   OpenWeather REST API                                                                          │
│            │                                                                                    │
│            ▼                                                                                    │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │ Amazon EC2 Instance (t3.medium / t3.small + 2GB swap)                                   │   │
│   │                                                                                         │   │
│   │   Docker Bridge Network (weather_pipeline_network)                                      │   │
│   │   ┌─────────────────────────────────────┐   ┌───────────────────────────────────────┐   │   │
│   │   │ Airflow Container (Port 8080)       │   │ Streamlit Container (Port 8501)       │   │   │
│   │   │ • Apache Airflow 3.1.0 Standalone   │   │ • Interactive Multi-tab UI            │   │   │
│   │   │ • PySpark 4.2.0 Engine              │   │ • 6 Real-time KPI Cards               │   │   │
│   │   │ • OpenJDK 21 Runtime                │   │ • S3 Gold Parquet Reader              │   │   │
│   │   └──────────────────┬──────────────────┘   └───────────────────┬───────────────────┘   │   │
│   └──────────────────────┼──────────────────────────────────────────┼───────────────────────┘   │
│                          │                                          │                           │
│                          ▼                                          ▼                           │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │ Amazon S3 Bucket: weather-data-pipeline-abhay-699258776334                              │   │
│   │   ├── bronze/weather/year=YYYY/month=MM/day=DD/<city>.json                              │   │
│   │   ├── silver/weather/year=YYYY/month=MM/day=DD/*.snappy.parquet                         │   │
│   │   └── gold/weather/{city_temperature, monthly_temperature, ...}/*.snappy.parquet       │   │
│   └─────────────────────────────────────────────────────────────────────────────────────────┘   │
│                          │                                          │                           │
│                          ▼                                          │                           │
│   ┌─────────────────────────────────────────────────────────────────┼───────────────────────┐   │
│   │ Amazon RDS for MySQL: weather-db.c9qcuiuy6x3m.ap-south-1.rds... │                           │   │
│   │   └── weather_db.weather_data (45 records, deduplicated) ◄──────┘                           │   │
│   └─────────────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ AWS Services Used

| Service | Purpose in Architecture | Configuration Details |
| :--- | :--- | :--- |
| **Amazon S3** | Cloud Data Lake storage for Bronze (raw JSON), Silver (cleaned Parquet), and Gold (aggregates) layers. | Bucket: `weather-data-pipeline-abhay-699258776334`<br/>Region: `ap-south-1`<br/>Encryption: SSE-S3 AES-256<br/>Public Access: Block All Public Access |
| **Amazon RDS for MySQL** | Relational serving layer powering historical SQL queries, KPI aggregations, and Streamlit charts. | Engine: MySQL Community 8.4.9<br/>Database: `weather_db`<br/>Table: `weather_data`<br/>Port: `3306`<br/>Constraint: `UNIQUE KEY (city, weather_date)` |
| **Amazon EC2** | Compute host executing the containerized Airflow orchestrator, PySpark processing engine, and Streamlit webapp. | Instance Type: `t3.medium` (or `t3.small` with 2GB swap)<br/>OS: Amazon Linux 2023 or Ubuntu 22.04 LTS<br/>Storage: 20 GB gp3 root volume |
| **AWS IAM** | Identity and Access Management providing least-privilege credentials for S3 operations and EC2 roles. | User: `weather-pipeline-user`<br/>Policy: Scoped `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` |
| **Docker & Docker Compose** | Application isolation, dependency encapsulation, and multi-service orchestration on the compute host. | Bridge network: `weather_pipeline_network`<br/>Volumes: Persistent Airflow metadata and data staging |

---

## 📋 Prerequisites

Before deploying to AWS, ensure the following are configured:

1. **AWS Account**: Active AWS account with permissions in region `ap-south-1` (Mumbai).
2. **AWS CLI**: Installed and configured locally (`aws configure`) with credentials for region `ap-south-1`.
3. **OpenWeather API Key**: Valid API key from [OpenWeatherMap](https://openweathermap.org/api).
4. **SSH Key Pair**: An EC2 key pair (`.pem` file) created in `ap-south-1` for secure instance access.
5. **Docker Desktop** (local development) or Docker Engine on EC2.

---

## ⚙️ Configuration & Secrets Management

Configuration is managed strictly through environment variables. **Credentials are never hard-coded or committed to version control.**

Create or configure your `.env` file in the project root:

```env
# OpenWeather API Configuration
WEATHER_API_KEY=your_openweather_api_key

# AWS Cloud Configuration
AWS_REGION=ap-south-1
AWS_S3_BUCKET=weather-data-pipeline-abhay-699258776334

# AWS RDS MySQL Configuration
MYSQL_HOST=weather-db.c9qcuiuy6x3m.ap-south-1.rds.amazonaws.com
MYSQL_PORT=3306
MYSQL_USER=admin
MYSQL_PASSWORD=your_secure_rds_password
MYSQL_DATABASE=weather_db
```

> [!CAUTION]
> Ensure `.env` is listed in `.gitignore` and `.dockerignore`. Never commit database passwords, AWS secret keys, or API tokens to source control.

---

## 🚀 Step-by-Step AWS Deployment Guide

### 1. Launch Amazon EC2 Compute Instance

1. Open the **AWS Management Console** $\rightarrow$ **EC2** in region `ap-south-1`.
2. Click **Launch Instance**:
   - **Name**: `weather-pipeline-server`
   - **AMI**: Amazon Linux 2023 AMI or Ubuntu 22.04 LTS
   - **Instance Type**: `t3.medium` (2 vCPU, 4GB RAM) or `t3.small` (2 vCPU, 2GB RAM)
   - **Key Pair**: Select your existing key pair
   - **Storage**: 20 GiB gp3
3. **Security Group Rules** (Least Privilege):
   - **SSH** (TCP port 22): Restrict to `My IP` (`<your-public-ip>/32`).
   - **Airflow Web UI** (TCP port 8080): Restrict to `My IP` or internal VPC.
   - **Streamlit Dashboard** (TCP port 8501): Restrict to `My IP` or internal VPC.
   - **Outbound**: All traffic (allows downloading Docker packages and calling OpenWeather API).

### 2. Connect to the EC2 Instance

```bash
ssh -i /path/to/your-key.pem ec2-user@<EC2-PUBLIC-IP>
# (Or ubuntu@<EC2-PUBLIC-IP> if using Ubuntu)
```

### 3. Deploy via Automated Bootstrap Script

Clone your repository onto the EC2 instance and run the provided automated deployment script:

```bash
# 1. Clone repository
git clone https://github.com/<your-username>/Weather-Data-Pipeline.git
cd Weather-Data-Pipeline

# 2. Configure .env with your credentials
cp .env.example .env
nano .env

# 3. Execute the automated bootstrap script
chmod +x scripts/deploy_aws_ec2.sh
./scripts/deploy_aws_ec2.sh
```

The bootstrap script automatically:
- Installs Docker Engine and Docker Compose plugin.
- Allocates and activates 2GB virtual swap memory to guarantee stability during PySpark operations.
- Verifies `.env` and required staging directories.
- Builds and launches the multi-container stack in detached mode (`-d`).

---

## 🗄️ AWS S3 Data Lake Setup

The S3 bucket uses a structured prefix hierarchy separating raw, cleaned, and analytical datasets:

```text
s3://weather-data-pipeline-abhay-699258776334/
├── bronze/
│   └── weather/
│       └── year=2026/month=09/day=10/
│           ├── delhi.json
│           ├── mumbai.json
│           └── ...
├── silver/
│   └── weather/
│       └── year=2026/month=9/day=10/
│           ├── part-00000-*.snappy.parquet
│           └── _SUCCESS
└── gold/
    └── weather/
        ├── city_temperature/
        ├── monthly_temperature/
        ├── city_weather_summary/
        ├── humidity_trends/
        ├── wind_trends/
        └── rainfall_trends/
```

- **Encryption**: Server-Side Encryption with Amazon S3 managed keys (SSE-S3 AES-256).
- **Public Access**: S3 Block Public Access is fully enabled (all 4 settings: `BlockPublicAcls`, `IgnorePublicAcls`, `BlockPublicPolicy`, `RestrictPublicBuckets`).

---

## 🐬 AWS RDS MySQL Setup & Schema

1. The database instance is provisioned on MySQL Community 8.4.9.
2. Initialize the table schema using the provided SQL script:

```bash
# Apply schema to AWS RDS
mysql -h weather-db.c9qcuiuy6x3m.ap-south-1.rds.amazonaws.com -P 3306 -u admin -p weather_db < sql/create_tables.sql
```

3. **Table Schema**:

```sql
CREATE TABLE IF NOT EXISTS weather_data (
    id INT NOT NULL AUTO_INCREMENT,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(10),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    temperature DECIMAL(5,2),
    feels_like DECIMAL(5,2),
    humidity INT,
    pressure INT,
    wind_speed DECIMAL(5,2),
    precipitation_1h DECIMAL(6,2),
    weather_condition VARCHAR(50),
    weather_description VARCHAR(100),
    timestamp BIGINT,
    date_time DATETIME,
    temperature_category VARCHAR(20),
    weather_date DATE NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY unique_city_date (city, weather_date)
);
```

Idempotent ingestion is enforced via `INSERT IGNORE`, preventing duplicate `(city, weather_date)` entries.

---

## 🔄 Airflow & Streamlit Operation

### Accessing the Web Interfaces

- **Airflow Web UI**: `http://<EC2-PUBLIC-IP>:8080`
  - **Username**: `admin`
  - **Password**: Found in `simple_auth_manager_passwords.json.generated` inside the container or generated at first startup.
- **Streamlit Dashboard**: `http://<EC2-PUBLIC-IP>:8501`
  - Publicly accessible over HTTP on port 8501 (restricted to your IP via Security Group).

### Triggering the Pipeline Workflow

In the Airflow Web UI:
1. Locate the DAG **`weather_data_pipeline`**.
2. Toggle the switch to **Unpause**.
3. Click the **Trigger DAG** (▶) button.
4. Watch the 6-stage workflow execute:
   `extract_weather` $\rightarrow$ `upload_bronze` $\rightarrow$ `process_silver` $\rightarrow$ `process_gold` $\rightarrow$ `load_rds` $\rightarrow$ `pipeline_complete`.

Alternatively, trigger from the command line:

```bash
docker exec weather_airflow airflow dags trigger weather_data_pipeline
```

---

## 🧪 Cloud Deployment Verification

Execute the automated cloud verification script to validate all connected AWS services:

```bash
PYTHONPATH=src python scripts/verify_aws_deployment.py
```

### Verified Results

```text
############################################################
      Weather Data Pipeline — AWS Cloud Verification        
############################################################

============================================================
1. AWS S3 DATA LAKE VERIFICATION
============================================================
Target Region: ap-south-1
Target Bucket: weather-data-pipeline-abhay-699258776334
✅ Bucket exists in region: ap-south-1
✅ Server-side encryption: AES256
✅ Bronze Layer (Raw JSON):     40 objects
✅ Silver Layer (Parquet):      90 objects
✅ Gold Layer (Analytics):      78 objects

============================================================
2. AWS RDS MYSQL VERIFICATION
============================================================
Host:     weather-db.***.ap-south-1.rds.amazonaws.com:3306
Database: weather_db
User:     admin
✅ Successfully connected to AWS RDS MySQL in 265.8ms
✅ Server Version: 8.4.9
✅ Table 'weather_data' verified.
✅ Total Records: 45
✅ Data Integrity: 0 duplicate (city, weather_date) records found.

============================================================
3. OPENWEATHER REST API VERIFICATION
============================================================
✅ OpenWeather API responsive: Delhi is 28.0°C (HTTP 200)

============================================================
4. AIRFLOW ORCHESTRATION VERIFICATION
============================================================
✅ DAG 'weather_data_pipeline' loaded successfully with 6 tasks:
   • extract_weather
   • upload_bronze
   • process_silver
   • process_gold
   • load_rds
   • pipeline_complete

============================================================
5. STREAMLIT DATA LAYER VERIFICATION
============================================================
✅ Distinct Cities available: ['Bengaluru', 'Chennai', 'Delhi', 'Kolkata', 'Mumbai']
✅ Date Range Bounds: 2026-09-04 to 2026-09-20
✅ KPI Summary: 45 records | Avg Temp: 28.61°C

============================================================
VERIFICATION SUMMARY
============================================================
  AWS S3 Data Lake:         [PASS]
  AWS RDS MySQL:            [PASS]
  OpenWeather API:          [PASS]
  Airflow DAG Definition:   [PASS]
  Streamlit Data Layer:     [PASS]
============================================================
🎉 ALL CLOUD COMPONENTS VERIFIED SUCCESSFULLY!
```

---

## 💰 Cost Optimization & Management

AWS services incur charges based on active usage. Follow these practices to minimize costs:

| Resource | Billing Model | Typical Cost (ap-south-1) | Cost Optimization Strategy |
| :--- | :--- | :--- | :--- |
| **Amazon EC2 (`t3.medium`)** | Per second when running | ~$0.0416 / hour (~$1.00 / day) | **Stop the instance** when not in use. When stopped, compute charges drop to $0.00. |
| **Amazon RDS (db.t3.micro / db.t4g.micro)** | Per second when running | ~$0.018 / hour (~$13 / month) | Stop the DB instance when not developing (AWS allows stopping for up to 7 days). |
| **Amazon S3 (Standard Storage)** | Per GB-month + API requests | ~$0.023 / GB-month | Negligible for weather data (<100 MB). Objects can be archived to Glacier if needed. |
| **EBS Root Volume (20GB gp3)** | Per GB-month | ~$1.60 / month ($0.08/GB-month) | Persists when EC2 is stopped; delete volume only when terminating the project. |

### How to Stop Compute When Idle

```bash
# In AWS Console or via AWS CLI:
aws ec2 stop-instances --instance-ids <YOUR-INSTANCE-ID> --region ap-south-1
```

---

## 🧹 Resource Cleanup Guide

When you have finished using the cloud deployment and wish to decommission resources:

1. **Stop Containers**:
   ```bash
   docker compose down -v
   ```
2. **Terminate EC2 Instance**:
   - Go to EC2 Console $\rightarrow$ Select `weather-pipeline-server` $\rightarrow$ **Instance State** $\rightarrow$ **Terminate Instance**.
   - This removes the instance and releases its attached root volume.
3. **Delete RDS MySQL Database** (Only if you no longer need the data):
   - Go to RDS Console $\rightarrow$ Select `weather-db` $\rightarrow$ **Actions** $\rightarrow$ **Delete**.
   - Choose whether to create a final snapshot.
4. **Delete S3 Bucket Objects** (Only if cleaning up entirely):
   - Empty the bucket before deleting:
     ```bash
     aws s3 rm s3://weather-data-pipeline-abhay-699258776334/ --recursive
     aws s3 rb s3://weather-data-pipeline-abhay-699258776334
     ```

> [!WARNING]
> Deleting S3 bucket objects or the RDS instance permanently destroys all stored historical weather records and Parquet datasets. Always take a local database dump or backup before deletion.

---

## ⚠️ Limitations & Architectural Scope

- **Compute Architecture**: Single-node containerized deployment on EC2. PySpark runs in local multi-threaded mode (`local[*]`) on the host JVM; it is not a distributed multi-node cluster (such as Amazon EMR).
- **Orchestration**: Airflow operates in Standalone mode with Simple Auth Manager for development and portfolio purposes. It is not configured for high-availability multi-node CeleryExecutor.
- **Scheduling**: The DAG is configured with `schedule=None` for manual, controlled triggering. It can be scheduled automatically (e.g. daily `0 6 * * *`) by setting the `AIRFLOW_SCHEDULE` environment variable.
- **Streamlit Security**: The dashboard serves over standard HTTP on port 8501. In an enterprise production deployment, an Application Load Balancer (ALB) with an SSL/TLS certificate (AWS Certificate Manager) and reverse proxy would be recommended.
- **Power BI Integration**: Power BI desktop connects either to AWS RDS MySQL directly or imports the exported CSV dataset (`data/powerbi/weather_data.csv`). Power BI is not replaced by Streamlit; both co-exist.

---

# 🌐 Streamlit Community Cloud Public Deployment

This section guides you through deploying the interactive **Streamlit Weather Dashboard** publicly to **Streamlit Community Cloud** (e.g. `https://my-weather-dashboard.streamlit.app/`), enabling anyone to access the live dashboard from laptops, tablets, or mobile phones without installing Python, Docker, or database tools.

---

## 🏗️ Public Cloud Architecture

```text
               Streamlit Community Cloud (Zero Server Management)
┌─────────────────────────────────────────────────────────────────┐
│  Mobile & Desktop Web Browsers (Any Device, Globally)           │
│         │                                                       │
│         ▼                                                       │
│  Public URL: https://<your-subdomain>.streamlit.app             │
│         │                                                       │
│         ▼                                                       │
│  Streamlit Cloud Container (streamlit_app/app.py)               │
│  • Reads database credentials securely from st.secrets          │
│  • Installs dependencies from streamlit_app/requirements.txt    │
│  • Caches queries with @st.cache_data(ttl=300)                  │
│  • Renders 6 KPI cards, Plotly charts, and 7 analytical tabs    │
└────────────────┬───────────────────────────────┬────────────────┘
                 │ (Port 3306 MySQL TCP)         │ (HTTPS S3 API)
                 ▼                               ▼
      ┌──────────────────────┐       ┌─────────────────────────┐
      │  AWS RDS MySQL       │       │  AWS S3 Gold Lakehouse  │
      │  (weather_db)        │       │  (Parquet Datasets)     │
      └──────────────────────┘       └─────────────────────────┘
```

---

## 🛠️ Step-by-Step Deployment Guide

### 1. Prerequisites
- A **GitHub account** with access to this repository (`Abha2059/Weather-Data-Pipeline`).
- A free **Streamlit Community Cloud** account at [share.streamlit.io](https://share.streamlit.io).
- Active **AWS RDS MySQL** instance (`weather-db.c9qcuiuy6x3m.ap-south-1.rds.amazonaws.com:3306`).

### 2. Push Code to GitHub
Ensure all recent changes, including `streamlit_app/requirements.txt` and `.streamlit/config.toml`, are committed and pushed to your GitHub `main` branch:

```bash
git add .
git commit -m "Configure Streamlit Community Cloud deployment"
git push origin main
```

### 3. Deploy on Streamlit Community Cloud
1. Open [share.streamlit.io](https://share.streamlit.io) and log in with your GitHub account.
2. Click **Create app** (or **New app**).
3. Fill in the deployment form:
   - **Repository**: `Abha2059/Weather-Data-Pipeline`
   - **Branch**: `main`
   - **Main file path**: `streamlit_app/app.py`
   - **App URL**: `my-weather-dashboard` (choose your preferred subdomain, e.g. `abhay-weather-dashboard.streamlit.app`)

### 4. Configure Cloud Secrets (Critical)
1. Click **Advanced settings...** at the bottom of the deployment modal (or navigate to **App Settings $\rightarrow$ Secrets** if already created).
2. In the **Secrets** editor, paste your configuration in TOML format:

```toml
# AWS RDS MySQL Configuration
MYSQL_HOST = "weather-db.c9qcuiuy6x3m.ap-south-1.rds.amazonaws.com"
MYSQL_PORT = 3306
MYSQL_USER = "admin"
MYSQL_PASSWORD = "your_actual_rds_password"
MYSQL_DATABASE = "weather_db"

# Optional: AWS S3 Data Lake Configuration
AWS_DEFAULT_REGION = "ap-south-1"
AWS_S3_BUCKET = "weather-data-pipeline-abhay-699258776334"
```

> [!CAUTION]
> Replace `your_actual_rds_password` with your real RDS MySQL master password. Never commit this password to GitHub.

3. Click **Save** $\rightarrow$ **Deploy!**

### 5. Verify Your Live Public URL
Streamlit Community Cloud will clone your repository, install packages from `streamlit_app/requirements.txt` in ~30 seconds, connect to AWS RDS MySQL, and generate your live public URL:
```text
https://my-weather-dashboard.streamlit.app/
```
You can now share this URL with recruiters, colleagues, and friends to view on any browser or smartphone!

---

## 🔍 Troubleshooting Deployment Errors

| Problem | Cause | Solution |
| :--- | :--- | :--- |
| **"Unable to connect to database"** | Missing or incorrect secrets in Streamlit Cloud. | Open **App Settings $\rightarrow$ Secrets** in Streamlit Cloud, confirm `MYSQL_HOST`, `MYSQL_USER`, and `MYSQL_PASSWORD` are spelled correctly and saved. |
| **"Can't connect to MySQL server (timeout)"** | AWS RDS Security Group blocking port 3306. | In the AWS RDS Console $\rightarrow$ Connectivity & Security $\rightarrow$ VPC Security Groups $\rightarrow$ Inbound rules: Ensure port 3306 allows inbound connections from `0.0.0.0/0` (with strong user authentication). |
| **"ModuleNotFoundError: No module named '...'"** | Dependency missing from requirements. | Verify the dependency is declared in `streamlit_app/requirements.txt`. |
| **Build Timeout / Resource Exhaustion** | Heavy libraries like PySpark or Airflow being built. | Streamlit Cloud uses `streamlit_app/requirements.txt`, which cleanly excludes Airflow and PySpark. Ensure the entry point is set to `streamlit_app/app.py`. |

---

# 👨‍💻 Author

**Abhay Kumar**

Data Engineering Project

**Project:** Weather Data Aggregation Pipeline

---

# 📄 License

This project is created for educational, learning, and portfolio purposes.