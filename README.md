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

### Business Intelligence

- CSV data preparation
- Power BI data import
- KPI visualization
- Trend analysis
- City comparison

---

# 👨‍💻 Author

**Abhay Kumar**

Data Engineering Project

**Project:** Weather Data Aggregation Pipeline

---

# 📄 License

This project is created for educational, learning, and portfolio purposes.