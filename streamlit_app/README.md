# Streamlit Weather Analytics Dashboard

An interactive dashboard for the **Weather Data Aggregation Pipeline**, visualizing live weather observations and historical metrics stored in **AWS RDS MySQL** and **AWS S3 Gold Data Lake**.

---

## 🚀 Quickstart

### 1. Prerequisites
Ensure your virtual environment is active and dependencies are installed:
```bash
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Environment Variables
Verify your `.env` file contains valid AWS RDS MySQL connection details:
```ini
MYSQL_HOST=weather-db.c9qcuiuy6x3m.ap-south-1.rds.amazonaws.com
MYSQL_PORT=3306
MYSQL_USER=admin
MYSQL_PASSWORD=YourPassword
MYSQL_DATABASE=weather_db
```

### 3. Launch the Dashboard
Run the Streamlit application from the project root:
```bash
streamlit run streamlit_app/app.py
```

The application will open automatically in your browser at:
`http://localhost:8501`

---

## 📊 Dashboard Features

- **Sidebar Filters**: Multi-city selector, date range bounds from the database, weather conditions, and instant cache refresh.
- **KPI Summary**: Average Temperature, Maximum/Minimum Extremes, Humidity, Wind Speed, and 1-Hour Precipitation.
- **Analytics Tabs**:
  1. **Temperature Trends**: Daily progression line charts, city extremes comparison, and weather condition distributions.
  2. **Humidity & Wind**: Daily humidity trends and wind speed progression.
  3. **Precipitation**: Cumulative 1-hour recorded precipitation by city.
  4. **City Comparison**: Cross-city weather comparison matrix.
  5. **Monthly Trends**: Aggregated metrics grouped by `YYYY-MM`.
  6. **S3 Gold Lakehouse**: PySpark Gold Parquet datasets directly from the Data Lake.
  7. **Detailed Records**: Filtered tabular data with CSV download capability.

---

## ☁️ Deploying to Streamlit Community Cloud

To deploy this dashboard publicly with zero server maintenance:

1. **Push Changes to GitHub**:
   Ensure all changes are committed and pushed to your `main` branch.

2. **Open Streamlit Community Cloud**:
   Visit [share.streamlit.io](https://share.streamlit.io) and sign in with your GitHub account.

3. **Deploy a New App**:
   - **Repository**: `Abha2059/Weather-Data-Pipeline`
   - **Branch**: `main`
   - **Main file path**: `streamlit_app/app.py`
   - **App URL**: `my-weather-dashboard` (or your chosen subdomain)

4. **Configure Secrets**:
   Click **Advanced settings...** $\rightarrow$ **Secrets** and paste:
   ```toml
   MYSQL_HOST = "weather-db.c9qcuiuy6x3m.ap-south-1.rds.amazonaws.com"
   MYSQL_PORT = 3306
   MYSQL_USER = "admin"
   MYSQL_PASSWORD = "your_secure_rds_password"
   MYSQL_DATABASE = "weather_db"

   # Optional S3 Lakehouse access
   AWS_DEFAULT_REGION = "ap-south-1"
   AWS_S3_BUCKET = "weather-data-pipeline-abhay-699258776334"
   ```

5. **Deploy**:
   Click **Deploy!** Your public URL will be live in ~45 seconds.

