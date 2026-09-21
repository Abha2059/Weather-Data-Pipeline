import pandas as pd
try:
    from database import run_query
except ImportError:
    from .database import run_query


def _build_filter_clause(selected_cities, start_date, end_date, selected_condition=None):
    """
    Constructs a dynamic WHERE clause and parameter list for SQL queries.
    Uses parameterized placeholders to prevent SQL injection.
    """
    conditions = []
    params = []

    if selected_cities:
        placeholders = ", ".join(["%s"] * len(selected_cities))
        conditions.append(f"city IN ({placeholders})")
        params.extend(selected_cities)

    if start_date:
        conditions.append("weather_date >= %s")
        params.append(str(start_date))

    if end_date:
        conditions.append("weather_date <= %s")
        params.append(str(end_date))

    if selected_condition and selected_condition != "All":
        conditions.append("weather_condition = %s")
        params.append(selected_condition)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    return where_clause, params


def get_available_cities() -> list[str]:
    """
    Fetches the distinct list of cities currently stored in the database.
    """
    sql = "SELECT DISTINCT city FROM weather_data ORDER BY city ASC;"
    df = run_query(sql)
    if not df.empty:
        return df["city"].tolist()
    return []


def get_date_bounds():
    """
    Retrieves the minimum and maximum observation dates available in the database.
    """
    sql = "SELECT MIN(weather_date) as min_date, MAX(weather_date) as max_date FROM weather_data;"
    df = run_query(sql)
    if not df.empty and df.iloc[0]["min_date"] is not None:
        return df.iloc[0]["min_date"], df.iloc[0]["max_date"]
    return None, None


def get_available_conditions() -> list[str]:
    """
    Retrieves the unique weather conditions (e.g. Clouds, Rain, Clear).
    """
    sql = "SELECT DISTINCT weather_condition FROM weather_data WHERE weather_condition IS NOT NULL ORDER BY weather_condition ASC;"
    df = run_query(sql)
    if not df.empty:
        return ["All"] + df["weather_condition"].tolist()
    return ["All"]


def get_kpi_summary(selected_cities, start_date, end_date, selected_condition=None) -> dict:
    """
    Calculates summary key performance indicators (KPIs) based on the active filters.
    """
    where_clause, params = _build_filter_clause(selected_cities, start_date, end_date, selected_condition)

    sql = f"""
        SELECT
            COUNT(*) AS record_count,
            ROUND(AVG(temperature), 2) AS avg_temp,
            ROUND(MAX(temperature), 2) AS max_temp,
            ROUND(MIN(temperature), 2) AS min_temp,
            ROUND(AVG(feels_like), 2) AS avg_feels_like,
            ROUND(AVG(humidity), 2) AS avg_humidity,
            ROUND(AVG(pressure), 2) AS avg_pressure,
            ROUND(AVG(wind_speed), 2) AS avg_wind_speed,
            ROUND(SUM(COALESCE(precipitation_1h, 0)), 2) AS total_precipitation
        FROM weather_data
        {where_clause};
    """
    df = run_query(sql, params)
    if not df.empty:
        row = df.iloc[0].to_dict()
        return {k: (v if pd.notnull(v) else 0) for k, v in row.items()}
    return {
        "record_count": 0, "avg_temp": 0.0, "max_temp": 0.0, "min_temp": 0.0,
        "avg_feels_like": 0.0, "avg_humidity": 0.0, "avg_pressure": 0.0,
        "avg_wind_speed": 0.0, "total_precipitation": 0.0
    }


def get_daily_trends(selected_cities, start_date, end_date, selected_condition=None) -> pd.DataFrame:
    """
    Returns daily aggregated temperature, humidity, and wind speed trends over time.
    """
    where_clause, params = _build_filter_clause(selected_cities, start_date, end_date, selected_condition)

    sql = f"""
        SELECT
            weather_date,
            city,
            ROUND(AVG(temperature), 2) AS avg_temp,
            ROUND(MAX(temperature), 2) AS max_temp,
            ROUND(MIN(temperature), 2) AS min_temp,
            ROUND(AVG(humidity), 2) AS avg_humidity,
            ROUND(AVG(wind_speed), 2) AS avg_wind_speed,
            ROUND(SUM(COALESCE(precipitation_1h, 0)), 2) AS total_precipitation
        FROM weather_data
        {where_clause}
        GROUP BY weather_date, city
        ORDER BY weather_date ASC, city ASC;
    """
    return run_query(sql, params)


def get_city_comparison(selected_cities, start_date, end_date, selected_condition=None) -> pd.DataFrame:
    """
    Returns comparative weather statistics grouped by city.
    """
    where_clause, params = _build_filter_clause(selected_cities, start_date, end_date, selected_condition)

    sql = f"""
        SELECT
            city,
            country,
            COUNT(*) AS total_records,
            ROUND(AVG(temperature), 2) AS avg_temp,
            ROUND(MAX(temperature), 2) AS max_temp,
            ROUND(MIN(temperature), 2) AS min_temp,
            ROUND(AVG(humidity), 2) AS avg_humidity,
            ROUND(AVG(pressure), 2) AS avg_pressure,
            ROUND(AVG(wind_speed), 2) AS avg_wind_speed,
            ROUND(SUM(COALESCE(precipitation_1h, 0)), 2) AS total_precipitation
        FROM weather_data
        {where_clause}
        GROUP BY city, country
        ORDER BY avg_temp DESC;
    """
    return run_query(sql, params)


def get_monthly_trends(selected_cities, selected_condition=None) -> pd.DataFrame:
    """
    Returns monthly aggregated trends by year and month.
    """
    conditions = []
    params = []

    if selected_cities:
        placeholders = ", ".join(["%s"] * len(selected_cities))
        conditions.append(f"city IN ({placeholders})")
        params.extend(selected_cities)

    if selected_condition and selected_condition != "All":
        conditions.append("weather_condition = %s")
        params.append(selected_condition)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT
            SUBSTRING(weather_date, 1, 7) AS weather_month,
            city,
            ROUND(AVG(temperature), 2) AS avg_temp,
            ROUND(MAX(temperature), 2) AS max_temp,
            ROUND(MIN(temperature), 2) AS min_temp,
            ROUND(AVG(humidity), 2) AS avg_humidity,
            ROUND(AVG(wind_speed), 2) AS avg_wind_speed,
            ROUND(SUM(COALESCE(precipitation_1h, 0)), 2) AS total_precipitation,
            COUNT(*) AS record_count
        FROM weather_data
        {where_clause}
        GROUP BY weather_month, city
        ORDER BY weather_month ASC, city ASC;
    """
    df = run_query(sql, params)
    if not df.empty and "weather_month" in df.columns:
        df["year_month"] = df["weather_month"]
    return df


def get_filtered_records(selected_cities, start_date, end_date, selected_condition=None, limit: int = 200) -> pd.DataFrame:
    """
    Retrieves raw tabular records matching the active filters for user inspection.
    """
    where_clause, params = _build_filter_clause(selected_cities, start_date, end_date, selected_condition)
    params.append(int(limit))

    sql = f"""
        SELECT
            id,
            city,
            country,
            weather_date,
            date_time,
            temperature,
            feels_like,
            humidity,
            pressure,
            wind_speed,
            precipitation_1h,
            weather_condition,
            weather_description,
            temperature_category
        FROM weather_data
        {where_clause}
        ORDER BY weather_date DESC, city ASC
        LIMIT %s;
    """
    return run_query(sql, params)
