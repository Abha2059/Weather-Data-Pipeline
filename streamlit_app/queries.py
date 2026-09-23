"""
Analytical Query Engine for Weather Dashboard.
Executes high-performance in-memory transformations and aggregations
over AWS S3 Silver Parquet datasets (and local cache fallback).
Completely serverless - zero database dependency.
"""

from datetime import datetime, date
import pandas as pd

try:
    from s3_data_loader import load_silver_weather_data
except ImportError:
    from .s3_data_loader import load_silver_weather_data


def _get_base_dataframe() -> pd.DataFrame:
    """Retrieves the underlying Silver weather dataset."""
    df = load_silver_weather_data()
    if df is not None and not df.empty:
        return df.copy()
    return pd.DataFrame()


def _filter_dataframe(
    df: pd.DataFrame,
    selected_cities: list[str] | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    selected_condition: str | None = None
) -> pd.DataFrame:
    """
    Applies city, date range, and weather condition filters in-memory.
    """
    if df.empty:
        return df

    filtered = df.copy()

    # City filter
    if selected_cities:
        filtered = filtered[filtered["city"].isin(selected_cities)]

    # Normalize weather_date column to datetime.date for consistent comparison
    if "weather_date" in filtered.columns:
        if not isinstance(filtered["weather_date"].iloc[0], date):
            filtered["weather_date"] = pd.to_datetime(filtered["weather_date"]).dt.date

    # Date bounds
    if start_date is not None:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        filtered = filtered[filtered["weather_date"] >= start_date]

    if end_date is not None:
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        filtered = filtered[filtered["weather_date"] <= end_date]

    # Weather condition
    if selected_condition and selected_condition != "All":
        filtered = filtered[filtered["weather_condition"] == selected_condition]

    return filtered


def get_available_cities() -> list[str]:
    """
    Fetches the distinct list of cities currently stored in the Silver Lakehouse dataset.
    """
    df = _get_base_dataframe()
    if not df.empty and "city" in df.columns:
        return sorted(df["city"].dropna().unique().tolist())
    return []


def get_date_bounds():
    """
    Retrieves the minimum and maximum observation dates available in the Silver dataset.
    """
    df = _get_base_dataframe()
    if not df.empty and "weather_date" in df.columns:
        dates = pd.to_datetime(df["weather_date"]).dt.date
        min_date = dates.min()
        max_date = dates.max()
        if pd.notnull(min_date) and pd.notnull(max_date):
            return min_date, max_date
    return None, None


def get_available_conditions() -> list[str]:
    """
    Retrieves the unique weather conditions (e.g. Clouds, Rain, Clear).
    """
    df = _get_base_dataframe()
    if not df.empty and "weather_condition" in df.columns:
        conditions = sorted(df["weather_condition"].dropna().unique().tolist())
        return ["All"] + conditions
    return ["All"]


def get_kpi_summary(selected_cities, start_date, end_date, selected_condition=None) -> dict:
    """
    Calculates summary key performance indicators (KPIs) based on the active filters.
    """
    df = _get_base_dataframe()
    filtered = _filter_dataframe(df, selected_cities, start_date, end_date, selected_condition)

    if filtered.empty:
        return {
            "record_count": 0, "avg_temp": 0.0, "max_temp": 0.0, "min_temp": 0.0,
            "avg_feels_like": 0.0, "avg_humidity": 0.0, "avg_pressure": 0.0,
            "avg_wind_speed": 0.0, "total_precipitation": 0.0
        }

    return {
        "record_count": int(len(filtered)),
        "avg_temp": round(float(filtered["temperature"].mean()), 2) if "temperature" in filtered else 0.0,
        "max_temp": round(float(filtered["temperature"].max()), 2) if "temperature" in filtered else 0.0,
        "min_temp": round(float(filtered["temperature"].min()), 2) if "temperature" in filtered else 0.0,
        "avg_feels_like": round(float(filtered["feels_like"].mean()), 2) if "feels_like" in filtered else 0.0,
        "avg_humidity": round(float(filtered["humidity"].mean()), 2) if "humidity" in filtered else 0.0,
        "avg_pressure": round(float(filtered["pressure"].mean()), 2) if "pressure" in filtered else 0.0,
        "avg_wind_speed": round(float(filtered["wind_speed"].mean()), 2) if "wind_speed" in filtered else 0.0,
        "total_precipitation": round(float(filtered["precipitation_1h"].fillna(0).sum()), 2) if "precipitation_1h" in filtered else 0.0,
    }


def get_daily_trends(selected_cities, start_date, end_date, selected_condition=None) -> pd.DataFrame:
    """
    Returns daily aggregated temperature, humidity, and wind speed trends over time.
    """
    df = _get_base_dataframe()
    filtered = _filter_dataframe(df, selected_cities, start_date, end_date, selected_condition)

    if filtered.empty or "weather_date" not in filtered.columns or "city" not in filtered.columns:
        return pd.DataFrame(columns=[
            "weather_date", "city", "avg_temp", "max_temp", "min_temp",
            "avg_humidity", "avg_wind_speed", "total_precipitation"
        ])

    grouped = filtered.groupby(["weather_date", "city"], as_index=False).agg(
        avg_temp=("temperature", "mean"),
        max_temp=("temperature", "max"),
        min_temp=("temperature", "min"),
        avg_humidity=("humidity", "mean"),
        avg_wind_speed=("wind_speed", "mean"),
        total_precipitation=("precipitation_1h", "sum")
    )

    for col in ["avg_temp", "max_temp", "min_temp", "avg_humidity", "avg_wind_speed", "total_precipitation"]:
        if col in grouped.columns:
            grouped[col] = grouped[col].round(2)

    return grouped.sort_values(by=["weather_date", "city"], ascending=[True, True]).reset_index(drop=True)


def get_city_comparison(selected_cities, start_date, end_date, selected_condition=None) -> pd.DataFrame:
    """
    Returns comparative weather statistics grouped by city.
    """
    df = _get_base_dataframe()
    filtered = _filter_dataframe(df, selected_cities, start_date, end_date, selected_condition)

    if filtered.empty or "city" not in filtered.columns:
        return pd.DataFrame(columns=[
            "city", "country", "total_records", "avg_temp", "max_temp", "min_temp",
            "avg_humidity", "avg_pressure", "avg_wind_speed", "total_precipitation"
        ])

    country_col = "country" if "country" in filtered.columns else "city"
    group_cols = ["city", "country"] if "country" in filtered.columns else ["city"]

    grouped = filtered.groupby(group_cols, as_index=False).agg(
        total_records=("city", "count"),
        avg_temp=("temperature", "mean"),
        max_temp=("temperature", "max"),
        min_temp=("temperature", "min"),
        avg_humidity=("humidity", "mean"),
        avg_pressure=("pressure", "mean"),
        avg_wind_speed=("wind_speed", "mean"),
        total_precipitation=("precipitation_1h", "sum")
    )

    if "country" not in grouped.columns:
        grouped["country"] = "IN"

    for col in ["avg_temp", "max_temp", "min_temp", "avg_humidity", "avg_pressure", "avg_wind_speed", "total_precipitation"]:
        if col in grouped.columns:
            grouped[col] = grouped[col].round(2)

    return grouped.sort_values(by="avg_temp", ascending=False).reset_index(drop=True)


def get_monthly_trends(selected_cities, selected_condition=None) -> pd.DataFrame:
    """
    Returns monthly aggregated trends by year and month.
    """
    df = _get_base_dataframe()
    filtered = _filter_dataframe(df, selected_cities=selected_cities, selected_condition=selected_condition)

    if filtered.empty or "weather_date" not in filtered.columns or "city" not in filtered.columns:
        return pd.DataFrame(columns=[
            "weather_month", "city", "avg_temp", "max_temp", "min_temp",
            "avg_humidity", "avg_wind_speed", "total_precipitation", "record_count", "year_month"
        ])

    filtered["weather_month"] = filtered["weather_date"].astype(str).str.slice(0, 7)

    grouped = filtered.groupby(["weather_month", "city"], as_index=False).agg(
        avg_temp=("temperature", "mean"),
        max_temp=("temperature", "max"),
        min_temp=("temperature", "min"),
        avg_humidity=("humidity", "mean"),
        avg_wind_speed=("wind_speed", "mean"),
        total_precipitation=("precipitation_1h", "sum"),
        record_count=("city", "count")
    )

    for col in ["avg_temp", "max_temp", "min_temp", "avg_humidity", "avg_wind_speed", "total_precipitation"]:
        if col in grouped.columns:
            grouped[col] = grouped[col].round(2)

    grouped["year_month"] = grouped["weather_month"]
    return grouped.sort_values(by=["weather_month", "city"], ascending=[True, True]).reset_index(drop=True)


def get_filtered_records(selected_cities, start_date, end_date, selected_condition=None, limit: int = 200) -> pd.DataFrame:
    """
    Retrieves raw tabular records matching the active filters for user inspection.
    """
    df = _get_base_dataframe()
    filtered = _filter_dataframe(df, selected_cities, start_date, end_date, selected_condition)

    if filtered.empty:
        return pd.DataFrame()

    # Sort most recent first
    sort_cols = [c for c in ["weather_date", "city"] if c in filtered.columns]
    if sort_cols:
        filtered = filtered.sort_values(by=sort_cols, ascending=[False, True])

    if limit and limit > 0:
        filtered = filtered.head(limit)

    filtered = filtered.reset_index(drop=True)
    if "id" not in filtered.columns:
        filtered.insert(0, "id", range(1, len(filtered) + 1))

    desired_columns = [
        "id", "city", "country", "weather_date", "date_time",
        "temperature", "feels_like", "humidity", "pressure",
        "wind_speed", "precipitation_1h", "weather_condition",
        "weather_description", "temperature_category"
    ]
    existing_cols = [c for c in desired_columns if c in filtered.columns]
    return filtered[existing_cols]
