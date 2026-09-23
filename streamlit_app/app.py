import os
import sys
from pathlib import Path
from datetime import datetime
import streamlit as st
import pandas as pd

# Ensure project root and streamlit_app directory are importable
APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent
for path_entry in [str(PROJECT_ROOT), str(APP_DIR)]:
    if path_entry not in sys.path:
        sys.path.insert(0, path_entry)

from s3_data_loader import get_s3_lakehouse_status, load_silver_weather_data
import queries
import charts
import gold_lakehouse

# Configure page settings
st.set_page_config(
    page_title="Weather Data Analytics Dashboard",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for polished aesthetics
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        color: #A0AEC0;
        font-size: 1.05rem;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background-color: rgba(255, 255, 255, 0.04);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 10px;
        padding: 16px;
        text-align: center;
    }
    .source-badge {
        display: inline-block;
        padding: 4px 10px;
        font-size: 0.8rem;
        font-weight: 600;
        border-radius: 6px;
        background: #1E3A8A;
        color: #93C5FD;
        margin-bottom: 12px;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 8px 16px;
        border-radius: 6px;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=300)
def cached_filter_options():
    cities = queries.get_available_cities()
    min_date, max_date = queries.get_date_bounds()
    conditions = queries.get_available_conditions()
    return cities, min_date, max_date, conditions


@st.cache_data(ttl=300)
def cached_dashboard_data(selected_cities, start_date, end_date, selected_condition):
    kpis = queries.get_kpi_summary(selected_cities, start_date, end_date, selected_condition)
    daily_trends = queries.get_daily_trends(selected_cities, start_date, end_date, selected_condition)
    city_comparison = queries.get_city_comparison(selected_cities, start_date, end_date, selected_condition)
    records_df = queries.get_filtered_records(selected_cities, start_date, end_date, selected_condition, limit=500)
    return kpis, daily_trends, city_comparison, records_df


def main():
    # Inspect AWS S3 Data Lake connection
    lakehouse_status = get_s3_lakehouse_status()
    bucket_display = lakehouse_status["bucket"]
    if len(bucket_display) > 28:
        bucket_display = bucket_display[:20] + "..." + bucket_display[-8:]

    # Sidebar: Title & Status
    st.sidebar.markdown("### 🌤️ Weather Dashboard")
    st.sidebar.success(f"AWS S3 Data Lake\n\n`{bucket_display}`\n\nRegion: `{lakehouse_status['region']}`", icon="🟢")

    # Fetch available filter options
    available_cities, min_date, max_date, available_conditions = cached_filter_options()

    if not available_cities:
        st.warning("No weather records found in AWS S3 or local data lake. Please run the ETL pipeline first to populate data.")
        return

    st.sidebar.markdown("---")
    st.sidebar.markdown("#### 🔍 Filter Controls")

    # City filter
    select_all_cities = st.sidebar.checkbox("Select All Cities", value=True)
    if select_all_cities:
        selected_cities = available_cities
        st.sidebar.multiselect("Active Cities:", available_cities, default=available_cities, disabled=True)
    else:
        selected_cities = st.sidebar.multiselect("Choose Cities:", available_cities, default=available_cities[:2])

    if not selected_cities:
        st.warning("Please select at least one city in the sidebar filters.")
        return

    # Date range filter
    if min_date and max_date:
        date_range = st.sidebar.date_input(
            "Observation Date Range:",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            start_date, end_date = date_range
        elif isinstance(date_range, (list, tuple)) and len(date_range) == 1:
            start_date = end_date = date_range[0]
        else:
            start_date, end_date = min_date, max_date
    else:
        start_date, end_date = None, None

    # Weather condition filter
    selected_condition = st.sidebar.selectbox("Weather Condition:", available_conditions, index=0)

    # Refresh Button
    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        load_silver_weather_data(force_refresh=True)
        st.rerun()

    st.sidebar.caption(f"Storage Layer: `AWS S3 Parquet Lakehouse`")
    st.sidebar.caption(f"Bucket: `{lakehouse_status['bucket']}`")

    # Header section
    st.markdown('<div class="main-header">Weather Data Analytics Dashboard</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Interactive monitoring and historical analytics powered by AWS S3 Data Lake (Silver & Gold Parquet).</div>',
        unsafe_allow_html=True
    )

    # Fetch filtered data
    kpis, daily_trends, city_comp, records_df = cached_dashboard_data(
        selected_cities, start_date, end_date, selected_condition
    )

    # Check for empty data
    if kpis["record_count"] == 0:
        st.info("ℹ️ No weather observations match the current filter criteria. Try expanding your date range or city selection.")
        return

    # KPI Metrics Row
    kpi_col1, kpi_col2, kpi_col3, kpi_col4, kpi_col5, kpi_col6 = st.columns(6)
    with kpi_col1:
        st.metric(label="🌡️ Avg Temperature", value=f"{kpis['avg_temp']:.2f} °C")
    with kpi_col2:
        st.metric(label="🔥 Max Temperature", value=f"{kpis['max_temp']:.2f} °C")
    with kpi_col3:
        st.metric(label="❄️ Min Temperature", value=f"{kpis['min_temp']:.2f} °C")
    with kpi_col4:
        st.metric(label="💧 Avg Humidity", value=f"{kpis['avg_humidity']:.1f} %")
    with kpi_col5:
        st.metric(label="💨 Avg Wind Speed", value=f"{kpis['avg_wind_speed']:.2f} m/s")
    with kpi_col6:
        st.metric(label="🌧️ Total Precip (1h)", value=f"{kpis['total_precipitation']:.2f} mm")

    st.markdown("---")

    # Tabbed views
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "🌡️ Temperature Analytics",
        "💧 Humidity & Wind",
        "🌧️ Precipitation",
        "🏙️ City Comparison",
        "📅 Monthly Trends",
        "🏛️ S3 Gold Lakehouse",
        "📋 Detailed Records"
    ])

    # Tab 1: Temperature Analytics
    with tab1:
        col_t1, col_t2 = st.columns([2, 1])
        with col_t1:
            fig_temp_trend = charts.create_temperature_trend_chart(daily_trends)
            if fig_temp_trend:
                st.plotly_chart(fig_temp_trend, use_container_width=True)
            else:
                st.info("No temperature trend data available.")
        with col_t2:
            fig_conditions = charts.create_condition_distribution_chart(records_df)
            if fig_conditions:
                st.plotly_chart(fig_conditions, use_container_width=True)

        fig_extremes = charts.create_temperature_extremes_chart(city_comp)
        if fig_extremes:
            st.plotly_chart(fig_extremes, use_container_width=True)

    # Tab 2: Humidity & Wind Dynamics
    with tab2:
        col_h1, col_h2 = st.columns(2)
        with col_h1:
            fig_humidity = charts.create_humidity_trend_chart(daily_trends)
            if fig_humidity:
                st.plotly_chart(fig_humidity, use_container_width=True)
            fig_city_hum = charts.create_city_humidity_chart(city_comp)
            if fig_city_hum:
                st.plotly_chart(fig_city_hum, use_container_width=True)

        with col_h2:
            fig_wind = charts.create_wind_speed_chart(daily_trends)
            if fig_wind:
                st.plotly_chart(fig_wind, use_container_width=True)

    # Tab 3: Precipitation
    with tab3:
        st.markdown("##### Recorded 1-Hour Precipitation Breakdown")
        st.caption("ℹ️ Note: Precipitation data represents the 1-hour recorded rainfall (`precipitation_1h` in mm) from API observation snapshots.")
        col_p1, col_p2 = st.columns([2, 1])
        with col_p1:
            fig_precip = charts.create_precipitation_chart(city_comp)
            if fig_precip:
                st.plotly_chart(fig_precip, use_container_width=True)
        with col_p2:
            precip_table = city_comp[["city", "total_precipitation", "total_records"]].copy()
            precip_table.columns = ["City", "Total Recorded (mm)", "Observations"]
            st.dataframe(precip_table, use_container_width=True, hide_index=True)

    # Tab 4: City Comparison Matrix
    with tab4:
        st.markdown("##### Comparative Weather Summary by City")
        st.dataframe(
            city_comp.rename(columns={
                "city": "City",
                "country": "Country",
                "total_records": "Observations",
                "avg_temp": "Avg Temp (°C)",
                "max_temp": "Max Temp (°C)",
                "min_temp": "Min Temp (°C)",
                "avg_humidity": "Avg Humidity (%)",
                "avg_pressure": "Avg Pressure (hPa)",
                "avg_wind_speed": "Avg Wind (m/s)",
                "total_precipitation": "Total Precip (mm)"
            }),
            use_container_width=True,
            hide_index=True
        )

    # Tab 5: Monthly Trends
    with tab5:
        monthly_df = queries.get_monthly_trends(selected_cities, selected_condition)
        if not monthly_df.empty:
            fig_monthly = charts.create_monthly_trend_chart(monthly_df)
            if fig_monthly:
                st.plotly_chart(fig_monthly, use_container_width=True)
            st.dataframe(
                monthly_df.rename(columns={
                    "year_month": "Period (YYYY-MM)",
                    "city": "City",
                    "avg_temp": "Avg Temp (°C)",
                    "max_temp": "Max Temp (°C)",
                    "min_temp": "Min Temp (°C)",
                    "avg_humidity": "Avg Humidity (%)",
                    "avg_wind_speed": "Avg Wind (m/s)",
                    "record_count": "Records"
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Insufficient multi-month historical data for monthly trend analysis.")

    # Tab 6: S3 Gold Lakehouse Benchmarks
    with tab6:
        st.markdown("##### AWS S3 Gold Data Lake Analytical Datasets")
        st.markdown("""
        These tables represent pre-aggregated Parquet datasets computed by PySpark during the Lakehouse ETL pipeline and stored in AWS S3 (`s3://.../gold/weather/`).
        """)

        gold_datasets = gold_lakehouse.get_all_gold_summaries()
        if gold_datasets:
            gold_choice = st.selectbox("Select Gold Analytical Dataset to Inspect:", list(gold_datasets.keys()))
            selected_gold_df = gold_datasets[gold_choice]
            st.dataframe(selected_gold_df, use_container_width=True, hide_index=True)
            st.caption(f"Loaded {len(selected_gold_df)} rows from Gold dataset: `{gold_choice}` (Parquet format)")
        else:
            diag = gold_lakehouse.get_gold_diagnostics()
            if not diag["has_access_key"] or not diag["has_secret_key"]:
                st.info("AWS credentials not detected in Streamlit Secrets. Please ensure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are saved in Settings > Secrets and reboot the app.")
            else:
                st.info(f"Targeting S3 bucket: `{diag['bucket']}` ({diag['region']}). If recently updated, please reboot the app from the bottom-right menu.")


    # Tab 7: Detailed Records Viewer
    with tab7:
        st.markdown("##### Filtered Weather Observations")
        st.caption(f"Showing up to 500 most recent records matching active filters ({len(records_df)} records loaded).")

        display_cols = [
            "city", "country", "weather_date", "date_time",
            "temperature", "feels_like", "humidity", "pressure",
            "wind_speed", "precipitation_1h", "weather_condition", "weather_description"
        ]
        available_display_cols = [c for c in display_cols if c in records_df.columns]

        st.dataframe(
            records_df[available_display_cols].rename(columns={
                "city": "City", "country": "Country", "weather_date": "Date",
                "date_time": "Timestamp", "temperature": "Temp (°C)",
                "feels_like": "Feels Like (°C)", "humidity": "Humidity (%)",
                "pressure": "Pressure (hPa)", "wind_speed": "Wind (m/s)",
                "precipitation_1h": "Precip (mm)", "weather_condition": "Condition",
                "weather_description": "Description"
            }),
            use_container_width=True,
            hide_index=True
        )

        csv_data = records_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download Filtered Records as CSV",
            data=csv_data,
            file_name=f"weather_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

    # Footer
    st.markdown("---")
    st.caption(f"Data Sources: **AWS S3 Silver Parquet** & **AWS S3 Gold Parquet** | Total Records Filtered: `{kpis['record_count']}` | Last Refreshed: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`")


if __name__ == "__main__":
    main()
