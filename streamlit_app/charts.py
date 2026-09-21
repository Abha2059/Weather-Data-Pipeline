import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Consistent color palette
CITY_COLORS = {
    "Delhi": "#FF6B6B",
    "Mumbai": "#4ECDC4",
    "Bangalore": "#45B7D1",
    "Bengaluru": "#45B7D1",
    "Kolkata": "#FFA07A",
    "Chennai": "#98D8C8"
}

CHART_TEMPLATE = "plotly_dark"


def apply_common_layout(fig, title: str, xaxis_title: str, yaxis_title: str):
    """
    Applies unified styling and layout standards across all dashboard charts.
    """
    fig.update_layout(
        template=CHART_TEMPLATE,
        title={
            "text": f"<b>{title}</b>",
            "x": 0.02,
            "xanchor": "left",
            "font": {"size": 16}
        },
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        legend_title_text="City",
        margin=dict(l=40, r=20, t=50, b=40),
        hovermode="x unified",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif")
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(255,255,255,0.08)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.08)")
    return fig


def create_temperature_trend_chart(df: pd.DataFrame):
    """
    Creates an interactive line chart tracking average temperature over time by city.
    """
    if df.empty or "avg_temp" not in df.columns:
        return None

    fig = px.line(
        df,
        x="weather_date",
        y="avg_temp",
        color="city",
        markers=True,
        color_discrete_map=CITY_COLORS,
        labels={"weather_date": "Observation Date", "avg_temp": "Avg Temperature (°C)", "city": "City"}
    )
    fig.update_traces(line=dict(width=2.5), marker=dict(size=7))
    return apply_common_layout(fig, "Daily Temperature Trend Over Time", "Observation Date", "Temperature (°C)")


def create_temperature_extremes_chart(df: pd.DataFrame):
    """
    Creates a grouped bar chart displaying Maximum, Average, and Minimum temperature by city.
    """
    if df.empty or "city" not in df.columns:
        return None

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["city"],
        y=df["max_temp"],
        name="Max Temp (°C)",
        marker_color="#FF4B4B",
        text=df["max_temp"],
        textposition="outside"
    ))
    fig.add_trace(go.Bar(
        x=df["city"],
        y=df["avg_temp"],
        name="Avg Temp (°C)",
        marker_color="#FFA726",
        text=df["avg_temp"],
        textposition="outside"
    ))
    fig.add_trace(go.Bar(
        x=df["city"],
        y=df["min_temp"],
        name="Min Temp (°C)",
        marker_color="#29B6F6",
        text=df["min_temp"],
        textposition="outside"
    ))

    fig.update_layout(barmode="group")
    return apply_common_layout(fig, "Temperature Extremes & Averages by City", "City", "Temperature (°C)")


def create_humidity_trend_chart(df: pd.DataFrame):
    """
    Creates an interactive line chart of daily average humidity trends by city.
    """
    if df.empty or "avg_humidity" not in df.columns:
        return None

    fig = px.line(
        df,
        x="weather_date",
        y="avg_humidity",
        color="city",
        markers=True,
        color_discrete_map=CITY_COLORS,
        labels={"weather_date": "Observation Date", "avg_humidity": "Humidity (%)", "city": "City"}
    )
    fig.update_traces(line=dict(width=2.5), marker=dict(size=7))
    return apply_common_layout(fig, "Daily Humidity Trend Over Time", "Observation Date", "Humidity (%)")


def create_city_humidity_chart(df: pd.DataFrame):
    """
    Creates a horizontal bar chart displaying average humidity by city.
    """
    if df.empty or "avg_humidity" not in df.columns:
        return None

    df_sorted = df.sort_values("avg_humidity", ascending=True)
    fig = px.bar(
        df_sorted,
        x="avg_humidity",
        y="city",
        orientation="h",
        color="avg_humidity",
        color_continuous_scale="Teal",
        labels={"avg_humidity": "Avg Humidity (%)", "city": "City"},
        text="avg_humidity"
    )
    fig.update_traces(texttemplate="%{text}%", textposition="outside")
    return apply_common_layout(fig, "Average Humidity by City", "Avg Humidity (%)", "City")


def create_wind_speed_chart(df: pd.DataFrame):
    """
    Creates a line chart illustrating daily wind speed over time by city.
    """
    if df.empty or "avg_wind_speed" not in df.columns:
        return None

    fig = px.line(
        df,
        x="weather_date",
        y="avg_wind_speed",
        color="city",
        markers=True,
        color_discrete_map=CITY_COLORS,
        labels={"weather_date": "Observation Date", "avg_wind_speed": "Wind Speed (m/s)", "city": "City"}
    )
    fig.update_traces(line=dict(width=2.5), marker=dict(size=7))
    return apply_common_layout(fig, "Daily Average Wind Speed Over Time", "Observation Date", "Wind Speed (m/s)")


def create_precipitation_chart(df: pd.DataFrame):
    """
    Creates a bar chart showing cumulative 1-hour recorded precipitation by city.
    """
    if df.empty or "total_precipitation" not in df.columns:
        return None

    fig = px.bar(
        df,
        x="city",
        y="total_precipitation",
        color="city",
        color_discrete_map=CITY_COLORS,
        labels={"city": "City", "total_precipitation": "Total Recorded Precip (mm)"},
        text="total_precipitation"
    )
    fig.update_traces(texttemplate="%{text} mm", textposition="outside")
    return apply_common_layout(fig, "Recorded 1-Hour Precipitation by City", "City", "Precipitation (mm)")


def create_monthly_trend_chart(df: pd.DataFrame):
    """
    Creates a monthly temperature progression chart grouped by Year-Month.
    """
    if df.empty or "year_month" not in df.columns:
        return None

    fig = px.line(
        df,
        x="year_month",
        y="avg_temp",
        color="city",
        markers=True,
        color_discrete_map=CITY_COLORS,
        labels={"year_month": "Period (YYYY-MM)", "avg_temp": "Avg Temperature (°C)", "city": "City"}
    )
    fig.update_traces(line=dict(width=2.5), marker=dict(size=8))
    return apply_common_layout(fig, "Monthly Average Temperature Progression (YYYY-MM)", "Period (YYYY-MM)", "Avg Temperature (°C)")


def create_condition_distribution_chart(df: pd.DataFrame):
    """
    Creates a donut chart representing the distribution of weather conditions.
    """
    if df.empty or "weather_condition" not in df.columns:
        return None

    counts = df["weather_condition"].value_counts().reset_index()
    counts.columns = ["weather_condition", "count"]

    fig = px.pie(
        counts,
        names="weather_condition",
        values="count",
        hole=0.45,
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    fig.update_layout(
        template=CHART_TEMPLATE,
        title={"text": "<b>Weather Conditions Distribution</b>", "x": 0.02, "font": {"size": 16}},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=50, b=20)
    )
    return fig
