"""
AWS S3 Silver Parquet Data Loader for Streamlit.
Streams partitioned Silver Parquet datasets directly from AWS S3 into memory,
caching with Streamlit for sub-second dashboard performance.
"""

import os
import io
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

logger = logging.getLogger("streamlit_s3_loader")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s in %(module)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Global in-memory cache to ensure sub-second response times across calls
_CACHE = {
    "df": None,
    "last_fetched": 0.0,
    "ttl_seconds": 300.0  # 5 minutes cache
}


def _get_secret(key: str, default: str = "") -> str:
    """Safely retrieves a configuration value from Streamlit secrets or environment variables."""
    try:
        import streamlit as st
        if hasattr(st, "secrets") and st.secrets:
            # 1. Direct top-level match
            if key in st.secrets:
                return str(st.secrets[key])
            # 2. Case-insensitive top-level match
            for sec_k in st.secrets:
                if sec_k.lower() == key.lower():
                    return str(st.secrets[sec_k])
            # 3. Search inside any nested table/section (e.g. [aws])
            for sec_name in st.secrets:
                try:
                    section = st.secrets[sec_name]
                    if hasattr(section, "items"):
                        for sub_k, sub_v in section.items():
                            clean_key = key.replace("AWS_", "").lower()
                            if sub_k.lower() in (key.lower(), clean_key):
                                return str(sub_v)
                except Exception:
                    continue
    except Exception:
        pass
    return os.getenv(key, default)


def _download_single_parquet(s3_client, bucket_name: str, key: str) -> pd.DataFrame | None:
    """Helper to download and parse a single parquet file in a worker thread."""
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        buffer = io.BytesIO(obj["Body"].read())
        return pd.read_parquet(buffer)
    except Exception as read_err:
        logger.warning(f"Failed to read parquet object {key}: {read_err}")
        return None


def _load_from_s3(bucket_name: str, region_name: str) -> pd.DataFrame:
    """Downloads all Silver Parquet partitions from AWS S3 in parallel into memory."""
    import boto3

    client_kwargs = {"region_name": region_name}
    aws_access_key = _get_secret("AWS_ACCESS_KEY_ID")
    aws_secret_key = _get_secret("AWS_SECRET_ACCESS_KEY")
    aws_session_token = _get_secret("AWS_SESSION_TOKEN")

    if aws_access_key and aws_secret_key:
        client_kwargs["aws_access_key_id"] = aws_access_key
        client_kwargs["aws_secret_access_key"] = aws_secret_key
        os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_key
    if aws_session_token:
        client_kwargs["aws_session_token"] = aws_session_token
        os.environ["AWS_SESSION_TOKEN"] = aws_session_token

    s3_client = boto3.client("s3", **client_kwargs)
    s3_prefix = "silver/weather/"

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix)

    parquet_keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet") and not key.endswith("_SUCCESS"):
                parquet_keys.append(key)

    if not parquet_keys:
        logger.warning(f"No parquet objects found in s3://{bucket_name}/{s3_prefix}")
        return pd.DataFrame()

    # Parallel download with ThreadPoolExecutor for high throughput
    dfs = []
    max_workers = min(16, max(4, len(parquet_keys)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {
            executor.submit(_download_single_parquet, s3_client, bucket_name, key): key
            for key in parquet_keys
        }
        for future in as_completed(future_to_key):
            res_df = future.result()
            if res_df is not None and not res_df.empty:
                dfs.append(res_df)

    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        # Ensure proper data types
        if "weather_date" in combined.columns:
            combined["weather_date"] = pd.to_datetime(combined["weather_date"]).dt.date
        if "temperature" in combined.columns:
            combined["temperature"] = pd.to_numeric(combined["temperature"], errors="coerce")
        if "humidity" in combined.columns:
            combined["humidity"] = pd.to_numeric(combined["humidity"], errors="coerce")
        if "wind_speed" in combined.columns:
            combined["wind_speed"] = pd.to_numeric(combined["wind_speed"], errors="coerce")
        if "pressure" in combined.columns:
            combined["pressure"] = pd.to_numeric(combined["pressure"], errors="coerce")
        if "precipitation_1h" in combined.columns:
            combined["precipitation_1h"] = pd.to_numeric(combined["precipitation_1h"], errors="coerce").fillna(0.0)

        # Deduplicate on (city, weather_date) keeping most recent
        sort_cols = [c for c in ["weather_date", "timestamp"] if c in combined.columns]
        if sort_cols:
            combined = combined.sort_values(sort_cols, ascending=False)
        if "city" in combined.columns and "weather_date" in combined.columns:
            combined = combined.drop_duplicates(subset=["city", "weather_date"], keep="first")

        logger.info(f"Loaded {len(combined)} rows from {len(parquet_keys)} S3 Silver Parquet partitions.")
        return combined

    return pd.DataFrame()


def _load_local_fallback() -> pd.DataFrame:
    """Fallback loader for local offline development when AWS S3 is unreachable."""
    # 1. Try local processed parquet
    local_parquet_dir = PROJECT_ROOT / "data/processed/weather_parquet"
    if local_parquet_dir.exists():
        try:
            df = pd.read_parquet(str(local_parquet_dir))
            if not df.empty:
                logger.info("Loaded weather observations from local Parquet directory.")
                return df
        except Exception as e:
            logger.warning(f"Failed to read local Parquet directory: {e}")

    # 2. Try cleaned_weather.json
    cleaned_json_path = PROJECT_ROOT / "data/processed/cleaned_weather.json"
    if cleaned_json_path.exists():
        try:
            with open(cleaned_json_path, "r") as f:
                data = json.load(f)
            if data:
                df = pd.DataFrame(data)
                if "weather_date" in df.columns:
                    df["weather_date"] = pd.to_datetime(df["weather_date"]).dt.date
                logger.info(f"Loaded {len(df)} records from {cleaned_json_path}")
                return df
        except Exception as e:
            logger.warning(f"Failed to read cleaned_weather.json: {e}")

    return pd.DataFrame()


def load_silver_weather_data(force_refresh: bool = False) -> pd.DataFrame:
    """
    Main entry point for loading the complete weather observation dataset.
    Prioritizes AWS S3 Silver Parquet, falling back to local storage if offline.
    Uses in-memory caching to guarantee sub-millisecond repeated queries.
    """
    global _CACHE
    now = time.time()

    if not force_refresh and _CACHE["df"] is not None and (now - _CACHE["last_fetched"]) < _CACHE["ttl_seconds"]:
        return _CACHE["df"].copy()

    bucket_name = _get_secret("AWS_S3_BUCKET", "weather-data-pipeline-abhay-699258776334")
    region_name = _get_secret("AWS_DEFAULT_REGION", _get_secret("AWS_REGION", "ap-south-1"))

    # Attempt S3 load first
    result_df = pd.DataFrame()
    if bucket_name:
        try:
            s3_df = _load_from_s3(bucket_name, region_name)
            if not s3_df.empty:
                result_df = s3_df
        except Exception as s3_err:
            logger.warning(f"S3 Silver Parquet loading encountered error: {s3_err}")

    # Fallback to local files if S3 returned empty
    if result_df.empty:
        logger.info("Falling back to local storage files...")
        result_df = _load_local_fallback()

    if not result_df.empty:
        _CACHE["df"] = result_df
        _CACHE["last_fetched"] = now

    return result_df.copy()


def get_s3_lakehouse_status() -> dict:
    """Returns metadata and health details about the S3 Lakehouse connection."""
    bucket_name = _get_secret("AWS_S3_BUCKET", "weather-data-pipeline-abhay-699258776334")
    region_name = _get_secret("AWS_DEFAULT_REGION", _get_secret("AWS_REGION", "ap-south-1"))
    has_key = bool(_get_secret("AWS_ACCESS_KEY_ID"))
    has_secret = bool(_get_secret("AWS_SECRET_ACCESS_KEY"))

    return {
        "bucket": bucket_name,
        "region": region_name,
        "authenticated": has_key and has_secret,
        "mode": "AWS S3 Silver & Gold Lakehouse (Serverless)",
    }
