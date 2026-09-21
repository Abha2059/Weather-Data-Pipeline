import os
import logging
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

logger = logging.getLogger("streamlit_gold_lakehouse")
LOCAL_GOLD_DIR = PROJECT_ROOT / "data/processed/gold_weather"

GOLD_DATASET_NAMES = [
    "city_temperature",
    "monthly_temperature",
    "city_weather_summary",
    "humidity_trends",
    "wind_trends",
    "rainfall_trends"
]


def _get_secret(key: str, default: str = "") -> str:
    """Safely retrieves a configuration value from Streamlit secrets or environment variables."""
    try:
        import streamlit as st
        if hasattr(st, "secrets") and key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass
    return os.getenv(key, default)


def load_gold_dataset(dataset_name: str) -> pd.DataFrame:
    """
    Loads a pre-aggregated PySpark Gold Parquet dataset.
    Prioritizes local cached Parquet files, falling back to AWS S3 if needed.
    """
    if dataset_name not in GOLD_DATASET_NAMES:
        raise ValueError(f"Unknown Gold dataset: {dataset_name}. Expected one of {GOLD_DATASET_NAMES}")

    dataset_path = LOCAL_GOLD_DIR / dataset_name

    # 1. Check local Parquet directory
    if dataset_path.exists():
        try:
            df = pd.read_parquet(str(dataset_path))
            return df
        except Exception as err:
            logger.warning(f"Error reading local Gold dataset {dataset_name}: {err}")

    # 2. Attempt reading from AWS S3 via boto3 if configured
    bucket_name = _get_secret("AWS_S3_BUCKET")
    if bucket_name:
        try:
            import boto3
            client_kwargs = {
                "region_name": _get_secret("AWS_DEFAULT_REGION", _get_secret("AWS_REGION", "ap-south-1"))
            }
            aws_access_key = _get_secret("AWS_ACCESS_KEY_ID")
            aws_secret_key = _get_secret("AWS_SECRET_ACCESS_KEY")
            if aws_access_key and aws_secret_key:
                client_kwargs["aws_access_key_id"] = aws_access_key
                client_kwargs["aws_secret_access_key"] = aws_secret_key

            s3_client = boto3.client("s3", **client_kwargs)
            s3_prefix = f"gold/weather/{dataset_name}/"
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
            parquet_keys = [
                obj["Key"] for obj in response.get("Contents", [])
                if obj["Key"].endswith(".parquet") and not obj["Key"].endswith("_SUCCESS")
            ]

            dfs = []
            for key in parquet_keys:
                s3_uri = f"s3://{bucket_name}/{key}"
                part_df = pd.read_parquet(s3_uri)
                dfs.append(part_df)

            if dfs:
                return pd.concat(dfs, ignore_index=True)
        except Exception as s3_err:
            logger.warning(f"Unable to read Gold dataset {dataset_name} directly from S3: {s3_err}")

    return pd.DataFrame()


def get_all_gold_summaries() -> dict[str, pd.DataFrame]:
    """
    Returns a dictionary of all available Gold analytical datasets.
    """
    results = {}
    for name in GOLD_DATASET_NAMES:
        df = load_gold_dataset(name)
        if not df.empty:
            results[name] = df
    return results
