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


import io

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
            # 3. Search inside any nested table/section (e.g. [mysql], [aws])
            for sec_name in st.secrets:
                try:
                    section = st.secrets[sec_name]
                    if hasattr(section, "items"):
                        for sub_k, sub_v in section.items():
                            clean_key = key.replace("AWS_", "").replace("MYSQL_", "").lower()
                            if sub_k.lower() in (key.lower(), clean_key):
                                return str(sub_v)
                except Exception:
                    continue
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
    bucket_name = _get_secret("AWS_S3_BUCKET", "weather-data-pipeline-abhay-699258776334")
    if bucket_name:
        try:
            import boto3
            client_kwargs = {
                "region_name": _get_secret("AWS_DEFAULT_REGION", _get_secret("AWS_REGION", "ap-south-1"))
            }
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
            s3_prefix = f"gold/weather/{dataset_name}/"
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
            parquet_keys = [
                obj["Key"] for obj in response.get("Contents", [])
                if obj["Key"].endswith(".parquet") and not obj["Key"].endswith("_SUCCESS")
            ]

            dfs = []
            for key in parquet_keys:
                obj = s3_client.get_object(Bucket=bucket_name, Key=key)
                buffer = io.BytesIO(obj["Body"].read())
                part_df = pd.read_parquet(buffer)
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


def get_gold_diagnostics() -> dict:
    """Returns diagnostic information regarding AWS S3 and credentials state."""
    bucket = _get_secret("AWS_S3_BUCKET", "weather-data-pipeline-abhay-699258776334")
    has_key = bool(_get_secret("AWS_ACCESS_KEY_ID"))
    has_secret = bool(_get_secret("AWS_SECRET_ACCESS_KEY"))
    region = _get_secret("AWS_DEFAULT_REGION", _get_secret("AWS_REGION", "ap-south-1"))
    return {
        "bucket": bucket,
        "has_access_key": has_key,
        "has_secret_key": has_secret,
        "region": region,
    }

