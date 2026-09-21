"""
Amazon S3 utility functions for the Weather Data Lake.
Handles S3 client creation, bucket verification, and object transfers
for Bronze (raw JSON), Silver (Parquet), and Gold (Parquet) layers.
"""

import io
import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

from config import (
    AWS_REGION,
    AWS_S3_BUCKET,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
)
from logger_config import logger


def get_s3_client():
    """
    Initializes and returns a boto3 S3 client using configured credentials
    or default AWS credential provider chain.
    """
    client_kwargs: Dict[str, Any] = {"region_name": AWS_REGION}

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        client_kwargs["aws_access_key_id"] = AWS_ACCESS_KEY_ID
        client_kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
        if AWS_SESSION_TOKEN:
            client_kwargs["aws_session_token"] = AWS_SESSION_TOKEN

    return boto3.client("s3", **client_kwargs)


def is_s3_available(bucket_name: Optional[str] = None) -> bool:
    """
    Checks if AWS credentials are valid and if S3 can be communicated with.
    Returns True if accessible, False otherwise.
    """
    bucket = bucket_name or AWS_S3_BUCKET
    try:
        s3 = get_s3_client()
        s3.head_bucket(Bucket=bucket)
        return True
    except (NoCredentialsError, PartialCredentialsError):
        logger.warning("AWS credentials not found. S3 operations will operate in local fallback mode.")
        return False
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchBucket"):
            logger.info(f"S3 bucket '{bucket}' does not exist yet.")
            return True
        elif error_code in ("403", "AccessDenied"):
            logger.warning(f"Access denied to S3 bucket '{bucket}'. Verify IAM permissions.")
            return False
        logger.error(f"S3 verification failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking S3 availability: {e}")
        return False


def ensure_bucket_exists(bucket_name: Optional[str] = None) -> bool:
    """
    Checks if the target S3 bucket exists, and creates it if it does not.
    Enforces private access by default.
    """
    bucket = bucket_name or AWS_S3_BUCKET
    s3 = get_s3_client()

    try:
        s3.head_bucket(Bucket=bucket)
        logger.info(f"S3 bucket verified: '{bucket}'")
        return True
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchBucket"):
            logger.info(f"Bucket '{bucket}' not found. Creating bucket in region '{AWS_REGION}'...")
            try:
                if AWS_REGION == "us-east-1":
                    s3.create_bucket(Bucket=bucket)
                else:
                    s3.create_bucket(
                        Bucket=bucket,
                        CreateBucketConfiguration={"LocationConstraint": AWS_REGION}
                    )
                logger.info(f"Successfully created private S3 bucket: '{bucket}'")
                return True
            except Exception as create_err:
                logger.error(f"Failed to create S3 bucket '{bucket}': {create_err}")
                return False
        else:
            logger.error(f"Cannot access bucket '{bucket}': {e}")
            return False
    except (NoCredentialsError, PartialCredentialsError):
        logger.warning("No AWS credentials provided. Skipping bucket creation.")
        return False


def upload_json_to_bronze(city: str, data: Dict[str, Any], date_str: str, bucket_name: Optional[str] = None) -> Optional[str]:
    """
    Uploads raw API weather JSON data to the S3 Bronze layer.
    Partitioned by year=YYYY/month=MM/day=DD/<city>.json
    """
    bucket = bucket_name or AWS_S3_BUCKET
    city_clean = city.lower().replace(" ", "_")

    parts = date_str.split("-")
    if len(parts) == 3:
        year_str, month_str, day_str = parts[0], parts[1], parts[2]
    else:
        year_str, month_str, day_str = "unknown", "unknown", "unknown"

    s3_key = f"bronze/weather/year={year_str}/month={month_str}/day={day_str}/{city_clean}.json"

    try:
        s3 = get_s3_client()
        json_bytes = json.dumps(data, indent=4).encode("utf-8")
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json_bytes,
            ContentType="application/json"
        )
        logger.info(f"Uploaded Bronze JSON to s3://{bucket}/{s3_key}")
        return s3_key
    except (NoCredentialsError, PartialCredentialsError):
        logger.warning(f"AWS credentials not configured. Skipped Bronze S3 upload for {city}.")
        return None
    except Exception as e:
        logger.error(f"Failed to upload {city} to S3 Bronze: {e}")
        return None


def sync_local_raw_to_bronze(raw_dir: Optional[Path] = None, bucket_name: Optional[str] = None) -> int:
    """
    Syncs local data/raw JSON files into S3 Bronze.
    Ensures all historical daily raw API responses exist in the cloud Data Lake.
    """
    from config import PROJECT_ROOT
    source_dir = raw_dir or (PROJECT_ROOT / "data" / "raw")
    bucket = bucket_name or AWS_S3_BUCKET
    uploaded_count = 0

    if not source_dir.exists():
        return 0

    for json_file in source_dir.glob("*/*.json"):
        date_folder = json_file.parent.name
        city_name = json_file.stem
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            uploaded_key = upload_json_to_bronze(city_name, data, date_folder, bucket)
            if uploaded_key:
                uploaded_count += 1
        except Exception as e:
            logger.error(f"Error syncing {json_file} to S3 Bronze: {e}")

    logger.info(f"Synced {uploaded_count} raw JSON records to S3 Bronze.")
    return uploaded_count


def upload_file(local_file_path: Path, s3_key: str, bucket_name: Optional[str] = None) -> bool:
    """
    Uploads a single local file to S3.
    """
    bucket = bucket_name or AWS_S3_BUCKET
    try:
        s3 = get_s3_client()
        s3.upload_file(str(local_file_path), bucket, s3_key)
        logger.info(f"Uploaded {local_file_path.name} to s3://{bucket}/{s3_key}")
        return True
    except Exception as e:
        logger.error(f"Failed uploading {local_file_path} to s3://{bucket}/{s3_key}: {e}")
        return False


def upload_directory(local_directory: Path, s3_prefix: str, bucket_name: Optional[str] = None) -> int:
    """
    Recursively uploads an entire directory (such as partitioned Parquet files)
    to the specified S3 prefix. Returns number of files uploaded.
    """
    bucket = bucket_name or AWS_S3_BUCKET
    uploaded_count = 0

    if not local_directory.exists():
        logger.warning(f"Directory {local_directory} does not exist for S3 upload.")
        return 0

    if not (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) and not os.path.exists(os.path.expanduser("~/.aws")):
        logger.warning(f"AWS credentials not configured. Skipped S3 upload for '{s3_prefix}' (data preserved in local Data Lake).")
        return 0

    s3 = get_s3_client()
    for file_path in local_directory.rglob("*"):
        if file_path.is_file() and not file_path.name.startswith("."):
            relative_path = file_path.relative_to(local_directory)
            s3_key = f"{s3_prefix.rstrip('/')}/{str(relative_path)}"

            try:
                s3.upload_file(str(file_path), bucket, s3_key)
                uploaded_count += 1
            except Exception as e:
                logger.error(f"Failed uploading {file_path} to s3://{bucket}/{s3_key}: {e}")

    logger.info(f"Uploaded {uploaded_count} files to s3://{bucket}/{s3_prefix}")
    return uploaded_count


def list_s3_objects(prefix: str, bucket_name: Optional[str] = None) -> List[str]:
    """
    Lists all object keys in the S3 bucket matching a given prefix.
    """
    bucket = bucket_name or AWS_S3_BUCKET
    s3 = get_s3_client()
    keys: List[str] = []

    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
    except (NoCredentialsError, PartialCredentialsError):
        logger.warning("AWS credentials not configured. Cannot list S3 objects.")
    except Exception as e:
        logger.error(f"Error listing objects under s3://{bucket}/{prefix}: {e}")

    return keys


def read_json_from_s3(s3_key: str, bucket_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Reads and parses a JSON object directly from S3.
    """
    bucket = bucket_name or AWS_S3_BUCKET
    try:
        s3 = get_s3_client()
        response = s3.get_object(Bucket=bucket, Key=s3_key)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
    except Exception as e:
        logger.error(f"Failed reading s3://{bucket}/{s3_key}: {e}")
        return None
