import pandas as pd
import numpy as np
import sys
import json
import os
import boto3
from io import BytesIO
from scipy.stats import ks_2samp
from dotenv import load_dotenv

load_dotenv()

# AWS CONFIG
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

# ===============================
# S3 UTILITIES
# ===============================

def download_blob_as_bytes(bucket_name: str, key: str) -> bytes:
    buffer = BytesIO()
    s3.download_fileobj(bucket_name, key, buffer)
    buffer.seek(0)
    return buffer.read()

def upload_to_s3_from_bytes(data: bytes, bucket_name: str, key: str) -> str:
    s3.put_object(Bucket=bucket_name, Key=key, Body=data)
    return f"s3://{bucket_name}/{key}"

# ===============================
# DATA LOADER
# ===============================

def load_data(bucket_name: str, blob_name: str) -> pd.DataFrame:
    file_bytes = download_blob_as_bytes(bucket_name, blob_name)

    if blob_name.endswith(".csv"):
        return pd.read_csv(BytesIO(file_bytes))
    elif blob_name.endswith(".parquet"):
        return pd.read_parquet(BytesIO(file_bytes))
    else:
        raise ValueError("Unsupported file type. Use CSV or Parquet.")

# ===============================
# PROFILING
# ===============================

def profile_column(col: pd.Series) -> dict:
    profile = {
        "dtype": str(col.dtype),
        "null_count": int(col.isnull().sum()),
        "null_percentage": float(col.isnull().mean()) * 100,
        "unique_count": int(col.nunique()),
    }

    if pd.api.types.is_numeric_dtype(col):
        profile.update({
            "min": float(col.min(skipna=True)),
            "max": float(col.max(skipna=True)),
            "mean": float(col.mean(skipna=True)),
            "std": float(col.std(skipna=True)),
        })
    elif pd.api.types.is_string_dtype(col):
        lengths = col.dropna().astype(str).str.len()
        profile.update({
            "min_length": int(lengths.min()) if not lengths.empty else 0,
            "max_length": int(lengths.max()) if not lengths.empty else 0,
            "avg_length": float(lengths.mean()) if not lengths.empty else 0,
        })
        top_freq = col.value_counts().head(5).to_dict()
        profile["top_values"] = {str(k): int(v) for k, v in top_freq.items()}

    return profile

def profile_dataframe(df: pd.DataFrame) -> dict:
    overall_profile = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "duplicate_rows": int(df.duplicated().sum()),
        "memory_usage_mb": float(df.memory_usage(deep=True).sum() / 1024 ** 2),
        "columns": {}
    }

    for col in df.columns:
        overall_profile["columns"][col] = profile_column(df[col])

    return overall_profile

def upload_json_to_gcs(data: dict, bucket_name: str, blob_name: str) -> str:
    json_bytes = json.dumps(data).encode("utf-8")
    return upload_to_s3_from_bytes(json_bytes, bucket_name, blob_name)

# ===============================
# DRIFT
# ===============================

def calculate_psi(expected: pd.Series, actual: pd.Series, buckets: int = 10) -> float:
    def scale_bins(series):
        return np.histogram(series, bins=buckets)[0] / len(series)

    expected_percents = scale_bins(expected.dropna())
    actual_percents = scale_bins(actual.dropna())

    psi_value = np.sum([
        (e - a) * np.log((e + 1e-8) / (a + 1e-8))
        for e, a in zip(expected_percents, actual_percents)
    ])
    return psi_value

def detect_drift(baseline_df: pd.DataFrame, current_df: pd.DataFrame) -> dict:
    drift_results = {}
    common_cols = [col for col in baseline_df.columns if col in current_df.columns]

    for col in common_cols:
        baseline_col = baseline_df[col]
        current_col = current_df[col]

        if pd.api.types.is_numeric_dtype(current_col):
            psi_score = calculate_psi(baseline_col, current_col)
            ks_pvalue = ks_2samp(baseline_col.dropna(), current_col.dropna()).pvalue

            drift_results[col] = {
                "psi_score": round(psi_score, 4),
                "drift_by_psi": psi_score > 0.2,
                "ks_p_value": round(ks_pvalue, 4),
                "drift_by_ks": ks_pvalue < 0.05
            }

    return drift_results