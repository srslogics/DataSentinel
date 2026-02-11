import io
import numpy as np
import pandas as pd
import boto3
import os
from scipy.stats import normaltest
from scipy.stats.mstats import winsorize
from sklearn.preprocessing import LabelEncoder, MinMaxScaler, OneHotEncoder
import pyarrow.parquet as pq
import pyarrow as pa
import logging
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
# LOAD FILE FROM S3
# ===============================

def load_file_from_s3(s3_path):
    bucket_name, key = s3_path.replace("s3://", "").split("/", 1)

    buffer = io.BytesIO()
    s3.download_fileobj(bucket_name, key, buffer)
    buffer.seek(0)

    ext = key.split(".")[-1].lower()

    if ext == "csv":
        return pd.read_csv(buffer)
    elif ext in ["xlsx", "xls"]:
        return pd.read_excel(buffer)
    elif ext == "parquet":
        return pd.read_parquet(buffer)
    else:
        raise ValueError("Unsupported file type")

# ===============================
# OUTLIERS
# ===============================

def detect_outliers(df, method=None, threshold=1.5):
    outlier_percentages = {}
    numeric_cols = df.select_dtypes(include=[np.number]).columns

    if method is None:
        sample = df.sample(frac=0.1, random_state=42)
        normality_pvals = sample[numeric_cols].apply(
            lambda x: normaltest(x.dropna())[1] if x.dropna().shape[0] > 8 else np.nan
        ).dropna()

        method = "zscore" if not normality_pvals.empty and (normality_pvals > 0.05).all() else "iqr"

    for col in numeric_cols:
        if method == "iqr":
            Q1, Q3 = df[col].quantile([0.25, 0.75])
            IQR = Q3 - Q1
            lower, upper = Q1 - threshold * IQR, Q3 + threshold * IQR
            mask = (df[col] < lower) | (df[col] > upper)
        else:
            mean, std = df[col].mean(), df[col].std()
            mask = abs((df[col] - mean) / std) > threshold

        outlier_percentages[col] = (mask.sum() / len(df)) * 100
    return outlier_percentages

def clean_or_winsorize(df, outlier_percentages, threshold=5):
    for col, pct in outlier_percentages.items():
        if not np.issubdtype(df[col].dtype, np.number):
            continue

        Q1, Q3 = df[col].quantile([0.25, 0.75])
        IQR = Q3 - Q1
        lower, upper = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR

        if pct <= threshold:
            df[col] = df[col].where((df[col] >= lower) & (df[col] <= upper))
        else:
            df[col] = winsorize(df[col], limits=(0.05, 0.05))

    return df

# ===============================
# ENCODE + SCALE
# ===============================

def encode_categorical(df):
    cat_cols = df.select_dtypes(include=["object", "category"]).columns
    for col in cat_cols:
        unique_vals = df[col].nunique()
        if unique_vals <= 10:
            enc = OneHotEncoder(sparse_output=False, handle_unknown="ignore")
            transformed = enc.fit_transform(df[[col]])
            col_names = [f"{col}_{i}" for i in range(transformed.shape[1])]
            df = df.drop(columns=[col]).join(pd.DataFrame(transformed, columns=col_names, index=df.index))
        else:
            df[col] = LabelEncoder().fit_transform(df[col].astype(str))
    return df

def scale_numerical(df):
    num_cols = df.select_dtypes(include=["float64", "int64"]).columns
    if not num_cols.empty:
        scaler = MinMaxScaler()
        df[num_cols] = scaler.fit_transform(df[num_cols])
    return df

# ===============================
# SAVE TO S3
# ===============================

def save_parquet_to_s3(df, output_path):
    bucket_name, key = output_path.replace("s3://", "").split("/", 1)

    sink = pa.BufferOutputStream()
    table = pa.Table.from_pandas(df)

    with pq.ParquetWriter(sink, schema=table.schema, compression="snappy") as writer:
        writer.write_table(table)

    s3.put_object(Bucket=bucket_name, Key=key, Body=sink.getvalue().to_pybytes())
    logging.info(f"Saved normalized file: {output_path}")

# ===============================
# MAIN NORMALIZE
# ===============================

def normalize_file(s3_path):
    logging.info(f"Starting normalization: {s3_path}")
    df = load_file_from_s3(s3_path)

    df.dropna(axis=1, how="all", inplace=True)

    for col in df.columns:
        if df[col].dtype == "object":
            converted = pd.to_datetime(df[col], errors="coerce")
            if converted.notna().sum() > 0.5 * len(df):
                df[col] = converted

    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

    outliers = detect_outliers(df)
    df = clean_or_winsorize(df, outliers)

    df = encode_categorical(df)
    df = scale_numerical(df)

    output_path = s3_path.replace("raw/", "normalized/").rsplit(".", 1)[0] + "_normalized.parquet"
    save_parquet_to_s3(df, output_path)

    return output_path