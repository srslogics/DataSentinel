import os
import io
import json
import logging
import tempfile
from typing import Optional

import pandas as pd
import boto3
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

# ✅ Load env
load_dotenv()

# ✅ AWS CONFIG
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("S3_BUCKET")
AWS_BACKEND = "http://datasentinel-alb-84389164.ap-south-1.elb.amazonaws.com"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name="ap-south-1"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://datasentinel.srslogics.com"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_PREFIX = "converted/"
SUPPORTED_EXTENSIONS = [".csv", ".parquet", ".xlsx"]
SUPPORTED_FORMATS = ('.csv', '.json', '.xlsx', '.parquet')

# ✅ Module Imports
from app.routes.conversion import read_from_buffer, convert_to_buffer, get_extension
from app.routes.profiling import load_data, profile_dataframe, detect_drift, upload_json_to_gcs
from app.routes.normalization import normalize_file
from app.routes.validation import validate
from app.routes.prediction import predict_from_parquet, download_blob

# ✅ FastAPI init
app = FastAPI()

# ===============================
# 🔥 S3 UTILS
# ===============================

def download_from_s3(bucket, key) -> io.BytesIO:
    buffer = io.BytesIO()
    try:
        s3.download_fileobj(bucket, key, buffer)
        buffer.seek(0)
        return buffer
    except Exception:
        print("❌ S3 ERROR")
        print("BUCKET:", bucket)
        print("KEY:", key)
        raise HTTPException(status_code=404, detail=f"File not found in S3: {key}")


def upload_to_s3(bucket, key, buffer: io.BytesIO):
    buffer.seek(0)
    s3.upload_fileobj(buffer, bucket, key)


def file_exists_s3(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False


# ===============================
# HEALTH
# ===============================

@app.get("/")
def root():
    return {"status": "ok", "message": "💚 AWS Service is healthy"}


# ===============================
# CONVERSION
# ===============================

@app.post("/convert-and-upload")
def convert_and_upload(
    filename: str = Query(...),
    source_format: str = Query(..., pattern="^(csv|json|excel|parquet)$"),
    target_format: str = Query(..., pattern="^(csv|json|excel|parquet)$")
):
    source_buffer = download_from_s3(BUCKET_NAME, filename)
    df = read_from_buffer(source_buffer, source_format)
    converted_buffer = convert_to_buffer(df, target_format)

    base_name = filename.split("/")[-1].rsplit(".", 1)[0]
    converted_filename = f"{UPLOAD_PREFIX}{base_name}_converted.{get_extension(target_format)}"
    upload_to_s3(BUCKET_NAME, converted_filename, converted_buffer)

    return {
        "message": "✅ Conversion successful",
        "converted_file_path": f"s3://{BUCKET_NAME}/{converted_filename}"
    }


# ===============================
# PROFILING
# ===============================

class ProfileRequest(BaseModel):
    bucket_name: str
    current_blob: str
    baseline_blob: Optional[str] = None


@app.post("/profile")
def generate_profile(request: ProfileRequest):
    bucket = request.bucket_name
    current_blob = request.current_blob
    baseline_blob = request.baseline_blob

    current_df = load_data(bucket, current_blob)
    profile_result = profile_dataframe(current_df)

    profile_blob = f"profiling/{os.path.splitext(os.path.basename(current_blob))[0]}_profile.json"
    profile_url = upload_json_to_gcs(profile_result, bucket, profile_blob)

    result = {"profile_url": profile_url}

    if baseline_blob:
        baseline_df = load_data(bucket, baseline_blob)
        drift_result = detect_drift(baseline_df, current_df)
        drift_blob = f"profiling/{os.path.splitext(os.path.basename(current_blob))[0]}_drift.json"
        drift_url = upload_json_to_gcs(drift_result, bucket, drift_blob)
        result["drift_url"] = drift_url

    return result


# ===============================
# NORMALIZATION
# ===============================

@app.post("/normalize")
async def normalize_handler(request: Request):
    data = await request.json()
    name = data.get("name")
    bucket = data.get("bucket")

    if not name or not bucket:
        return JSONResponse(content={"error": "Missing 'name' or 'bucket'"}, status_code=400)

    if not any(name.endswith(ext) for ext in SUPPORTED_EXTENSIONS):
        return JSONResponse(content={"message": f"Ignored unsupported file: {name}"}, status_code=200)

    if not file_exists_s3(bucket, name):
        return JSONResponse(content={"error": f"File not found: {name}"}, status_code=404)

    gcs_path = f"s3://{bucket}/{name}"
    output_path = normalize_file(gcs_path)

    return {"message": "✅ Normalization complete", "output_path": output_path}


# ===============================
# VALIDATION
# ===============================

@app.post("/validate")
async def validate_file(request: Request):
    data = await request.json()
    bucket_name = data.get("bucket")
    file_name = data.get("name")

    if not bucket_name or not file_name:
        raise HTTPException(status_code=400, detail="Missing 'bucket' or 'name'")

    if not file_name.endswith(SUPPORTED_FORMATS):
        raise HTTPException(status_code=400, detail="Unsupported file format")

    tmp_path = f"/tmp/{os.path.basename(file_name)}"
    s3.download_file(bucket_name, file_name, tmp_path)

    if file_name.endswith(".csv"):
        df = pd.read_csv(tmp_path)
    elif file_name.endswith(".json"):
        df = pd.read_json(tmp_path)
    elif file_name.endswith(".xlsx"):
        df = pd.read_excel(tmp_path)
    elif file_name.endswith(".parquet"):
        df = pd.read_parquet(tmp_path)

    with open("validation_rules.json") as f:
        rules = json.load(f)

    result = validate(df, rules)

    s3.put_object(
        Bucket=bucket_name,
        Key=f"validation-results/{file_name}.results.json",
        Body=json.dumps(result, indent=2),
        ContentType="application/json"
    )

    return JSONResponse(content={"status": "success", "file": file_name})


# ===============================
# PREDICTION COLUMNS
# ===============================

@app.post("/columns")
async def get_columns(request: Request):
    data = await request.json()
    bucket_name = data.get("bucket_name")
    scaled_blob_path = data.get("scaled_blob_path")

    if not bucket_name or not scaled_blob_path:
        return JSONResponse(status_code=400, content={"error": "Missing fields"})

    if not file_exists_s3(bucket_name, scaled_blob_path):
        return JSONResponse(status_code=404, content={"error": "File not found"})

    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "temp.parquet")
        s3.download_file(bucket_name, scaled_blob_path, file_path)
        df = pd.read_parquet(file_path)
        return {"columns": df.columns.tolist()}


# ===============================
# PREDICT
# ===============================

@app.post("/predict")
async def predict(request: Request):
    data = await request.json()
    bucket_name = data.get("bucket_name")
    scaled_blob_path = data.get("scaled_blob_path")
    target_column = data.get("target_column")

    if not bucket_name or not scaled_blob_path or not target_column:
        return JSONResponse(status_code=400, content={"error": "Missing required fields"})

    if not file_exists_s3(bucket_name, scaled_blob_path):
        return JSONResponse(status_code=404, content={"error": "File not found"})

    result = predict_from_parquet(bucket_name, scaled_blob_path, target_column)
    return JSONResponse(content=result)


# ===============================
# LOCAL RUN
# ===============================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)