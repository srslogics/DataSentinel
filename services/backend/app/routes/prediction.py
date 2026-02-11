import os
import pandas as pd
import tempfile
import boto3
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
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

def download_blob(bucket_name, key, local_path):
    s3.download_file(bucket_name, key, local_path)

def upload_blob(bucket_name, local_path, key):
    s3.upload_file(local_path, bucket_name, key)

def predict_from_parquet(bucket_name, scaled_blob_path, target_column):
    with tempfile.TemporaryDirectory() as tmpdir:

        # download parquet
        local_path = os.path.join(tmpdir, "scaled.parquet")
        download_blob(bucket_name, scaled_blob_path, local_path)

        df = pd.read_parquet(local_path)

        if target_column not in df.columns:
            raise ValueError(f"Target column '{target_column}' not found.")

        X = df.drop(columns=[target_column])
        y = df[target_column]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)

        predictions = model.predict(X)
        df["prediction"] = predictions

        predictions_parquet_path = os.path.join(tmpdir, "predictions.parquet")
        predictions_json_path = os.path.join(tmpdir, "predictions.json")

        df.to_parquet(predictions_parquet_path, index=False)
        df[["prediction"]].to_json(predictions_json_path, orient="records")

        user_prefix = os.path.dirname(scaled_blob_path)

        upload_blob(bucket_name, predictions_parquet_path, f"{user_prefix}/predictions.parquet")
        upload_blob(bucket_name, predictions_json_path, f"{user_prefix}/predictions.json")

        return {
            "message": "✅ Prediction completed",
            "target_used": target_column,
            "parquet": f"{user_prefix}/predictions.parquet",
            "json": f"{user_prefix}/predictions.json",
            "report": classification_report(y, predictions, output_dict=True)
        }