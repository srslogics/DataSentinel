import pandas as pd
import io
from fastapi import HTTPException

def read_from_buffer(buffer: io.BytesIO, file_type: str) -> pd.DataFrame:
    try:
        if file_type == "csv":
            return pd.read_csv(buffer)
        elif file_type == "json":
            return pd.read_json(buffer)
        elif file_type == "excel":
            return pd.read_excel(buffer)
        elif file_type == "parquet":
            return pd.read_parquet(buffer)
        else:
            raise ValueError("Unsupported source format!")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Read error: {str(e)}")

def convert_to_buffer(df: pd.DataFrame, target_format: str) -> io.BytesIO:
    buffer = io.BytesIO()
    try:
        if target_format == "csv":
            df.to_csv(buffer, index=False)
        elif target_format == "json":
            df.to_json(buffer, orient="records", lines=True)
        elif target_format == "excel":
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False)
        elif target_format == "parquet":
            df.to_parquet(buffer, index=False, engine="pyarrow")  # or engine="fastparquet"
        else:
            raise ValueError("Unsupported target format!")
        buffer.seek(0)
        return buffer
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Conversion failed: {str(e)}")

def get_extension(file_type: str) -> str:
    return {"csv": "csv", "json": "json", "excel": "xlsx", "parquet": "parquet"}.get(file_type)