from sqlalchemy import Column, Integer, String, DateTime, Text
from datetime import datetime

from .database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    email = Column(String, unique=True, index=True)
    name = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


class ValidationResult(Base):
    __tablename__ = "validation_results"

    id = Column(Integer, primary_key=True)
    email = Column(String, index=True)
    input_file = Column(String)
    status = Column(String, default="success")
    result_path = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class NormalizedFile(Base):
    __tablename__ = "normalized_files"

    id = Column(Integer, primary_key=True)
    email = Column(String, index=True)
    input_file = Column(String)
    normalized_file = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


class ConvertedFile(Base):
    __tablename__ = "converted_files"

    id = Column(Integer, primary_key=True)
    email = Column(String, index=True)
    original_file = Column(String)
    converted_path = Column(String)
    format = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


class PredictionResult(Base):
    __tablename__ = "prediction_results"

    id = Column(Integer, primary_key=True)
    email = Column(String, index=True)
    input_file = Column(String)
    status = Column(String, default="success")
    created_at = Column(DateTime, default=datetime.utcnow)

class ProfileResult(Base):
    __tablename__ = "profile_results"

    id = Column(Integer, primary_key=True)
    email = Column(String, index=True)
    input_file = Column(String)
    profile_url = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
