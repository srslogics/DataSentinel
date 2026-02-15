from fastapi import (
    FastAPI, Request, UploadFile, File, Form
)
from fastapi.responses import (
    JSONResponse, HTMLResponse, RedirectResponse
)
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from dotenv import load_dotenv
from datetime import datetime
import os
import shutil
import uuid
import logging
import boto3
import requests
from pathlib import Path

from sqlalchemy.orm import Session
from sqlalchemy import func
from database import SessionLocal, engine
from models import (
    Base,
    User,
    ValidationResult,
    NormalizedFile,
    ConvertedFile,
    ProfileResult,
    PredictionResult
)
from stripe_utils import create_checkout_session





# ─────────────────────────────
# App Setup
# ─────────────────────────────

load_dotenv()
logging.basicConfig(level=logging.INFO)

Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="DataSentinel",
    root_path="/datasentinel",
    docs_url="/docs",
    openapi_url="/openapi.json"
)

app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ.get("SECRET_KEY", "change-me"),
    same_site="lax",
    https_only=True
)

# Static + Templates
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)
app.mount(
    "/static",
    StaticFiles(directory="static"),
    name="static"
)

templates = Jinja2Templates(directory="templates")

# ─────────────────────────────
# AWS
# ─────────────────────────────

s3 = boto3.client("s3")
BUCKET_NAME = os.environ.get("DATA_BUCKET", "datumsync-prod")
BACKEND_API_URL = os.environ.get("BACKEND_API_URL")

# ─────────────────────────────
# Helpers
# ─────────────────────────────

def require_user(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(
            request.url_for("login_page"),
            status_code=302
        )
    return user

def upload_to_s3(file: UploadFile, key: str):
    s3.upload_fileobj(file.file, BUCKET_NAME, key)

# ─────────────────────────────
# Health Check
# ─────────────────────────────

@app.api_route("/health", methods=["GET", "HEAD"])
def health():
    return {"status": "ok"}

# ─────────────────────────────
# Auth
# ─────────────────────────────

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse(
        "login.html",
        {"request": request}
    )

@app.post("/login")
async def login_submit(
    request: Request,
    email: str = Form(...)
):
    email = email.lower().strip()

    db: Session = SessionLocal()
    user = db.query(User).filter(User.email == email).first()

    if not user:
        user = User(
            email=email,
            name=email.split("@")[0],
            created_at=datetime.utcnow()
        )
        db.add(user)
        db.commit()

    db.close()

    request.session["user"] = {
        "email": user.email,
        "name": user.name
    }

    return RedirectResponse(
        "/datasentinel/dashboard",
        status_code=303
    )



@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(
        request.url_for("index"),
        status_code=303
    )

# ─────────────────────────────
# Pages
# ─────────────────────────────
history = []

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "user": request.session.get("user")
        }
    )

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user

    db = SessionLocal()
    stats = {
        "validation": db.query(func.count(ValidationResult.id))
            .filter_by(email=user["email"]).scalar(),
        "normalization": db.query(func.count(NormalizedFile.id))
            .filter_by(email=user["email"]).scalar(),
        "conversion": db.query(func.count(ConvertedFile.id))
            .filter_by(email=user["email"]).scalar(),
        "prediction": db.query(func.count(PredictionResult.id))
            .filter_by(email=user["email"]).scalar(),
    }
    db.close()

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": user,
            "stats": stats,
            "history": history,
            "now": datetime.utcnow()
        }
    )

# ─────────────────────────────
# Conversion
# ─────────────────────────────

@app.get("/convert", response_class=HTMLResponse)
async def convert_page(request: Request):
    user = require_user(request)

    db = SessionLocal()
    records = (
        db.query(ConvertedFile)
        .filter_by(email=user["email"])
        .order_by(ConvertedFile.created_at.desc())
        .all()
    )
    db.close()

    return templates.TemplateResponse(
        "convert.html",
        {
            "request": request,
            "user": user,
            "records": records
        }
    )


@app.post("/convert")
async def convert_submit(
    request: Request,
    file: UploadFile = File(...),
    target_format: str = Form(...)
):
    user = require_user(request)

    os.makedirs("uploads", exist_ok=True)

    uid = uuid.uuid4().hex
    stored_name = f"{uid}_{file.filename}"
    stored_path = f"uploads/{stored_name}"

    with open(stored_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    db = SessionLocal()
    db.add(
        ConvertedFile(
            email=user["email"],
            original_file=file.filename,
            converted_path=stored_path,
            format=target_format
        )
    )
    db.commit()
    db.close()

    return RedirectResponse(
        "/datasentinel/convert",
        status_code=303
    )

# ─────────────────────────────
# Validation
# ─────────────────────────────

@app.get("/validation", response_class=HTMLResponse)
async def validation_page(request: Request):
    user = require_user(request)

    db = SessionLocal()
    records = (
        db.query(ValidationResult)
        .filter_by(email=user["email"])
        .order_by(ValidationResult.created_at.desc())
        .all()
    )
    db.close()

    return templates.TemplateResponse(
        "validation.html",
        {
            "request": request,
            "user": user,
            "records": records
        }
    )
@app.post("/validation")
async def run_validation(
    request: Request,
    file: UploadFile = File(...)
):
    user = require_user(request)

    # Save file
    unique_name = f"{uuid.uuid4()}_{file.filename}"
    file_path = UPLOAD_DIR / unique_name

    with open(file_path, "wb") as f:
        f.write(await file.read())

    # Record result
    db = SessionLocal()
    record = ValidationResult(
        email=user["email"],
        input_file=file.filename,
        status="success"
    )

    db.add(record)
    db.commit()
    db.close()

    return RedirectResponse(
        "/datasentinel/validation",
        status_code=303
    )
# ─────────────────────────────
# Normalization
# ─────────────────────────────
@app.get("/normalization", response_class=HTMLResponse)
async def normalization_page(request: Request):
    user = require_user(request)

    db = SessionLocal()
    records = db.query(NormalizedFile)\
        .filter_by(email=user["email"])\
        .order_by(NormalizedFile.created_at.desc())\
        .all()
    db.close()

    return templates.TemplateResponse(
        "normalization.html",
        {
            "request": request,
            "user": user,
            "records": records
        }
    )
@app.post("/normalization")
async def run_normalization(
    request: Request,
    file: UploadFile = File(...)
):
    user = require_user(request)

    db = SessionLocal()
    record = NormalizedFile(
        email=user["email"],
        input_file=file.filename,
        normalized_file=f"normalized_{file.filename}",
    )
    db.add(record)
    db.commit()
    db.close()

    return RedirectResponse("/datasentinel/normalization", status_code=303)

# ─────────────────────────────
# Profiling
# ─────────────────────────────
@app.get("/profiling", response_class=HTMLResponse)
async def profiling_page(request: Request):
    user = require_user(request)

    db = SessionLocal()
    records = db.query(ProfileResult)\
        .filter_by(email=user["email"])\
        .order_by(ProfileResult.created_at.desc())\
        .all()
    db.close()

    return templates.TemplateResponse(
        "profiling.html",
        {
            "request": request,
            "user": user,
            "records": records
        }
    )
@app.post("/profiling")
async def run_profiling(
    request: Request,
    file: UploadFile = File(...)
):
    user = require_user(request)

    db = SessionLocal()
    record = ProfileResult(
        email=user["email"],
        input_file=file.filename,
        profile_url=f"/profiles/{file.filename}.html"
    )
    db.add(record)
    db.commit()
    db.close()

    return RedirectResponse("/datasentinel/profiling", status_code=303)

# ─────────────────────────────
# Prediction
# ─────────────────────────────
def require_pro(user):
    if not user.get("is_pro"):
        return False
    return True
@app.get("/prediction", response_class=HTMLResponse)
async def prediction_page(request: Request):
    user = require_user(request)

    if not require_pro(user):
        return RedirectResponse(
            "/datasentinel/prediction/locked",
            status_code=302
        )

    return templates.TemplateResponse(
        "prediction.html",
        {
            "request": request,
            "user": user
        }
    )
@app.get("/prediction/locked", response_class=HTMLResponse)
async def prediction_locked(request: Request):
    user = require_user(request)

    return templates.TemplateResponse(
        "prediction_locked.html",
        {
            "request": request,
            "user": user
        }
    )


# ─────────────────────────────
# Subscription
# ─────────────────────────────

@app.get("/subscribe/pro")
async def subscribe_pro(request: Request):
    user = require_user(request)
    if isinstance(user, RedirectResponse):
        return user

    return RedirectResponse(
        create_checkout_session(user["email"]),
        status_code=303
    )

# ─────────────────────────────
# Reports
# ─────────────────────────────

@app.get("/reports", response_class=HTMLResponse)
async def reports_page(request: Request):
    user = require_user(request)

    db = SessionLocal()
    history = []

    # Validation
    for r in db.query(ValidationResult).filter_by(email=user["email"]).all():
        history.append({
            "module": "Validation",
            "file": r.input_file,
            "status": r.status,
            "created_at": r.created_at,
            "view": f"/datasentinel/view/validation/{r.id}"
        })

    # Normalization
    for r in db.query(NormalizedFile).filter_by(email=user["email"]).all():
        history.append({
            "module": "Normalization",
            "file": r.input_file,
            "status": r.status,
            "created_at": r.created_at,
            "view": f"/datasentinel/view/normalization/{r.id}"
        })

    # Conversion
    for r in db.query(ConvertedFile).filter_by(email=user["email"]).all():
        history.append({
            "module": "Conversion",
            "file": r.original_file,
            "status": "success",
            "created_at": r.created_at,
            "view": f"/datasentinel/view/conversion/{r.id}"
        })

    # Profiling
    for r in db.query(ProfileResult).filter_by(email=user["email"]).all():
        history.append({
            "module": "Profiling",
            "file": r.input_file,
            "status": r.status,
            "created_at": r.created_at,
            "view": f"/datasentinel/view/profiling/{r.id}"
        })

        db.close()

    # Sort newest first
    history.sort(key=lambda x: x["created_at"], reverse=True)

    return templates.TemplateResponse(
        "reports.html",
        {
            "request": request,
            "user": user,
            "history": history
        }
    )
# ─────────────────────────────
# Views
# ─────────────────────────────
@app.get("/view/validation/{id}", response_class=HTMLResponse)
async def view_validation(request: Request, id: int):
    user = require_user(request)

    db = SessionLocal()
    r = db.query(ValidationResult)\
          .filter_by(id=id, email=user["email"])\
          .first()
    db.close()

    if not r:
        return RedirectResponse("/datasentinel/reports", status_code=302)

    return templates.TemplateResponse(
        "view_validation.html",
        {
            "request": request,
            "user": user,
            "record": r
        }
    )
@app.get("/view/profiling/{id}", response_class=HTMLResponse)
async def view_profiling(request: Request, id: int):
    user = require_user(request)

    db = SessionLocal()
    r = db.query(ProfileResult)\
          .filter_by(id=id, email=user["email"])\
          .first()
    db.close()

    if not r:
        return RedirectResponse("/datasentinel/reports", status_code=302)

    return templates.TemplateResponse(
        "view_profiling.html",
        {
            "request": request,
            "user": user,
            "record": r
        }
    )
@app.get("/view/{module}/{id}", response_class=HTMLResponse)
async def view_placeholder(request: Request, module: str, id: int):
    user = require_user(request)

    return templates.TemplateResponse(
        "view_placeholder.html",
        {
            "request": request,
            "user": user,
            "module": module.capitalize()
        }
    )
# ─────────────────────────────
# Settings
# ─────────────────────────────
@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    user = require_user(request)

    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "user": user
        }
    )
# ─────────────────────────────
# Logout
# ─────────────────────────────
@app.get("/logout")
async def logout(request: Request):
    request.session.clear()

    response = RedirectResponse("/datasentinel", status_code=302)
    response.headers["Cache-Control"] = "no-store"
    return response

