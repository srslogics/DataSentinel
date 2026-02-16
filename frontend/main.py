from fastapi import (
    FastAPI, Request, UploadFile, File, Form, HTTPException
)
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from sqlalchemy.orm import Session
from sqlalchemy import func

from pathlib import Path
from datetime import datetime
import uuid
import os
import shutil
import logging

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

logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="DataSentinel"
)

app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ.get("SECRET_KEY", "dev-secret"),
    same_site="lax",
    https_only=False  # REQUIRED for Render
)

BASE_DIR = Path(__file__).parent

templates = Jinja2Templates(directory=BASE_DIR / "templates")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

# ─────────────────────────────
# Database init (MVP only)
# ─────────────────────────────

Base.metadata.create_all(bind=engine)

# ─────────────────────────────
# Upload directories
# ─────────────────────────────

UPLOAD_ROOT = BASE_DIR / "uploads"
VALIDATION_DIR = UPLOAD_ROOT / "validation"
NORMALIZATION_DIR = UPLOAD_ROOT / "normalization"
CONVERSION_DIR = UPLOAD_ROOT / "conversion"
PROFILING_DIR = UPLOAD_ROOT / "profiling"

for d in [
    UPLOAD_ROOT,
    VALIDATION_DIR,
    NORMALIZATION_DIR,
    CONVERSION_DIR,
    PROFILING_DIR,
]:
    d.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────
# Auth helpers
# ─────────────────────────────

def require_user(request: Request) -> dict:
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401)
    return user

def require_pro(user: dict) -> bool:
    return bool(user.get("is_pro"))

# ─────────────────────────────
# Health
# ─────────────────────────────

@app.get("/health")
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
        db.refresh(user)

    session_user = {
        "email": user.email,
        "name": user.name,
        "is_pro": False  # wired later via Stripe
    }

    db.close()

    request.session["user"] = session_user

    return RedirectResponse(
        "/datasentinel/dashboard",
        status_code=303
    )

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    response = RedirectResponse("/datasentinel", status_code=302)
    response.headers["Cache-Control"] = "no-store"
    return response

# ─────────────────────────────
# Pages
# ─────────────────────────────

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

    db = SessionLocal()
    stats = {
        "validation": db.query(func.count(ValidationResult.id))
            .filter_by(email=user["email"]).scalar(),
        "normalization": db.query(func.count(NormalizedFile.id))
            .filter_by(email=user["email"]).scalar(),
        "conversion": db.query(func.count(ConvertedFile.id))
            .filter_by(email=user["email"]).scalar(),
    }
    db.close()

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": user,
            "stats": stats,
            "now": datetime.utcnow()
        }
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

    if not file.filename:
        return RedirectResponse("/datasentinel/validation", status_code=303)

    uid = uuid.uuid4().hex
    path = VALIDATION_DIR / f"{uid}_{file.filename}"

    with open(path, "wb") as f:
        f.write(await file.read())

    db = SessionLocal()
    record = ValidationResult(
        email=user["email"],
        input_file=file.filename,
        status="success"
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    db.close()

    return RedirectResponse("/datasentinel/validation", status_code=303)

# ─────────────────────────────
# Normalization
# ─────────────────────────────

@app.get("/normalization", response_class=HTMLResponse)
async def normalization_page(request: Request):
    user = require_user(request)

    db = SessionLocal()
    records = (
        db.query(NormalizedFile)
        .filter_by(email=user["email"])
        .order_by(NormalizedFile.created_at.desc())
        .all()
    )
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

    if not file.filename:
        return RedirectResponse("/datasentinel/normalization", status_code=303)

    uid = uuid.uuid4().hex
    input_path = NORMALIZATION_DIR / f"{uid}_{file.filename}"
    output_path = NORMALIZATION_DIR / f"{uid}_normalized_{file.filename}"

    with open(input_path, "wb") as f:
        f.write(await file.read())

    shutil.copyfile(input_path, output_path)

    db = SessionLocal()
    record = NormalizedFile(
        email=user["email"],
        input_file=file.filename,
        normalized_file=str(output_path)
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    db.close()

    return RedirectResponse("/datasentinel/normalization", status_code=303)

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

    if not file.filename:
        return RedirectResponse("/datasentinel/convert", status_code=303)

    uid = uuid.uuid4().hex
    path = CONVERSION_DIR / f"{uid}_{file.filename}"

    with open(path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    db = SessionLocal()
    record = ConvertedFile(
        email=user["email"],
        original_file=file.filename,
        converted_path=str(path),
        format=target_format
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    db.close()

    return RedirectResponse("/datasentinel/convert", status_code=303)

# ─────────────────────────────
# Profiling
# ─────────────────────────────

@app.get("/profiling", response_class=HTMLResponse)
async def profiling_page(request: Request):
    user = require_user(request)

    db = SessionLocal()
    records = (
        db.query(ProfileResult)
        .filter_by(email=user["email"])
        .order_by(ProfileResult.created_at.desc())
        .all()
    )
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

    if not file.filename:
        return RedirectResponse("/datasentinel/profiling", status_code=303)

    uid = uuid.uuid4().hex
    path = PROFILING_DIR / f"{uid}_{file.filename}"

    with open(path, "wb") as f:
        f.write(await file.read())

    db = SessionLocal()
    record = ProfileResult(
        email=user["email"],
        input_file=file.filename,
        profile_url=f"/profiles/{uid}"
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    db.close()

    return RedirectResponse("/datasentinel/profiling", status_code=303)

# ─────────────────────────────
# Prediction (PRO)
# ─────────────────────────────

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

    for r in db.query(ValidationResult).filter_by(email=user["email"]).all():
        history.append({
            "module": "Validation",
            "file": r.input_file,
            "status": r.status,
            "created_at": r.created_at,
            "view": f"/datasentinel/view/validation/{r.id}"
        })

    for r in db.query(NormalizedFile).filter_by(email=user["email"]).all():
        history.append({
            "module": "Normalization",
            "file": r.input_file,
            "status": "success",
            "created_at": r.created_at,
            "view": f"/datasentinel/view/normalization/{r.id}"
        })

    for r in db.query(ConvertedFile).filter_by(email=user["email"]).all():
        history.append({
            "module": "Conversion",
            "file": r.original_file,
            "status": "success",
            "created_at": r.created_at,
            "view": f"/datasentinel/view/conversion/{r.id}"
        })

    for r in db.query(ProfileResult).filter_by(email=user["email"]).all():
        history.append({
            "module": "Profiling",
            "file": r.input_file,
            "status": "success",
            "created_at": r.created_at,
            "view": f"/datasentinel/view/profiling/{r.id}"
        })

    db.close()

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
ALLOWED_MODULES = {"validation", "normalization", "conversion", "profiling"}

@app.get("/view/{module}/{id}", response_class=HTMLResponse)
async def view_placeholder(request: Request, module: str, id: int):
    user = require_user(request)

    if module not in ALLOWED_MODULES:
        return RedirectResponse("/datasentinel/reports", status_code=302)

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
    try:
        user = require_user(request)
    except HTTPException:
        return RedirectResponse("/datasentinel/login", status_code=302)

    return templates.TemplateResponse(
        "settings.html",
        {"request": request, "user": user}
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

