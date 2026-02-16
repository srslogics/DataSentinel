# 🚀 DataSentinel — Production Deployment Guide (AWS + Custom Domain)

**DataSentinel** is a production-grade data validation, conversion, normalization, profiling, and prediction platform built with **FastAPI**, deployed on **AWS ECS (Fargate)**, backed by **S3** and **PostgreSQL**.

This document provides a complete backend deployment guide to run DataSentinel on AWS and serve it at:

https://srslogics.com/datasentinel

---

## 🧠 Architecture Overview

User Browser  
↓  
Domain: srslogics.com  
↓  
Application Load Balancer (HTTPS)  
↓  
ECS Fargate Service (FastAPI – DataSentinel)  
↓  
S3 (File Storage)  
↓  
PostgreSQL (RDS / Supabase)

---

## 🛠 Tech Stack

### Application
- FastAPI  
- Jinja2 (server-rendered UI)  
- SQLAlchemy ORM  
- Session-based authentication  
- Stripe subscriptions (PRO features)

### AWS Infrastructure
- ECS Fargate  
- ECR (Docker registry)  
- Application Load Balancer  
- S3 (file storage)  
- IAM roles  
- Route53 (domain + DNS)

### Database
- PostgreSQL  
  - AWS RDS or  
  - Supabase (managed PostgreSQL)
---

## 🔐 Environment Variables

Create a `.env` file:
```bash
SECRET_KEY=your_secret_key
DATABASE_URL=postgresql://user:password@host:5432/dbname

AWS_ACCESS_KEY_ID=xxxx
AWS_SECRET_ACCESS_KEY=xxxx
AWS_REGION=ap-south-1
AWS_BUCKET_NAME=datasentinel

STRIPE_SECRET_KEY=xxxx
STRIPE_PRICE_ID=xxxx

APP_ENV=production
```

---

## 🐳 Docker Build & Push (ECR)

Build image:
```bash
docker build -t datasentinel-backend .
```

Tag for ECR:
```bash
docker tag datasentinel-backend:latest
ACCOUNT_ID.dkr.ecr.ap-south-1.amazonaws.com/datasentinel-backend:latest
```

Login to ECR:
```bash
aws ecr get-login-password --region ap-south-1 |
docker login --username AWS --password-stdin ACCOUNT_ID.dkr.ecr.ap-south-1.amazonaws.com
```

Push image:
```bash
docker push ACCOUNT_ID.dkr.ecr.ap-south-1.amazonaws.com/datasentinel-backend:latest
```

---

## ☁️ ECS Deployment

Create ECS Cluster:
- Name: datasentinel-cluster
- Launch type: Fargate

Task Definition:
- CPU: 1 vCPU
- Memory: 2 GB
- Container Port: 8080
- Image: ECR image URL

Add all environment variables from `.env`.

---

## 🌐 Application Load Balancer (ALB)

Listener:
- HTTPS : 443
- SSL Certificate: ACM (srslogics.com)

Target Group:
- Name: datasentinel-tg
- Protocol: HTTP
- Port: 8080

Health Check Path:
/datasentinel/health


---

## 🔀 Path-Based Routing

We want:
https://srslogics.com/datasentinel


ALB Listener Rule:
IF path is:
/datasentinel
/datasentinel/*
THEN forward to:
datasentinel-tg


---

## ⚙️ FastAPI Root Path

In `main.py`:
```bash
app = FastAPI(
title="DataSentinel",
)
```

---

## 🧪 Health Check Endpoint
```bash
@app.get("/health")
@app.head("/health")
def health():
return {"status": "ok"}
```

---

## 🔐 Security Groups
```bash
Inbound:
- HTTP 80
- HTTPS 443
- TCP 8080 (from ALB only)
```
---

## 💳 Stripe Subscription Flow
```bash
Routes:
/subscribe/pro
/subscription/success
/subscription/cancel
```

PRO-only features:
- Prediction
- Advanced profiling

---

## 📊 Modules Included

- Validation  
- Conversion  
- Normalization  
- Profiling  
- Prediction (PRO)  
- Reports & Audit History  

---

## 🚀 Production URLs

* https://srslogics.com/datasentinel
* https://srslogics.com/datasentinel/docs
* https://srslogics.com/datasentinel/dashboard


---

## 🧯 Common Issues

403 on Docker push:
- Attach `AmazonEC2ContainerRegistryFullAccess`

ECS task exits immediately:
- CPU/memory mismatch
- Wrong container port
- Missing environment variables

Site not loading:
- Check ALB target health
- Verify `/datasentinel/health`

---

## 🔮 Planned Improvements

- S3-only storage
- RDS PostgreSQL
- Background jobs
- Autoscaling ECS
- GitHub Actions CI/CD
- Stripe webhooks
- Terraform infrastructure

---

Maintained by **SrS Logics**  
https://srslogics.com
