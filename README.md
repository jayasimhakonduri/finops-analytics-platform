# 💰 FinOps Analytics Platform

> Unified multi-cloud billing API and PySpark ETL platform processing **100M+ billing records/day** across **AWS and Azure**. Covers 30+ resource types: EC2, S3, RDS, Lambda, EKS, Azure VMs, Blob Storage, SQL DB, AKS, Functions, and more.

![Python](https://img.shields.io/badge/Python-3.11-blue) ![FastAPI](https://img.shields.io/badge/FastAPI-0.111-green) ![PySpark](https://img.shields.io/badge/PySpark-3.5-orange) ![AWS](https://img.shields.io/badge/AWS-Cost_Explorer-yellow) ![Azure](https://img.shields.io/badge/Azure-Cost_Management-blue)

---

## 🏗️ Architecture

```
AWS Cost Explorer API ──┐
AWS Billing API ────────┤                           ┌── Dashboard APIs
                        ├──► BillingAggregator ─────┤
Azure Cost Mgmt API ────┘        (FastAPI)           └── Executive Reports

         ▼
  Nightly PySpark ETL
  (100M+ records/day)
         │
         ├── AWS CUR (S3) ─────► normalize ─┐
         └── Azure Export (Blob) ► normalize ─┴──► PostgreSQL / Apache Druid
```

## ✨ Key Features

| Feature | Detail |
|---|---|
| Multi-cloud billing | AWS Cost Explorer + Azure Cost Management unified API |
| 30+ resource types | EC2, S3, RDS, Lambda, EKS, CloudFront, DynamoDB (AWS); VMs, Blob, SQL, AKS, Functions, Cosmos DB (Azure) |
| PySpark ETL | 100M+ records/day with adaptive query execution |
| Cost forecasting | AWS Cost Explorer forecasting + Azure budget projections |
| Anomaly detection | AWS Cost Anomaly Detection + custom Azure spike logic |
| Tag-based chargeback | Group costs by team/project/environment tags |
| Nightly scheduler | APScheduler-driven ETL at 02:00 AM |

## 📊 Production Impact

- **100M+** billing records processed per day
- **20% cloud cost reduction** identified and actioned via cost insights
- **3× query speed** improvement via Apache Druid OLAP over raw PostgreSQL
- **API response < 200ms** via Redis caching of aggregated rollups
- Covers **AWS + Azure** — 30+ distinct resource types normalized to unified schema

## 🚀 Quick Start

```bash
git clone https://github.com/jayasimhakonduri/finops-analytics-platform
cd finops-analytics-platform
pip install -r requirements.txt
cp .env.example .env   # add AWS & Azure credentials
python main.py          # API at http://localhost:8001/docs
```

## 📡 API Examples

### Get AWS costs for January
```bash
curl -X POST http://localhost:8001/api/v1/billing/costs \
  -H "X-API-Key: your-key" \
  -d '{"provider":"aws","start_date":"2025-01-01","end_date":"2025-01-31","resource_types":["EC2","S3","RDS","Lambda"]}'
```

### Get resource inventory (all VMs > $1000/month)
```bash
curl "http://localhost:8001/api/v1/resources?provider=azure&resource_type=VM&min_monthly_cost=1000" \
  -H "X-API-Key: your-key"
```

### Forecast next 3 months
```bash
curl -X POST http://localhost:8001/api/v1/billing/forecast \
  -H "X-API-Key: your-key" \
  -d '{"provider":"all","months_ahead":3}'
```

### Cost by team tag
```bash
curl "http://localhost:8001/api/v1/billing/by-tag?tag_key=team&year=2025&month=1" \
  -H "X-API-Key: your-key"
```

## 📁 Project Structure

```
finops-analytics-platform/
├── main.py
├── api/
│   ├── routes.py        # All FastAPI endpoints
│   ├── billing_api.py   # AWS + Azure API clients + aggregator
│   ├── models.py        # Pydantic schemas
│   ├── auth.py
│   └── config.py
├── etl/
│   ├── spark_pipeline.py  # PySpark ETL: 100M+ records/day
│   └── scheduler.py       # Nightly ETL scheduler
└── requirements.txt
```

---

Built by [Jaya Simha Konduri](https://linkedin.com/in/jaya-simha-713234130) · [Portfolio](https://jayasimhakonduri.github.io)
