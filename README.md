finops-analytics-platform/
├── README.md
├── api/
│   ├── billing_api.py   ← AWS Cost Explorer + Azure Cost Mgmt
│   └── routes.py        ← FastAPI routes
├── etl/
│   ├── spark_pipeline.py ← PySpark ETL jobs
│   └── normalizer.py    ← multi-cloud schema normalization
├── requirements.txt
└── .env.example
