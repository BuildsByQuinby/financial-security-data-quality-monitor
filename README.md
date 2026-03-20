# Financial Security Data Quality Monitor

## 📊 Overview
This project simulates a financial institution’s daily security data feed validation process. It identifies data quality issues, detects anomalies, performs reconciliation against prior-day records, and generates audit-ready reports.

## 🎯 Objective
To demonstrate how financial data systems monitor incoming security data for accuracy, consistency, and operational risk before ingestion into production systems.

## 🛠️ Tools Used
- Python (pandas)
- SQL
- CSV data processing
- Data validation techniques

## 🔍 Key Features
- Detects missing or invalid values
- Identifies duplicate records
- Flags negative or unrealistic prices
- Validates date logic (issue vs maturity)
- Reconciles current vs prior-day data
- Detects abnormal price changes
- Generates audit and exception reports

## 📁 Project Structure
```text
financial-security-data-quality-monitor/
│
├── data/
│   ├── previous_day_data.csv
│   └── current_day_data.csv
│
├── sql/
│   ├── duplicate_checks.sql
│   ├── null_checks.sql
│   ├── reconciliation_checks.sql
│   └── anomaly_checks.sql
│
├── scripts/
│   ├── validate_data.py
│   └── generate_audit_summary.py
│
├── output/
│   ├── flagged_records.csv
│   ├── validation_report.csv
│   └── audit_summary.txt
│
├── images/
│   └── dashboard_preview.png
│
├── README.md
└── requirements.txt
```
## 📊 Outputs
- `flagged_records.csv` → Records with identified issues  
- `validation_report.csv` → Summary of validation checks  
- `audit_summary.txt` → High-level audit results  

## 🚨 Why This Matters
Poor-quality financial data can lead to:
- Incorrect reporting
- Trade errors
- Reconciliation failures
- Operational risk

This project demonstrates real-world controls used in financial data operations.

## ▶️ How to Run
1. Install dependencies: pip install -r requirements.txt
2. Run validation: python scripts/validate_data.py
3. enerate audit summary: python scripts/generate_audit_summary.py

## 📌 Future Improvements
- Add database integration (SQLite/PostgreSQL)
- Build dashboard visualization (Tableau or Power BI)
- Automate daily data pipeline
