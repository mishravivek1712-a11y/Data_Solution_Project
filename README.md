# Data_Solution_Project
# Fannie Mae Big Data POC — End-to-End AWS Implementation Guide

A fully runnable Proof of Concept implementing the complete big data pipeline:
**Data Sources → Kinesis → S3 (Medallion) → Glue ETL → RDS → Analytics**

---

## Architecture Overview

```
Data Sources          Ingestion              Storage (S3 Medallion)
──────────────        ──────────────         ──────────────────────────────────────────
Loan Events     ──►  Kinesis Streams  ──►   Bronze (raw JSON, GZIP)
(producer.py)         │                      ↓ Glue ETL Job 1
                       ↓                    Silver (Parquet, validated, partitioned)
                      Kinesis Firehose        ↓ Glue ETL Job 2
                       │                    Gold (aggregated: portfolio, risk, HMDA, KPIs)
                       ▼                            │
                      S3 Raw                         ├── Athena (ad-hoc SQL)
                                                     ├── Redshift Spectrum
Processing                                           └── QuickSight dashboards
──────────────
Glue Data Catalog  (schema registry, crawlers)
Glue ETL Jobs      (PySpark: raw→silver, silver→gold)

Serving
──────────────
RDS Aurora PostgreSQL  (loan master, OLTP, 99.99% SLA)
  Tables: loan_master, borrower, property, risk_scores, loan_events, servicer_scorecard

Consuming
──────────────
Athena queries → Gold S3 Parquet
RDS queries    → Real-time OLTP (loan lookup, risk alerts)
consumer.py    → Portfolio KPIs, risk matrix, servicer scorecard, fraud alerts
```

---

## File Structure

```
fannie_mae_poc/
├── config.py                   ← EDIT THIS FIRST (bucket prefix, region, etc.)
├── requirements.txt            ← Python dependencies
├── provision.py                ← Step 1: Create all AWS infrastructure
├── producer.py                 ← Step 2: Stream simulated loan events to Kinesis
├── rds_setup.py                ← Step 3: Create RDS schema + load data
├── consumer.py                 ← Step 4: Run analytics (Athena + RDS)
├── glue_jobs/
│   ├── glue_raw_to_silver.py   ← Glue PySpark: Bronze → Silver ETL
│   └── glue_silver_to_gold.py  ← Glue PySpark: Silver → Gold aggregations
└── poc_output.json             ← Auto-generated: all resource ARNs/endpoints
```

---

## Prerequisites

### 1. AWS Account Setup

You need an AWS IAM user/role with these permissions:
- `AmazonS3FullAccess`
- `AmazonKinesisFullAccess`
- `AmazonKinesisFirehoseFullAccess`
- `AWSGlueConsoleFullAccess`
- `AmazonRDSFullAccess`
- `IAMFullAccess`
- `AmazonAthenaFullAccess`

**Set credentials** (choose one method):
```bash
# Method 1: AWS CLI profile
aws configure --profile fnm-poc
export AWS_PROFILE=fnm-poc

# Method 2: Environment variables
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1

# Method 3: EC2/ECS instance role (no config needed)
```

### 2. Python Environment

```bash
python3 --version      # needs 3.8+
pip install -r requirements.txt
```

### 3. Configure the POC

Edit `config.py` — change at minimum:
```python
BUCKET_PREFIX = "yourname-fnm"   # must be globally unique!
AWS_REGION    = "us-east-1"      # your region
RDS_PASSWORD  = "YourPassword1!" # or set env var: export RDS_PASSWORD=...
```

---

## Step-by-Step Execution

### Step 1 — Provision Infrastructure (~8 minutes)

```bash
python provision.py
```

This creates (in order):
1. IAM roles for Glue and Firehose
2. S3 buckets: raw, silver, gold, scripts, athena-results
3. Kinesis Data Stream (1 shard)
4. Kinesis Firehose (stream → S3 raw, buffered 5MB/60s, GZIP)
5. Glue Database + Crawlers (6-hour schedule)
6. Glue ETL Jobs (registers PySpark scripts)
7. RDS Aurora PostgreSQL cluster (takes ~5 min)

Output saved to `poc_output.json`.

**Estimated cost**: ~$0.50/hour while running (Kinesis $0.015/shard-hr, RDS db.t3.medium $0.068/hr, S3 pennies).

---

### Step 2 — Stream Loan Events (~2 minutes)

```bash
# Default: 500 loan events (25 per batch, every 2 seconds)
python producer.py

# Continuous streaming (Ctrl+C to stop)
python producer.py --infinite

# Custom rate
python producer.py --records 2000 --batch 50 --interval 1
```

Watch events flow: Kinesis → Firehose → S3 raw zone (with ~60s buffer).

Check S3:
```bash
aws s3 ls s3://yourname-fnm-raw/loans/ --recursive
```

---

### Step 3 — Run Glue ETL Pipeline

```bash
# Trigger both Glue jobs in sequence
python consumer.py pipeline

# Or run individually via AWS CLI:
aws glue start-job-run --job-name fnm-etl-raw-to-silver
aws glue start-job-run --job-name fnm-etl-silver-to-gold

# Monitor in AWS Console:
# Glue → Jobs → Job runs → View logs in CloudWatch
```

**Job 1 (raw → silver, ~3 min):**
- Reads raw JSON from S3 bronze zone
- Validates FICO (300-850), LTV, DTI, loan amount ranges
- Quarantines invalid records to `silver/quarantine/`
- Enriches: adds expected_loss_usd, ltv_bucket, fico_band, monthly_pi
- Deduplicates by event_id
- Writes Parquet (snappy), partitioned by year/month/day/state

**Job 2 (silver → gold, ~3 min):**
- `portfolio_summary` — state × risk tier × loan purpose aggregations
- `risk_matrix` — FICO band × LTV bucket credit matrix
- `servicer_scorecard` — servicer delinquency/foreclosure rates
- `hmda_lar` — CFPB HMDA Loan Application Register (regulatory output)
- `daily_kpi` — daily snapshot for Lookout for Metrics

---

### Step 4 — Set Up RDS Schema

```bash
# Create schema + load from S3 silver (run after Glue job 1 completes)
python rds_setup.py

# Schema only (no data load)
python rds_setup.py --schema-only

# Load only (schema already exists)
python rds_setup.py --load-only
```

Creates tables: `loan_master`, `borrower`, `property`, `counterparty`, `risk_scores`, `loan_events` (partitioned by quarter).

Creates views: `vw_loan_portfolio`, `vw_portfolio_kpis`.

---

### Step 5 — Run Analytics (Consumer Layer)

```bash
python consumer.py
```

Runs these queries and prints results:

| Query | Source | Description |
|-------|--------|-------------|
| Top 10 states by UPB | Athena → S3 Gold | Portfolio geographic exposure |
| Credit risk matrix | Athena → S3 Gold | FICO × LTV expected loss heat map |
| Servicer scorecard | Athena → S3 Gold | Delinquency rates by servicer |
| Portfolio KPIs by risk tier | RDS Aurora | Real-time OLTP aggregation |
| High-risk loan alerts | RDS Aurora | Fraud score > 0.75 AND PD > 5% |

---

## Connect Additional Consumers

### Athena (ad-hoc SQL on S3 Gold)
```sql
-- In AWS Athena console, select database: fnm_poc_db
SELECT property_state, SUM(total_upb) as upb, AVG(avg_fico) as fico
FROM portfolio_summary
GROUP BY property_state
ORDER BY upb DESC;
```

### QuickSight Dashboard
1. QuickSight → New Dataset → Athena → Select `fnm_poc_db`
2. Choose `portfolio_summary` or `risk_matrix`
3. Build: bar chart (state vs UPB), scatter plot (FICO vs LTV), KPI cards

### Redshift Spectrum (external tables)
```sql
CREATE EXTERNAL SCHEMA fnm_gold FROM DATA CATALOG
DATABASE 'fnm_poc_db'
IAM_ROLE 'arn:aws:iam::ACCOUNT:role/RedshiftSpectrumRole';

SELECT * FROM fnm_gold.portfolio_summary LIMIT 100;
```

---

## Teardown (avoid ongoing costs)

```bash
python provision.py --teardown
```

Deletes: Glue jobs, crawlers, DB, Firehose, Kinesis stream, RDS cluster (~10 min), S3 buckets (emptied first), IAM roles.

**Estimated total POC cost**: $2–5 if run for 2–4 hours then torn down.

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `BucketAlreadyExists` | Change `BUCKET_PREFIX` in config.py to something unique |
| `AccessDenied` on S3 | Check IAM user has `AmazonS3FullAccess` |
| Glue job fails | Check CloudWatch Logs → `/aws-glue/jobs/error/` |
| RDS connection refused | Add your IP to the RDS security group inbound rules |
| Firehose not delivering | Wait 60–90s after producer runs (Firehose has buffer interval) |
| Athena query fails | Confirm Glue crawlers ran and tables exist in `fnm_poc_db` |

---

## Extending to Production

| POC | Production |
|-----|-----------|
| 1 Kinesis shard | Auto-scaling Kinesis (100+ shards) |
| Simulated JSON data | Real MISMO XML from lender APIs |
| db.t3.medium RDS | Aurora Global (multi-region, auto-scaling) |
| Manual Glue triggers | Airflow / Step Functions orchestration |
| Public RDS (security group) | VPC private subnet, VPN/Direct Connect |
| No encryption in transit | TLS everywhere, KMS CMK for S3/RDS |
| Single region | Multi-region DR with S3 cross-region replication |
| No monitoring | CloudWatch dashboards, PagerDuty, Lookout for Metrics |

---

## AWS AI Service Extensions

After the base pipeline is working, add:

```bash
# Textract: extract loan documents
aws textract start-document-analysis --document-location S3Object={Bucket=fnm-raw,...}

# SageMaker: train PD model on gold data
# See: sagemaker_pipeline.py (companion script)

# Bedrock: loan summary generation
aws bedrock-runtime invoke-model --model-id anthropic.claude-3-5-sonnet-20241022-v2:0 ...
```

---

*Fannie Mae Big Data POC — built for interview demonstration and environment replication.*
*All loan data is synthetically generated — no real PII.*
