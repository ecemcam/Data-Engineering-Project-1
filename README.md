# DATA PIPELINE PROJECT
 Data engineering project with dbt, bigquery and airflow orchestration.

## Quick Setup

### 1. Install Dependencies
```bash
cd ~/Desktop/data-pipeline-project
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Set Up Google Cloud Credentials
```bash
# Download service account JSON key from Google Cloud Console
mv ~/Downloads/gcp-key.json ~/.gcp/
chmod 600 ~/.gcp/gcp-key.json
export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/gcp-key.json
```

### 3. Configure dbt
```bash
cd ~/Desktop/data-pipeline-project/dbt
cat > profiles.yml << EOF
beije:
  outputs:
    dev:
      type: bigquery
      project: data-pipeline-project-id
      dataset: analytics_staging_raw
      keyfile: ~/.gcp/gcp-key.json
  target: dev
EOF

dbt debug
dbt deps
```

### 4. Start Airflow
```bash
cd ~/Desktop/data-pipeline-project
export AIRFLOW_HOME=~/Desktop/data-pipeline-project/airflow_orchestration
airflow db migrate
airflow standalone
```

Access UI: http://localhost:8080

### 5. Place Raw Data Files
Put all 10 CSV files in: `~/Desktop/data-pipeline-project/raw_data/`

## Run Pipeline

**Via Airflow UI:**
1. Go to http://localhost:8080
2. Find DAG: `beije_elt_pipeline_layered`
3. Click Trigger DAG

**Via Terminal:**
```bash
airflow dags trigger beije_elt_pipeline_layered
```

## Pipeline Steps

1. **Data Readiness** - Check CSV files exist
2. **Data Ingestion** - Load CSVs to BigQuery
3. **Source Tests** - Check data freshness
4. **Staging** - Clean and standardize data
5. **Staging Tests** - Validate cleaned data
6. **Intermediate** - Transform data
7. **Dimensions** - Create reference tables
8. **Facts** - Create transaction tables
9. **Marts** - Create analytics summaries
10. **Intermediate Tests** - Test business rules
11. **Data Quality Tests** - Final validation
12. **Log Metrics** - Report execution time

## Project Structure

```
data-pipeline-project/
├── airflow_orchestration/ # Airflow setup
│ └── dags/
│ └── etl_pipeline.py # Airflow DAG defining the ETL pipeline
├── dbt/ # dbt transformations
│ ├── analyses/ # SQL queries for data analysis
│ └── models/
│ ├── staging/ # Staging tables
│ ├── intermediate/ # Intermediate transformations
│ ├── dims/ # Dimension tables
│ ├── facts/ # Fact tables
│ └── marts/ # Data marts
├── ingestion/
│ └── load_to_bg.py # Script to load CSVs into BigQuery
├── raw_data/ # Source CSV files
├── venv/ # Python virtual environment
└── requirements.txt # Python dependencies
```

## Key Tables

**Dimensions:**
- `dim_customer` - Customer info
- `dim_subscription` - Subscription details

**Facts:**
- `fct_order` - Orders with revenue
- `fct_shipment` - Shipment tracking
- `fct_marketing_spend` - Marketing spend

**Marts:**
- `mart_subscription_daily` - Daily subscription metrics
- `mart_revenue_daily` - Daily revenue
- `mart_acquisition_efficiency` - CAC and payback
- `mart_kpis_latest` - Overall KPIs

## Troubleshooting

**dbt connection fails:**
```bash
cd dbt
dbt debug
```

**DAG not showing in Airflow:**
```bash
rm -rf airflow_orchestration/dags/__pycache__
airflow standalone
```

**Test dbt commands:**
```bash
cd dbt
dbt run --select staging
dbt test --select staging
```

## Tech Stack

- Python 3.12
- dbt 1.11
- Apache Airflow 3.1.0
- Google BigQuery
- Pandas

## Contact

For issues or questions, check logs at:
- Airflow logs: `airflow_orchestration/logs/`
- dbt logs: `dbt/logs/`
