from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import logging
import time
import subprocess
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================
PROJECT_ROOT = Path.home() / "Desktop" / "data-pipeline-project"
DBT_PROJECT_PATH = PROJECT_ROOT / "dbt"
INGESTION_SCRIPT = PROJECT_ROOT / "ingestion" / "load_to_bg.py"
RAW_DATA_PATH = PROJECT_ROOT / "raw_data"
LOGS_PATH = PROJECT_ROOT / "logs"

# Expected CSV files
EXPECTED_FILES = [
    'users.csv',
    'subscriptions.csv',
    'orders.csv',
    'shipments.csv',
    'Marketing Spend (TRY).csv',
    'countries.csv',
    'states.csv',
    'cities.csv',
    'neighborhoods.csv',
    'addresses.csv',
]

default_args = {
    'owner': 'data-engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=4),
}

dag = DAG(
    'beije_elt_pipeline_layered',
    default_args=default_args,
    description='BEIJE ELT Pipeline with Layer-by-Layer Testing',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['beije', 'elt', 'production'],
)

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================
def execute_command(cmd, task_name):
    """Execute shell command and log output"""
    logger.info(f"Executing: {cmd}")
    
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"✅ {task_name} completed successfully")
        if result.stdout:
            logger.info(f"Output: {result.stdout}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {task_name} failed")
        logger.error(f"Error: {e.stderr}")
        raise AirflowException(f"{task_name} failed with error: {e.stderr}")

# ============================================================================
# STEP 0: DATA READINESS - Check CSV Files Exist
# ============================================================================
def check_data_files_exist(**context):
    """Verify all required CSV files exist in raw_data folder"""
    logger.info("=" * 80)
    logger.info("📋 STEP 0: DATA READINESS CHECK")
    logger.info("=" * 80)
    logger.info(f"Checking for CSV files in: {RAW_DATA_PATH}")
    
    missing_files = []
    found_files = []
    
    for file in EXPECTED_FILES:
        file_path = RAW_DATA_PATH / file
        if file_path.exists():
            found_files.append(file)
            logger.info(f"  ✅ {file} exists")
        else:
            missing_files.append(file)
            logger.warning(f"  ❌ {file} NOT FOUND")
    
    logger.info("")
    logger.info(f"Summary: {len(found_files)}/{len(EXPECTED_FILES)} files found")
    
    if missing_files:
        logger.error(f"Missing files: {', '.join(missing_files)}")
        raise AirflowException(f"Missing required CSV files: {missing_files}")
    
    logger.info("✅ All required CSV files are present")
    context['task_instance'].xcom_push(key='files_ready', value=True)

data_readiness = PythonOperator(
    task_id='step_0_data_readiness_check',
    python_callable=check_data_files_exist,
    dag=dag,
)

# ============================================================================
# STEP 1: DATA INGESTION
# ============================================================================
def run_data_ingestion(**context):
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info("📥 STEP 1: DATA INGESTION")
    logger.info("=" * 80)
    logger.info("Loading all CSV files to BigQuery...")
    
    cmd = f"cd {PROJECT_ROOT} && python {INGESTION_SCRIPT}"
    execute_command(cmd, "Data Ingestion")
    
    duration = time.time() - start_time
    logger.info(f"⏱️  Duration: {duration:.2f}s")
    
    context['task_instance'].xcom_push(key='ingestion_duration', value=duration)

ingestion = PythonOperator(
    task_id='step_1_data_ingestion',
    python_callable=run_data_ingestion,
    dag=dag,
)

# ============================================================================
# STEP 2: TEST SOURCE FRESHNESS & MISSING IDs (raw_sources.yml - WARNING ONLY)
# ============================================================================
def run_source_tests(**context):
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info("🧪 STEP 2: SOURCE FRESHNESS & MISSING IDs TESTS")
    logger.info("=" * 80)
    logger.info("Running raw_sources.yml - Testing source freshness and missing IDs...")
    logger.info("(Warnings only - will not stop pipeline)")
    
    cmd = f"cd {DBT_PROJECT_PATH} && dbt test --select staging.raw_sources --profiles-dir ."
    
    try:
        execute_command(cmd, "Source Tests")
        logger.info("✅ Source tests completed (warnings noted)")
    except AirflowException as e:
        # Log warning but don't fail - these are warnings only
        logger.warning(f"⚠️  Source test warnings noted: {e}")
        logger.info("Continuing pipeline (source tests are warnings only)")
    
    duration = time.time() - start_time
    logger.info(f"⏱️  Duration: {duration:.2f}s")
    
    context['task_instance'].xcom_push(key='source_test_duration', value=duration)

source_tests = PythonOperator(
    task_id='step_2_source_freshness_tests',
    python_callable=run_source_tests,
    dag=dag,
)

# ============================================================================
# STEP 3: CREATE STAGING MODELS
# ============================================================================
def create_staging_models(**context):
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info("🔄 STEP 3: CREATE STAGING MODELS")
    logger.info("=" * 80)
    logger.info("Creating cleaned staging tables from raw data...")
    
    cmd = f"cd {DBT_PROJECT_PATH} && dbt run --select staging --profiles-dir ."
    execute_command(cmd, "Staging Models Creation")
    
    duration = time.time() - start_time
    logger.info(f"⏱️  Duration: {duration:.2f}s")
    
    context['task_instance'].xcom_push(key='staging_create_duration', value=duration)

staging_create = PythonOperator(
    task_id='step_3_create_staging_models',
    python_callable=create_staging_models,
    dag=dag,
)

# ============================================================================
# STEP 4: TEST STAGING SCHEMA (staging_schema.yml)
# ============================================================================
def test_staging_schema(**context):
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info("🧪 STEP 4: STAGING DATA INTEGRITY TESTS")
    logger.info("=" * 80)
    logger.info("Running staging_schema.yml - Testing PK/FK and data quality...")
    
    cmd = f"cd {DBT_PROJECT_PATH} && dbt test --select staging.staging_schema --profiles-dir ."
    
    try:
        execute_command(cmd, "Staging Schema Tests")
        logger.info("✅ All staging schema tests PASSED")
    except AirflowException as e:
        logger.error("❌ Staging schema tests FAILED - Pipeline stopping")
        raise
    
    duration = time.time() - start_time
    logger.info(f"⏱️  Duration: {duration:.2f}s")
    
    context['task_instance'].xcom_push(key='staging_test_duration', value=duration)

staging_test = PythonOperator(
    task_id='step_4_test_staging_schema',
    python_callable=test_staging_schema,
    dag=dag,
)

# ============================================================================
# STEP 5: CREATE INTERMEDIATE MODELS
# ============================================================================
def create_intermediate_models(**context):
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info("🔄 STEP 5: CREATE INTERMEDIATE MODELS")
    logger.info("=" * 80)
    logger.info("Creating intermediate transformation models...")
    
    cmd = f"cd {DBT_PROJECT_PATH} && dbt run --select intermediate --profiles-dir ."
    execute_command(cmd, "Intermediate Models Creation")
    
    duration = time.time() - start_time
    logger.info(f"⏱️  Duration: {duration:.2f}s")
    
    context['task_instance'].xcom_push(key='intermediate_create_duration', value=duration)

intermediate_create = PythonOperator(
    task_id='step_5_create_intermediate_models',
    python_callable=create_intermediate_models,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# STEP 6: CREATE DIMENSION MODELS
# ============================================================================
def create_dimension_models(**context):
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info("🔄 STEP 6: CREATE DIMENSION MODELS")
    logger.info("=" * 80)
    logger.info("Creating dimension tables (dim_*)...")
    
    cmd = f"cd {DBT_PROJECT_PATH} && dbt run --select dims --profiles-dir ."
    execute_command(cmd, "Dimension Models Creation")
    
    duration = time.time() - start_time
    logger.info(f"⏱️  Duration: {duration:.2f}s")
    
    context['task_instance'].xcom_push(key='dims_create_duration', value=duration)

dims_create = PythonOperator(
    task_id='step_6_create_dimension_models',
    python_callable=create_dimension_models,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# STEP 7: CREATE FACT MODELS
# ============================================================================
def create_fact_models(**context):
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info("🔄 STEP 7: CREATE FACT MODELS")
    logger.info("=" * 80)
    logger.info("Creating fact tables (fct_*)...")
    
    cmd = f"cd {DBT_PROJECT_PATH} && dbt run --select facts --profiles-dir ."
    execute_command(cmd, "Fact Models Creation")
    
    duration = time.time() - start_time
    logger.info(f"⏱️  Duration: {duration:.2f}s")
    
    context['task_instance'].xcom_push(key='facts_create_duration', value=duration)

facts_create = PythonOperator(
    task_id='step_7_create_fact_models',
    python_callable=create_fact_models,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# STEP 8: CREATE MART MODELS
# ============================================================================
def create_mart_models(**context):
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info("🔄 STEP 8: CREATE DATA MART MODELS")
    logger.info("=" * 80)
    logger.info("Creating data marts for analytics (mart_*)...")
    
    cmd = f"cd {DBT_PROJECT_PATH} && dbt run --select marts --profiles-dir ."
    execute_command(cmd, "Mart Models Creation")
    
    duration = time.time() - start_time
    logger.info(f"⏱️  Duration: {duration:.2f}s")
    
    context['task_instance'].xcom_push(key='marts_create_duration', value=duration)

marts_create = PythonOperator(
    task_id='step_8_create_mart_models',
    python_callable=create_mart_models,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# STEP 9: TEST INTERMEDIATE MODELS (after dims, facts, marts created)
# ============================================================================
def test_intermediate_models(**context):
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info("🧪 STEP 9: INTERMEDIATE DATA QUALITY & BUSINESS RULES TESTS")
    logger.info("=" * 80)
    logger.info("Running intermediate tests (including dbt-utils accepted_range)...")
    logger.info("Testing dims, facts, and marts with business rules...")
    
    cmd = f"cd {DBT_PROJECT_PATH} && dbt test --select intermediate --profiles-dir ."
    
    try:
        execute_command(cmd, "Intermediate Tests")
        logger.info("✅ All intermediate tests PASSED")
    except AirflowException as e:
        logger.error("❌ Intermediate tests FAILED - Pipeline stopping")
        raise
    
    duration = time.time() - start_time
    logger.info(f"⏱️  Duration: {duration:.2f}s")
    
    context['task_instance'].xcom_push(key='intermediate_test_duration', value=duration)

intermediate_test = PythonOperator(
    task_id='step_9_test_intermediate_models',
    python_callable=test_intermediate_models,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# STEP 10: RUN ALL DATA QUALITY TESTS
# ============================================================================
def run_all_data_quality_tests(**context):
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info("🧪 STEP 10: ALL REMAINING DATA QUALITY TESTS")
    logger.info("=" * 80)
    logger.info("Running all remaining dbt tests...")
    
    cmd = f"cd {DBT_PROJECT_PATH} && dbt test --exclude tag:source_test --profiles-dir ."
    
    try:
        execute_command(cmd, "All Data Quality Tests")
        logger.info("✅ All data quality tests PASSED")
    except AirflowException as e:
        logger.error("❌ Data quality tests FAILED - Pipeline stopping")
        raise
    
    duration = time.time() - start_time
    logger.info(f"⏱️  Duration: {duration:.2f}s")
    
    context['task_instance'].xcom_push(key='all_tests_duration', value=duration)

all_tests = PythonOperator(
    task_id='step_10_all_data_quality_tests',
    python_callable=run_all_data_quality_tests,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# STEP 11: LOG EXECUTION METRICS
# ============================================================================
def log_execution_metrics(**context):
    task_instance = context['task_instance']
    
    # Pull all metrics from XCom
    ingestion_duration = task_instance.xcom_pull(
        task_ids='step_1_data_ingestion', key='ingestion_duration') or 0
    source_test_duration = task_instance.xcom_pull(
        task_ids='step_2_source_freshness_tests', key='source_test_duration') or 0
    staging_create_duration = task_instance.xcom_pull(
        task_ids='step_3_create_staging_models', key='staging_create_duration') or 0
    staging_test_duration = task_instance.xcom_pull(
        task_ids='step_4_test_staging_schema', key='staging_test_duration') or 0
    intermediate_create_duration = task_instance.xcom_pull(
        task_ids='step_5_create_intermediate_models', key='intermediate_create_duration') or 0
    intermediate_test_duration = task_instance.xcom_pull(
        task_ids='step_6_test_intermediate_models', key='intermediate_test_duration') or 0
    dims_create_duration = task_instance.xcom_pull(
        task_ids='step_7_create_dimension_models', key='dims_create_duration') or 0
    facts_create_duration = task_instance.xcom_pull(
        task_ids='step_8_create_fact_models', key='facts_create_duration') or 0
    marts_create_duration = task_instance.xcom_pull(
        task_ids='step_9_create_mart_models', key='marts_create_duration') or 0
    all_tests_duration = task_instance.xcom_pull(
        task_ids='step_10_all_data_quality_tests', key='all_tests_duration') or 0
    
    total_duration = (
        ingestion_duration + source_test_duration + staging_create_duration +
        staging_test_duration + intermediate_create_duration + intermediate_test_duration +
        dims_create_duration + facts_create_duration + marts_create_duration + all_tests_duration
    )
    
    logger.info("=" * 80)
    logger.info("📊 PIPELINE EXECUTION METRICS")
    logger.info("=" * 80)
    logger.info(f"Execution Date: {context['execution_date']}")
    logger.info("")
    logger.info("⏱️  DURATION BREAKDOWN:")
    logger.info(f"  Step 0: Data Readiness Check       (N/A - validation only)")
    logger.info(f"  Step 1: Data Ingestion             {ingestion_duration:>10.2f}s")
    logger.info(f"  Step 2: Source Freshness Tests     {source_test_duration:>10.2f}s")
    logger.info(f"  Step 3: Create Staging Models      {staging_create_duration:>10.2f}s")
    logger.info(f"  Step 4: Test Staging Schema        {staging_test_duration:>10.2f}s")
    logger.info(f"  Step 5: Create Intermediate Models {intermediate_create_duration:>10.2f}s")
    logger.info(f"  Step 6: Test Intermediate Models   {intermediate_test_duration:>10.2f}s")
    logger.info(f"  Step 7: Create Dimension Models    {dims_create_duration:>10.2f}s")
    logger.info(f"  Step 8: Create Fact Models         {facts_create_duration:>10.2f}s")
    logger.info(f"  Step 9: Create Mart Models         {marts_create_duration:>10.2f}s")
    logger.info(f"  Step 10: All Data Quality Tests    {all_tests_duration:>10.2f}s")
    logger.info(f"  {'─' * 50}")
    logger.info(f"  Total Pipeline Duration:           {total_duration:>10.2f}s")
    logger.info("")
    logger.info("=" * 80)
    logger.info("✅ PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)

log_metrics = PythonOperator(
    task_id='step_11_log_execution_metrics',
    python_callable=log_execution_metrics,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# ERROR HANDLER
# ============================================================================
def on_pipeline_failure(**context):
    logger.error("=" * 80)
    logger.error("❌ PIPELINE FAILED")
    logger.error("=" * 80)
    logger.error(f"Failed Task: {context['task'].task_id}")
    logger.error(f"Execution Date: {context['execution_date']}")
    logger.error("Pipeline execution stopped due to error")
    logger.error("=" * 80)

# ============================================================================
# DEFINE TASK DEPENDENCIES (Execution Order)
# ============================================================================
(data_readiness >> ingestion >> source_tests >> staging_create >> staging_test >>
 intermediate_create >> intermediate_test >> dims_create >> facts_create >>
 marts_create >> all_tests >> log_metrics)

# Set error handler
dag.on_failure_callback = on_pipeline_failure