import sys
import os

# Add the src folder to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Now import tasks correctly
from customer_churn_data_pipeline.data_ingestion.data_ingestion import run as ingest_data
from customer_churn_data_pipeline.data_validation.data_validation import run as validate_data
from customer_churn_data_pipeline.data_preparation.data_preparation import run as prepare_data
from customer_churn_data_pipeline.data_transformation_loader.data_transformation import run as transform_data
from customer_churn_data_pipeline.feature_store.feature_store import run as feature_store
from customer_churn_data_pipeline.data_versioning.data_versioning_gitlfs import run as version_data
from customer_churn_data_pipeline.model_building.model_building import run as train_model

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'customer_churn_pipeline',
    default_args=default_args,
    description='Automated data pipeline for customer churn prediction',
    schedule_interval=timedelta(days=1),
)

# Define tasks
ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

prepare_task = PythonOperator(
    task_id='prepare_data',
    python_callable=prepare_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

feature_store_task = PythonOperator(
    task_id='feature_store',
    python_callable=feature_store,
    dag=dag,
)

version_task = PythonOperator(
    task_id='version_data',
    python_callable=version_data,
    dag=dag,
)

train_task = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    dag=dag,
)

# Define task dependencies
ingest_task >> validate_task >> prepare_task >> transform_task >> feature_store_task >> version_task >> train_task
