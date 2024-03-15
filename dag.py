from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from extract import fetch_and_upload_league_leaders, transform_data


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'nba_league_leaders_etl',
    default_args=default_args,
    description='Fetch NBA league leaders data, transform, and upload to GCS',
    schedule_interval='@daily',
    catchup=False,
)


def extract_task(**kwargs):
    """Wrapper for fetch_and_upload_league_leaders to fit Airflow's requirements."""
    ds_nodash = kwargs['ds_nodash']
    bucket_name = 'league-leaders'  # Update this to your GCS bucket name
    fetch_and_upload_league_leaders(bucket_name, ds_nodash)

def transform_task(**kwargs):
    """Wrapper for transform_data to fit Airflow's requirements."""
    ds_nodash = kwargs['ds_nodash']
    bucket_name = 'league-leaders'  # Update this to your GCS bucket name
    source_blob_name = f'raw/{ds_nodash}_data.csv'
    transform_data(bucket_name, source_blob_name)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    provide_context=True,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)

extract >> transform
