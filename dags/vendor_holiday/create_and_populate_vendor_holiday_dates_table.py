from datetime import datetime
from typing import Final

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

from util.constants import GCP_CONFIG, DEPLOYMENT_ENVIRONMENT_NAME, DOMAIN_TECHNICAL_DATASET_ID, WRITE_TRUNCATE
from util.miscutils import read_variable_or_file, read_file_env
from vendor_holiday.util.constants import VENDOR_HOLIDAY_DATES_TABLE_DDL_PATH, VENDOR_HOLIDAY_DATES_CSV_PATH, \
    VENDOR_HOLIDAY_DATES_TABLE_NAME
from dag_factory.terminus_dag_factory import add_tags

DEFAULT_ARGS: Final = {
    'owner': 'team-centaurs',
    'capability': 'Terminus Data Platform',
    'severity': 'P3',
    'sub_capability': 'Data Movement',
    'business_impact': 'Loading vendor Holiday dates to BQ',
    'customer_impact': 'N/A',
    'depends_on_past': False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False
}


def create_vendor_holiday_dates_table_if_not_exists():
    """
    This function creates the vendor holiday dates table if it does not exist already.
    """
    env = read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME]
    bigquery.Client().query(read_file_env(VENDOR_HOLIDAY_DATES_TABLE_DDL_PATH, env)).result()


def populate_vendor_holidays_table():
    """
   This function populates the vendor holiday dates table with the values from the csv located in the resources folder
   in this module. The function drops the table if it already exists and adds it back with the values in the csv. You
   should run this DAG again whenever you add new rows to the csv file.
   """
    bq_client = bigquery.Client()
    env = read_variable_or_file(GCP_CONFIG)[DEPLOYMENT_ENVIRONMENT_NAME]
    vendor_holidays_table_ref = f"pcb-{env}-landing.{DOMAIN_TECHNICAL_DATASET_ID}.{VENDOR_HOLIDAY_DATES_TABLE_NAME}"
    with open(VENDOR_HOLIDAY_DATES_CSV_PATH, 'rb') as source_file:
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = 'text/csv'
        job_config.write_disposition = WRITE_TRUNCATE
        job_config.skip_leading_rows = 1
        job = bq_client.load_table_from_file(
            source_file,
            vendor_holidays_table_ref,
            job_config=job_config)
    job.result()


dag = DAG(
    dag_id='create_and_populate_vendor_holiday_dates_table',
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=pendulum.timezone('America/Toronto')),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True
)

with dag:
    add_tags(dag)
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule='none_failed')

    create_vendor_holiday_dates_table_if_not_exists_task = PythonOperator(
        task_id="create_vendor_holiday_dates_table_if_not_exists",
        python_callable=create_vendor_holiday_dates_table_if_not_exists
    )

    populate_vendor_holidays_table_task = PythonOperator(
        task_id="populate_vendor_holidays_table",
        python_callable=populate_vendor_holidays_table
    )

    start >> create_vendor_holiday_dates_table_if_not_exists_task >> populate_vendor_holidays_table_task >> end
    start >> create_vendor_holiday_dates_table_if_not_exists_task >> populate_vendor_holidays_table_task >> end
