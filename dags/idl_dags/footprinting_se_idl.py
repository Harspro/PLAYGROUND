from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from util.miscutils import read_variable_or_file
import util.constants as consts
from dag_factory.terminus_dag_factory import add_tags

#############################################
# IMPORT CONSTANTS
#############################################

dag_config_file = 'idl_dags/footprinting_se_idl_config'
gcp_config = read_variable_or_file('gcp_config')
deploy_env_name = gcp_config['deployment_environment_name']
dag_config = read_variable_or_file(dag_config_file, deploy_env_name)

BQ_CONN_ID = 'google_cloud_default'
BQ_PROJECT = dag_config['tgt_bq_project_id']
BQ_DATASET = dag_config['tgt_bq_dataset_id']
BQ_TABLE = dag_config['tgt_bq_table_id']
GCS_BUCKET = dag_config['src_gcs_bucket']
GCS_FILE_PAT = dag_config['src_obj_pattern']
DAG_ID = 'footprinting-se-idl'

default_args = {
    'owner': 'team-money-movement-eng',
    'capability': 'TBD',
    'severity': 'P3',
    'sub_capability': 'TBD',
    'business_impact': 'TBD',
    'customer_impact': 'TBD',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email': ['denis.solomin@pcbank.ca'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


def create_dag(dag_id):
    with DAG(
            dag_id,
            schedule=None,
            catchup=False,
            max_active_runs=1,
            is_paused_upon_creation=True,
            default_args=default_args
    ) as dag:

        ingestion_task_start = BashOperator(
            task_id='ingestion_task_start',
            bash_command='echo StartIngestion'
        )

        ingestion_task = GCSToBigQueryOperator(
            task_id='ingestion_task',
            bucket=GCS_BUCKET,
            source_objects=[GCS_FILE_PAT],
            source_format='CSV',
            schema_fields=[
                {'name': 'EVENT_TYPE', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'TIMESTAMP', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'REQUEST_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'ORGANIZATION_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'USER_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'QUERY_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'NUM_RESULTS', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'SEARCH_QUERY', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'PREFIXES_SEARCHED', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'TIMESTAMP_DERIVED', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            ],
            skip_leading_rows=1,
            destination_project_dataset_table=BQ_PROJECT + '.' + BQ_DATASET + '.' + BQ_TABLE,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_APPEND',
            location=gcp_config.get(consts.BQ_QUERY_LOCATION)
        )

        ingestion_task_complete = BashOperator(
            task_id='ingestion_task_complete',
            bash_command='echo FinishIngestion'
        )

        ingestion_task_start >> ingestion_task >> ingestion_task_complete
    return add_tags(dag)

# Uncomment the following line to make the DAG work again
# globals()[DAG_ID] = create_dag(DAG_ID)
