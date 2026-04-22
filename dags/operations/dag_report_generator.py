from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import DagModel
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.settings import Session
from google.cloud import bigquery

from dag_factory.abc import BaseDagBuilder
from dag_factory import DAGFactory

import util.constants as consts
from util.datetime_utils import TORONTO_TZ
from util.gcs_utils import write_file_by_uri
from util.miscutils import read_variable_or_file

"""
  Generate a report of dags which records whether each dag is paused or not
"""

DAG_DEFAULT_ARGS = {
    "owner": "team-centaurs",
    'capability': 'Terminus Data Platform',
    'severity': 'P3',
    'sub_capability': 'Terminus Data Platform',
    'business_impact': 'NA',
    'customer_impact': 'NA',
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True
}


def collect_dag_metadata(**context):
    session = Session()
    dags = session.query(DagModel).all()
    dag_metadata = []
    timestamp = datetime.now(TORONTO_TZ).strftime('%Y-%m-%dT%H:%M:%S.%f')
    for d in dags:
        dag_metadata.append(f'{d.dag_id},{d.is_paused},{timestamp}')

    gcp_config = read_variable_or_file(consts.GCP_CONFIG)
    deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

    dag_metadata_file = f'gs://pcb-{deploy_env}-staging-extract/operations/dag_metadata.csv'

    write_file_by_uri(dag_metadata_file, '\n'.join(dag_metadata))

    context['ti'].xcom_push(key='deploy_env', value=deploy_env)
    context['ti'].xcom_push(key='dag_metadata_file', value=dag_metadata_file)


def load_metadata_into_bq(deploy_env: str, dag_metadata_file: str):
    processing_table_id = f'pcb-{deploy_env}-processing.domain_technical.DAG_METADATA'
    ext_table_ddl = f"""
            CREATE OR REPLACE EXTERNAL TABLE
                `{processing_table_id}`
            (
                DAG_ID STRING NOT NULL,
                IS_PAUSED BOOLEAN NOT NULL,
                REC_LOAD_TIMESTAMP DATETIME NOT NULL
            ) OPTIONS (
                format = 'CSV',
                uris = ['{dag_metadata_file}'],
                expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
            );
        """

    bq_client = bigquery.Client()
    bq_client.query(ext_table_ddl).result()

    columns = 'DAG_ID, IS_PAUSED, REC_LOAD_TIMESTAMP'
    landing_table_id = f'pcb-{deploy_env}-landing.domain_technical.DAG_METADATA'

    loading_stmt = f"""
                CREATE TABLE IF NOT EXISTS
                `{landing_table_id}`
                PARTITION BY DATE_TRUNC(REC_LOAD_TIMESTAMP, DAY)
            AS
                SELECT {columns}
                FROM {processing_table_id}
                LIMIT 0;

            INSERT INTO `{landing_table_id}`
            SELECT {columns}
            FROM {processing_table_id};
    """

    bq_client.query(loading_stmt).result()


class DAGReportGenerator(BaseDagBuilder):
    def build(self, dag_id: str, config: dict) -> DAG:
        with DAG(dag_id=dag_id,
                 start_date=datetime(2025, 1, 1),
                 schedule=None,
                 max_active_runs=1,
                 catchup=False,
                 is_paused_upon_creation=True,
                 default_args=DAG_DEFAULT_ARGS) as dag:
            start = EmptyOperator(task_id='start')
            end = EmptyOperator(task_id='end')

            collect_dag_metadata_task = PythonOperator(
                task_id="collect_dag_metadata",
                python_callable=collect_dag_metadata
            )

            load_metadata_into_bq_task = PythonOperator(
                task_id="load_metadata_into_bq",
                python_callable=load_metadata_into_bq,
                op_kwargs={
                    'deploy_env': "{{ ti.xcom_pull(task_ids='collect_dag_metadata', key='deploy_env')}}",
                    'dag_metadata_file': "{{ ti.xcom_pull(task_ids='collect_dag_metadata', key='dag_metadata_file')}}"
                }
            )

            start >> collect_dag_metadata_task >> load_metadata_into_bq_task >> end
        return dag


globals().update(DAGFactory().create_dag(DAGReportGenerator, 'dag_report_generator'))
