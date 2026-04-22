import pendulum
import util.constants as consts

from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from datetime import datetime, timedelta
from typing import Final
from util.miscutils import read_variable_or_file, read_yamlfile_env
from dag_factory.terminus_dag_factory import add_tags

ESTATEMENT_READY_CONFIG_FILE: Final = 'estatement_ready_config.yaml'
DAG_ID: Final = 'estatement_ready_launcher'

local_tz = pendulum.timezone('America/Toronto')

gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env_name = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
estatement_ready_config = read_yamlfile_env(f'{settings.DAGS_FOLDER}/digital_adoption/{ESTATEMENT_READY_CONFIG_FILE}', deploy_env_name)

DAG_DEFAULT_ARGS = {
    'owner': 'team-digital-adoption-alerts',
    'capability': 'money-movement"',
    'sub_capability': 'EMT',
    'business_impact': 'This dag triggers the e-statement_ready_kafka_writer dag to write e-statements to kafka',
    'customer_impact': 'In the event of a failure, e-statement_ready_kafka_writer dag triggering failed',
    'severity': 'P2',
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 3,
    'retry_delay': timedelta(seconds=10)
}

with DAG(dag_id=DAG_ID,
         default_args=DAG_DEFAULT_ARGS,
         schedule=None,
         description='DAG to load the E-Statements file to DP Kafka',
         render_template_as_native_obj=True,
         start_date=datetime(2023, 1, 1, tzinfo=local_tz),
         max_active_runs=1,
         catchup=False,
         dagrun_timeout=timedelta(hours=24),
         is_paused_upon_creation=True
         ) as dag:

    add_tags(dag)

    start_point = EmptyOperator(task_id=consts.START_TASK_ID)
    end_point = EmptyOperator(task_id=consts.END_TASK_ID)

    gcs_to_gcs_task = GCSToGCSOperator(
        task_id=consts.GCS_TO_GCS,
        gcp_conn_id=gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID),
        source_bucket="{{ dag_run.conf['bucket'] }}",
        source_object="{{ dag_run.conf['name'] }}",
        destination_bucket=estatement_ready_config.get(consts.STAGING_BUCKET),
        destination_object="{{ dag_run.conf['name']}}",
    )

    kafka_writer_task = TriggerDagRunOperator(
        task_id=consts.KAFKA_WRITER_TASK,
        trigger_dag_id=estatement_ready_config.get(consts.KAFKA_TRIGGER_DAG_ID),
        logical_date=datetime.now(local_tz),
        conf={
            consts.BUCKET: estatement_ready_config.get(consts.STAGING_BUCKET),
            consts.NAME: "{{ dag_run.conf['name'] }}",
            consts.FOLDER_NAME: "{{ dag_run.conf['folder_name'] }}",
            consts.FILE_NAME: "{{ dag_run.conf['file_name'] }}",
            consts.CLUSTER_NAME: estatement_ready_config.get(consts.KAFKA_CLUSTER_NAME)
        },
        wait_for_completion=True,
        poke_interval=30
    )

    start_point >> gcs_to_gcs_task >> kafka_writer_task >> end_point
