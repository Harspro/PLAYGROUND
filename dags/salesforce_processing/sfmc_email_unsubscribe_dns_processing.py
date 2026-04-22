from copy import deepcopy
from datetime import datetime, timedelta
from typing import Final

import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

import util.constants as consts

from airflow import DAG, settings

from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env
)
from dag_factory.terminus_dag_factory import add_tags

DNS_CONFIG_FILE: Final = 'sfmc_email_unsubscribe_dns_config.yaml'

DNS_CONFIG_PATH = f'{settings.DAGS_FOLDER}/salesforce_processing/{DNS_CONFIG_FILE}'

INITIAL_DEFAULT_ARGS: Final = {
    'owner': 'TBD',
    'capability': 'TBD',
    'severity': 'P3',
    'sub_capability': 'TBD',
    'business_impact': 'TBD',
    'customer_impact': 'TBD',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False
}


class SfmcEmailUnsubscribeDagBuilder:
    """
        This class processes inbound file from Salesforce Marketing Cloud that contains email unsubscribe DNS data.
        Data in the file is loaded into BigQuery and sent as events to Kafka.
    """
    def __init__(self, config_filename: str):
        self.local_tz = pendulum.timezone('America/Toronto')

        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dag_config = read_yamlfile_env(config_filename, self.deploy_env)

        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        """
            Defines structure for the dynamically created DAGs, made of 5 tasks:
            1. Empty Operator start point.
            2. Trigger DAG Run Operator, triggers another DAG in Airflow - trigger file loading DAG.
            3. GCS to GCS Operator, copies an item from one GCS bucket to another - copy inbound file from landing to processing zone.
            4. Trigger DAG Run Operator, triggers another DAG in Airflow - trigger kafka writer DAG.
            5. Empty Operator end point.

            Args:
                dag_id (str): unique identifier to be used.
                config (dict): job configuration provided.
        """

        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))

        with DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            description=(
                'DAG to update table domain_dns.SFMC_EMAIL_UNSUBSCRIBE_DNS with DNS data '
                'from salesforce. Additionally, DNS records are published to DP Kafka.'
            ),
            render_template_as_native_obj=True,
            schedule=None,
            start_date=datetime(2024, 9, 1, tzinfo=self.local_tz),
            max_active_runs=1,
            catchup=False,
            dagrun_timeout=timedelta(hours=24),
            tags=config[consts.TAGS],
            is_paused_upon_creation=True
        ) as dag:

            dag_config = config.get(consts.DAG)
            trigger_config = dag_config.get(consts.TRIGGER_CONFIG)

            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(dag_id, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            start = EmptyOperator(task_id=consts.START_TASK_ID)

            file_load_config = dag_config.get('file_load_config')
            # Task: Trigger sfmc_email_unsubscribe_file_loading_job DAG.
            trigger_file_loading_dag = TriggerDagRunOperator(
                task_id=file_load_config.get(consts.LOADING_TASK_ID),
                trigger_dag_id=file_load_config.get(consts.LOADING_DAG_ID),
                logical_date=datetime.now(tz=self.local_tz),
                conf={
                    consts.BUCKET: trigger_config.get(consts.BUCKET),
                    consts.NAME: trigger_config.get(consts.NAME),
                    consts.FOLDER_NAME: trigger_config.get(consts.FOLDER_NAME),
                    consts.FILE_NAME: trigger_config.get(consts.FILE_NAME)
                },
                wait_for_completion=dag_config.get(consts.WAIT_FOR_COMPLETION),
                poke_interval=dag_config.get(consts.POKE_INTERVAL)
            )

            # Task: GCS to GCS task responsible for copying the inbound sfmc file to processing zone from landing zone.
            gcs_to_gcs = GCSToGCSOperator(
                task_id=consts.GCS_TO_GCS,
                gcp_conn_id=self.gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID),
                source_bucket=trigger_config.get(consts.BUCKET),
                source_object=trigger_config.get(consts.NAME),
                destination_bucket=dag_config.get(consts.STAGING_BUCKET),
                destination_object=trigger_config.get(consts.NAME)
            )

            kafka_config = dag_config.get('kafka_config')
            # Task: Trigger sfmc_email_unsubscribe_dns_kafka_writer DAG.
            kafka_writer_task = TriggerDagRunOperator(
                task_id=kafka_config.get(consts.KAFKA_WRITER_TASK_ID),
                trigger_dag_id=kafka_config.get(consts.KAFKA_TRIGGER_DAG_ID),
                logical_date=datetime.now(tz=self.local_tz),
                conf={
                    consts.BUCKET: dag_config.get(consts.STAGING_BUCKET),
                    consts.NAME: trigger_config.get(consts.NAME),
                    consts.FOLDER_NAME: trigger_config.get(consts.FOLDER_NAME),
                    consts.FILE_NAME: trigger_config.get(consts.FILE_NAME),
                    consts.CLUSTER_NAME: kafka_config.get(consts.KAFKA_CLUSTER_NAME)
                },
                wait_for_completion=dag_config.get(consts.WAIT_FOR_COMPLETION),
                poke_interval=dag_config.get(consts.POKE_INTERVAL)
            )

            end = EmptyOperator(task_id=consts.END_TASK_ID)

            start >> trigger_file_loading_dag >> gcs_to_gcs >> kafka_writer_task >> end

            return add_tags(dag)

    def create_all_dags(self) -> dict:
        dags = {}
        for dag_id, config in self.dag_config.items():
            dags[dag_id] = self.create_dag(dag_id, config)

        return dags


sfmc_email_unsubscribe_processing_dags_builder = SfmcEmailUnsubscribeDagBuilder(DNS_CONFIG_PATH)
globals().update(sfmc_email_unsubscribe_processing_dags_builder.create_all_dags())
