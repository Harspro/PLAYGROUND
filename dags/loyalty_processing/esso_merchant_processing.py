import pendulum
import util.constants as consts

from airflow import DAG, settings
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from bigquery_db_connector_loader.bigquery_db_connector_base import BigQueryDbConnector

from datetime import datetime, timedelta

from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.miscutils import (
    read_file_env,
    read_variable_or_file,
    read_yamlfile_env_suffix,
    save_job_to_control_table
)
from dag_factory.terminus_dag_factory import add_tags

ESSO_MERCHANT_CONFIG_PATH = f'{settings.DAGS_FOLDER}/{consts.LOYALTY_PROCESSING}/esso_merchant_config.yaml'


class EssoMerchantProcessingDagsBuilder:
    def __init__(self, config_filename: str):
        self.tzinfo = pendulum.timezone('America/Toronto')

        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.deploy_env_storage_suffix = self.gcp_config[consts.DEPLOY_ENV_STORAGE_SUFFIX]
        self.dag_config = read_yamlfile_env_suffix(config_filename, self.deploy_env, self.deploy_env_storage_suffix)

        self.staging_bucket = f'pcb-{self.deploy_env}-staging'

        self.default_args = {
            'owner': 'team-convergence-alerts',
            'capability': 'Loyalty',
            'severity': 'P2',
            'sub_capability': 'Rewards',
            'business_impact': (
                'This will impact the loading of ESSO merchants. The merchant list is used to block all '
                'ESSO transactions from getting the points.'
            ),
            'customer_impact': (
                'If ESSO adds a new store and we receive transactions, the PC points will be awarded '
                '(Costing implication: Customer gets extra points).'
            ),
            'depends_on_past': False,
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "max_active_runs": 1
        }

    def save_job_rec_to_control_table(self, dags_triggered: str, **context):
        job_params = dags_triggered
        save_job_to_control_table(job_params, **context)

    def create_dag(self, dag_id: str, config: dict) -> DAG:

        with DAG(
                dag_id=dag_id,
                default_args=self.default_args,
                description=(
                    'DAG to update tables DB610.PCL.MERCHANT_CODE and DB610.PCL.CAMPAIGN_MERCHANT with new '
                    'merchants based on daily ESSO gas transaction file received. Additionally, ESSO merchants '
                    'are published to DP Kafka.'
                ),
                render_template_as_native_obj=True,
                schedule=None,
                start_date=pendulum.today('America/Toronto').add(days=-1),
                catchup=False,
                dagrun_timeout=timedelta(hours=24),
                tags=config[consts.TAGS],
                is_paused_upon_creation=True
        ) as dag:

            if config.get(consts.BATCH) and config[consts.BATCH].get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(dag_id, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            start = EmptyOperator(task_id=consts.START_TASK_ID)

            outbound_landing_bucket = config[consts.DAG][consts.BUCKET_ID].get(consts.OUTBOUND_LANDING_BUCKET)

            # Task: Delete GCS Landing Fiserv Response File Object.
            delete_gcs_landing_response_obj = GCSDeleteObjectsOperator(
                task_id=config[consts.DAG][consts.TASK_ID].get(consts.DELETE_GCS_LANDING_RESPONSE_OBJ_TASK_ID),
                bucket_name=outbound_landing_bucket,
                prefix=config[consts.DAG][consts.OBJ_PREFIX].get(
                    f"{consts.QUERY_FILE}_{consts.LANDING_OBJ_PREFIX}"
                )
            )

            # Task: Trigger esso_merchant_file_loading_job DAG.
            trigger_loading_dag = TriggerDagRunOperator(
                task_id=config[consts.DAG][consts.TASK_ID].get(consts.LOADING_TASK_ID),
                trigger_dag_id=config[consts.DAG][consts.DAG_ID].get(consts.LOADING_DAG_ID),
                conf={
                    consts.BUCKET: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.BUCKET),
                    consts.NAME: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.NAME),
                    consts.FOLDER_NAME: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.FOLDER_NAME),
                    consts.FILE_NAME: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.FILE_NAME)
                },
                wait_for_completion=config[consts.DAG].get(consts.WAIT_FOR_COMPLETION),
                poke_interval=config[consts.DAG].get(consts.POKE_INTERVAL),
                logical_date=datetime.now(tz=self.tzinfo)
            )

            # Task: Trigger padded_esso_merchant_file_loading_job DAG.
            trigger_padded_loading_dag = TriggerDagRunOperator(
                task_id=config[consts.DAG][consts.TASK_ID].get(consts.PADDED_LOADING_TASK_ID),
                trigger_dag_id=config[consts.DAG][consts.DAG_ID].get(consts.PADDED_LOADING_DAG_ID),
                conf={
                    consts.BUCKET: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.BUCKET),
                    consts.NAME: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.NAME),
                    consts.FOLDER_NAME: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.FOLDER_NAME),
                    consts.FILE_NAME: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.FILE_NAME)
                },
                wait_for_completion=config[consts.DAG].get(consts.WAIT_FOR_COMPLETION),
                poke_interval=config[consts.DAG].get(consts.POKE_INTERVAL),
                logical_date=datetime.now(tz=self.tzinfo)
            )

            # Task: Trigger esso_fuel_transactions_file_loading_job DAG.
            trigger_fuel_transactions_loading_dag = TriggerDagRunOperator(
                task_id=config[consts.DAG][consts.TASK_ID].get(consts.FUEL_TRANSACTIONS_TASK_ID),
                trigger_dag_id=config[consts.DAG][consts.DAG_ID].get(consts.FUEL_TRANSACTIONS_DAG_ID),
                conf={
                    consts.BUCKET: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.BUCKET),
                    consts.NAME: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.NAME),
                    consts.FOLDER_NAME: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.FOLDER_NAME),
                    consts.FILE_NAME: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.FILE_NAME)
                },
                wait_for_completion=config[consts.DAG].get(consts.WAIT_FOR_COMPLETION),
                poke_interval=config[consts.DAG].get(consts.POKE_INTERVAL),
                logical_date=datetime.now(tz=self.tzinfo)
            )

            # Task: GCS to GCS task responsible for copying the inbound esso file to processing zone from landing zone.
            gcs_to_gcs_processing = GCSToGCSOperator(
                task_id=config[consts.DAG][consts.TASK_ID].get(
                    f"{consts.GCS_TO_GCS}_{consts.PROCESSING_BUCKET}_{consts.TASK_ID}"
                ),
                gcp_conn_id=self.gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID),
                source_bucket=config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.BUCKET),
                source_object=config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.NAME),
                destination_bucket=self.staging_bucket,
                destination_object=config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.NAME),
            )

            # Task: Delete GCS Landing Object.
            delete_gcs_landing_obj = GCSDeleteObjectsOperator(
                task_id=config[consts.DAG][consts.TASK_ID].get(consts.DELETE_GCS_LANDING_OBJ_TASK_ID),
                bucket_name=config[consts.DAG][consts.BUCKET_ID].get(consts.INBOUND_LANDING_BUCKET),
                prefix=config[consts.DAG][consts.OBJ_PREFIX].get(consts.LANDING_OBJ_PREFIX)
            )

            # Task: Trigger pcl_merchant_code_updating_job DAG.
            trigger_merchant_code_updating_dag = TriggerDagRunOperator(
                task_id=config[consts.DAG][consts.TASK_ID].get(consts.UPDATING_MC_TASK_ID),
                trigger_dag_id=config[consts.DAG][consts.DAG_ID].get(consts.UPDATING_MC_DAG_ID),
                wait_for_completion=config[consts.DAG].get(consts.WAIT_FOR_COMPLETION),
                poke_interval=config[consts.DAG].get(consts.POKE_INTERVAL),
                logical_date=datetime.now(tz=self.tzinfo)
            )

            # Task: Trigger pcl_campaign_merchant_updating_job DAG.
            trigger_campaign_merchant_updating_dag = TriggerDagRunOperator(
                task_id=config[consts.DAG][consts.TASK_ID].get(consts.UPDATING_CM_TASK_ID),
                trigger_dag_id=config[consts.DAG][consts.DAG_ID].get(consts.UPDATING_CM_DAG_ID),
                wait_for_completion=config[consts.DAG].get(consts.WAIT_FOR_COMPLETION),
                poke_interval=config[consts.DAG].get(consts.POKE_INTERVAL),
                logical_date=datetime.now(tz=self.tzinfo)
            )

            # Task: Trigger ESSO Merchant Kafka Writer.
            trigger_esso_merchant_kafka_writer_dag = TriggerDagRunOperator(
                task_id=config[consts.DAG][consts.TASK_ID].get(consts.KAFKA_WRITER_TASK_ID),
                trigger_dag_id=config[consts.DAG][consts.DAG_ID].get(consts.KAFKA_TRIGGER_DAG_ID),
                conf={
                    consts.BUCKET: self.staging_bucket,
                    consts.NAME: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.NAME),
                    consts.FOLDER_NAME: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.FOLDER_NAME),
                    consts.FILE_NAME: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.FILE_NAME),
                    consts.CLUSTER_NAME: config[consts.DAG][consts.TRIGGER_CONFIG].get(consts.CLUSTER_NAME)
                },
                wait_for_completion=config[consts.DAG].get(consts.WAIT_FOR_COMPLETION),
                poke_interval=config[consts.DAG].get(consts.POKE_INTERVAL),
                logical_date=datetime.now(tz=self.tzinfo)
            )

            # Task: Delete GCS Processing Object.
            delete_gcs_processing_obj = GCSDeleteObjectsOperator(
                task_id=config[consts.DAG][consts.TASK_ID].get(consts.DELETE_GCS_PROCESSING_OBJ_TASK_ID),
                bucket_name=self.staging_bucket,
                prefix=config[consts.DAG][consts.OBJ_PREFIX].get(consts.PROCESSING_OBJ_PREFIX)
            )

            processing_bucket_extract = config[consts.DAG][consts.BUCKET_ID].get(consts.PROCESSING_BUCKET_EXTRACT)
            output_uri_processing = config[consts.DAG][consts.BIGQUERY].get(consts.OUTPUT_URI_PROCESSING).replace(
                f"{consts.CURRENT_DATE_PLACEHOLDER}", f"{datetime.now(tz=self.tzinfo).strftime('%Y%m%d%H%M%S')}"
            )

            query_file = f"{settings.DAGS_FOLDER}/{config[consts.DAG][consts.BIGQUERY].get(consts.QUERY_FILE)}"
            replacements = {
                f"{{{consts.OUTPUT_URI_PROCESSING}}}": f"gs://{processing_bucket_extract}/{output_uri_processing}"
            }

            # Query to Use When Generating Fiserv Response File.
            response_file_generation = PythonOperator(
                task_id=config[consts.DAG][consts.TASK_ID].get(consts.RESPONSE_FILE_GENERATION_TASK_ID),
                python_callable=BigQueryDbConnector.run_bigquery_staging_query,
                op_kwargs={consts.QUERY_FILE: query_file,
                           consts.DEPLOY_ENV: self.deploy_env,
                           consts.REPLACEMENTS: replacements}
            )

            output_uri_landing = config[consts.DAG][consts.BIGQUERY].get(consts.OUTPUT_URI_LANDING).replace(
                f"{consts.CURRENT_DATE_PLACEHOLDER}", f"{datetime.now(tz=self.tzinfo).strftime('%Y%m%d%H%M%S')}"
            )

            # Task: GCS to GCS Task Responsible for Copying the Outbound ESSO File to Landing Zone From Processing Zone.
            gcs_to_gcs_outbound_landing = GCSToGCSOperator(
                task_id=config[consts.DAG][consts.TASK_ID].get(
                    f"{consts.GCS_TO_GCS}_{consts.OUTBOUND_LANDING_BUCKET}_{consts.TASK_ID}"
                ),
                gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
                source_bucket=processing_bucket_extract,
                source_object=config[consts.DAG][consts.OBJ_PREFIX].get(
                    f"{consts.QUERY_FILE}_{consts.PROCESSING_OBJ_PREFIX}"
                ),
                destination_bucket=outbound_landing_bucket,
                destination_object=output_uri_landing.replace(
                    f"{{{consts.DEPLOYMENT_ENVIRONMENT_NAME}}}", f"{self.deploy_env}"
                )

            )

            # Task: Delete GCS Processing Object Created by Query.
            delete_gcs_processing_query_obj = GCSDeleteObjectsOperator(
                task_id=config[consts.DAG][consts.TASK_ID].get(consts.DELETE_GCS_PROCESSING_QUERY_OBJ_TASK_ID),
                bucket_name=processing_bucket_extract,
                prefix=config[consts.DAG][consts.OBJ_PREFIX].get(
                    f"{consts.QUERY_FILE}_{consts.PROCESSING_OBJ_PREFIX}"
                )
            )

            save_job_to_control_table = PythonOperator(
                task_id=consts.SAVE_JOB_TO_CONTROL_TABLE,
                python_callable=self.save_job_rec_to_control_table,
                op_kwargs={"dags_triggered": f"{config[consts.DAG][consts.DAG_ID].get(consts.LOADING_DAG_ID)}, "
                                             f"{config[consts.DAG][consts.DAG_ID].get(consts.PADDED_LOADING_DAG_ID)}, "
                                             f"{config[consts.DAG][consts.DAG_ID].get(consts.FUEL_TRANSACTIONS_DAG_ID)}, "
                                             f"{config[consts.DAG][consts.DAG_ID].get(consts.UPDATING_MC_DAG_ID)}, "
                                             f"{config[consts.DAG][consts.DAG_ID].get(consts.UPDATING_CM_DAG_ID)}, "
                                             f"{config[consts.DAG][consts.DAG_ID].get(consts.KAFKA_TRIGGER_DAG_ID)}"},
                dag=dag
            )

            end = EmptyOperator(task_id=consts.END_TASK_ID)

            start >> delete_gcs_landing_response_obj >> trigger_loading_dag >> trigger_padded_loading_dag \
                >> trigger_fuel_transactions_loading_dag >> gcs_to_gcs_processing >> delete_gcs_landing_obj \
                >> trigger_merchant_code_updating_dag >> trigger_campaign_merchant_updating_dag \
                >> trigger_esso_merchant_kafka_writer_dag >> delete_gcs_processing_obj >> response_file_generation \
                >> gcs_to_gcs_outbound_landing >> delete_gcs_processing_query_obj >> save_job_to_control_table >> end

            return add_tags(dag)

    def create_all_dags(self) -> dict:
        dags = {}
        for dag_id, config in self.dag_config.items():
            dags[dag_id] = self.create_dag(dag_id, config)

        return dags


esso_merchant_processing_dags_builder = EssoMerchantProcessingDagsBuilder(ESSO_MERCHANT_CONFIG_PATH)
globals().update(esso_merchant_processing_dags_builder.create_all_dags())
