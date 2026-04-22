import logging

from datetime import datetime
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator, BranchPythonOperator
from aml_processing.transaction import transaction_feed_tasks as tasks
from util.bq_utils import create_or_replace_table
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env_suffix
)
from typing import Final
from aml_processing import feed_commons as commons
import util.constants as consts
from dag_factory.terminus_dag_factory import add_tags

START_DATE = datetime(2022, 1, 1, tzinfo=commons.TORONTO_TZ)
TAGS = commons.TAGS.copy()
TBL_CURATED_TRANSACTION_FEED_SCHEMA = f"{settings.DAGS_FOLDER}/aml_processing/transaction/TRANSACTION_FEED_SCHEMA.json"
TBL_CURATED_TRANSACTION_FEED_HISTORICAL_SCHEMA = f"{settings.DAGS_FOLDER}/aml_processing/transaction/TRANSACTION_FEED_HISTORICAL_SCHEMA.json"
AML_TRANSACTION_FEED: Final = 'aml_transaction_feed'


class TransactionFeedExtractor:

    def __init__(self, config_filename: str, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.deploy_env_suffix = self.gcp_config['deploy_env_storage_suffix']
        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/aml_processing/transaction'

        self.config_dir = config_dir
        self.feed_config = read_yamlfile_env_suffix(f'{self.config_dir}/{config_filename}', self.deploy_env, self.deploy_env_suffix)
        logging.info(f"The feed config : {self.feed_config}")
        self.default_args = {
            'owner': 'team-defenders-alerts',
            'capability': 'risk-management',
            'severity': 'P2',
            'sub_capability': 'aml',
            'business_impact': 'Posted transactions needed for AML analysis will be missing in Verafin if this process fails',
            'customer_impact': 'None',
            'depends_on_past': False,
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False
        }

    def create_dag(self, dag_id: str, init_config: dict) -> DAG:
        config = tasks.read_dynamic_transaction_feed_config(init_config)

        source_headers_file_path = "aml_processing/csv_headers_files/transaction_headers.csv.gz"
        destination_headers_file_path = f"aml/transaction/{config[tasks.FEED_NAME]}/transaction_headers.csv.gz"

        logging.info(f"the config is {config}")

        transaction_tags = self.default_args['severity']
        TAGS.append(transaction_tags)
        dag = DAG(
            dag_id=dag_id,
            description=f"DAG to orchestrate the AML transaction {config[tasks.FEED_NAME]} Feed.",
            default_args=self.default_args,
            schedule=config[commons.SCHEDULING_INTERVAL],
            start_date=START_DATE,
            tags=TAGS,
            params={
                "start_time": "",
                "end_time": ""
            },
            max_active_runs=1,
            is_paused_upon_creation=True,
            catchup=False
        )

        with dag:
            if config[commons.PAUSE_DAG_BY_CONFIG] is True:
                IS_PAUSED = read_pause_unpause_setting(AML_TRANSACTION_FEED, commons.DEPLOY_ENV)
                pause_unpause_dag(dag, IS_PAUSED)

            pipeline_start = EmptyOperator(task_id='start')

            create_curated_transaction_feed_table_task = PythonOperator(
                task_id="create_curated_transaction_feed_table_task",
                python_callable=create_or_replace_table,
                op_args=[config[commons.DESTINATION_BQ_TABLE_WITH_DATASET], TBL_CURATED_TRANSACTION_FEED_SCHEMA],
                trigger_rule=TriggerRule.ALL_SUCCESS,
                dag=dag
            )

            create_curated_transaction_feed_historical_records_table_task = PythonOperator(
                task_id="create_curated_transaction_feed_historical_records_table_task",
                python_callable=tasks.create_curated_transaction_feed_historical_records_table,
                op_args=[TBL_CURATED_TRANSACTION_FEED_HISTORICAL_SCHEMA],
                trigger_rule=TriggerRule.ALL_SUCCESS,
                dag=dag
            )

            process_transaction_feed_task = PythonOperator(
                task_id="process_transaction_feed",
                python_callable=tasks.process_transaction_feed_task,
                op_args=[config[commons.FEED_NAME], config[commons.DESTINATION_BQ_TABLE_WITH_DATASET]],
                trigger_rule=TriggerRule.ALL_SUCCESS,
                dag=dag
            )

            export_table_to_gcs = PythonOperator(
                task_id="export_table_to_gcs",
                python_callable=commons.export_bq_table,
                op_args=[config[commons.STAGING_BUCKET], config[commons.BQ_TO_GCS_FOLDER],
                         config[commons.DESTINATION_BQ_DATASET], config[commons.BQ_TABLE_NAME], False, True],
                trigger_rule=TriggerRule.NONE_FAILED
            )

            compose_feed_file = PythonOperator(
                task_id="compose_feed_file",
                python_callable=commons.compose_infinite_files_into_one,
                op_args=[config[commons.STAGING_BUCKET], config[commons.STAGING_FOLDER],
                         source_headers_file_path, destination_headers_file_path,
                         config[commons.BQ_TO_GCS_FOLDER], config[commons.BQ_TABLE_NAME]],
                trigger_rule=TriggerRule.ALL_SUCCESS
            )

            copy_object_to_outbound_bucket_task = PythonOperator(
                task_id="copy_object_to_outbound_bucket_task",
                python_callable=commons.copy_object,
                op_args=[config[commons.STAGING_BUCKET], config[commons.STAGING_FOLDER],
                         config[commons.OUTBOUND_BUCKET], config[commons.OUTBOUND_FILENAME]],
                trigger_rule=TriggerRule.ALL_SUCCESS
            )

            insert_into_curated_transaction_feed_historical_records_table_task = PythonOperator(
                task_id="insert_into_curated_transaction_feed_historical_records_table_task",
                python_callable=tasks.insert_into_curated_transaction_feed_historical_records_table,
                op_args=[config[commons.FEED_NAME], config[commons.DESTINATION_BQ_TABLE_WITH_DATASET]],
                trigger_rule=TriggerRule.ALL_SUCCESS
            )

            control_table_update = PythonOperator(
                task_id="control_table_update",
                python_callable=tasks.control_table_update,
                op_args=[config[commons.FEED_NAME]],
                trigger_rule=TriggerRule.ALL_DONE,
                dag=dag
            )

            final_task = PythonOperator(
                task_id="final_task",
                python_callable=commons.final_task,
                trigger_rule=TriggerRule.ALL_DONE,
                dag=dag
            )

            pipeline_start >> create_curated_transaction_feed_table_task >> \
                create_curated_transaction_feed_historical_records_table_task >> \
                process_transaction_feed_task >> \
                export_table_to_gcs >> \
                compose_feed_file >> \
                copy_object_to_outbound_bucket_task >> \
                insert_into_curated_transaction_feed_historical_records_table_task >> \
                control_table_update >> \
                final_task

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        for job_id, config in self.feed_config.items():
            logging.info(f"The job id is {job_id} and the config is {config}")
            dags[job_id] = self.create_dag(job_id, config)

        return dags
