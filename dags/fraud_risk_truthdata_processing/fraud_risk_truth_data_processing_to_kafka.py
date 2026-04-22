import logging
from datetime import datetime, timedelta
from typing import Final, Optional
from copy import deepcopy
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowFailException

import util.constants as consts
from util.bq_utils import run_bq_query
from util.miscutils import read_file_env
from util.gcs_utils import cleanup_gcs_folder
from dag_factory.terminus_dag_factory import DAGFactory
from dag_factory.abc import BaseDagBuilder
from dag_factory.environment_config import EnvironmentConfig

logger = logging.getLogger(__name__)
config_file_nm = 'fraud_risk_truthdata_processing_config.yaml'


INITIAL_DEFAULT_ARGS: Final = {
    "owner": "team-growth-and-sales-alerts",
    "capability": "CustomerAcquisition",
    "severity": "P3",
    "sub_capability": "Applications",
    "business_impact": "Fraud Events from New applications or login will not be send as feedback to TMX",
    "customer_impact": "Customers are not categorized as Fraud from TMX",
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}


class FraudRiskTruthDataKafkaProcessorDagBuilder(BaseDagBuilder):
    """
    Fraud Risk Truth Data Kafka Processing DAG Builder

    A specialized DAG builder for processing Fraud Risk Truth Data from BigQuery
    to Kafka. This class orchestrates an end-to-end data pipeline that includes
    evaluating truth data count, exporting to parquet, triggering Kafka writer,
    and updating KafkaPublishStatus.

    ## Purpose
    The FraudRiskTruthDataKafkaProcessorDagBuilder processes Fraud Risk Truth Data
    from BigQuery by exporting pending records to parquet format and publishing
    them to Kafka, then updating the status to COMPLETED.

    ## Data Flow
    BigQuery Table → Evaluate Count → Export to Parquet → Kafka Writer → Update Status
    """

    def __init__(self, environment_config: EnvironmentConfig):
        super().__init__(environment_config)
        self.local_tz = environment_config.local_tz
        self.gcp_config = environment_config.gcp_config
        self.deploy_env = environment_config.deploy_env
        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)

    def clear_extracted_files(self, **context) -> None:
        """Task to clear extracted files from previous runs."""
        try:
            config = context[consts.CONFIG]
            bucket_name = config[consts.DAG][consts.BUCKET]
            folder_name = config[consts.DAG][consts.FOLDER_NAME]
            file_name = config[consts.DAG][consts.FILE_NAME]

            # Resolve {env} placeholder if present
            if '{env}' in bucket_name:
                bucket_name = bucket_name.replace('{env}', self.deploy_env)

            # Ensure folder_name ends with / for proper prefix matching
            folder_prefix = f"{folder_name}/" if not folder_name.endswith('/') else folder_name

            logger.info(f"Clearing extracted files from bucket: {bucket_name}, folder: {folder_prefix}, pattern: {file_name}")
            cleanup_gcs_folder(
                bucket_name=bucket_name,
                folder_prefix=folder_prefix
            )
            logger.info("Successfully cleared extracted files from previous runs")
        except Exception as e:
            logger.error(f"Failed to clear extracted files: {str(e)}")
            raise AirflowFailException(f"Failed to clear extracted files: {str(e)}")

    def export_data_to_parquet_from_bigquery(self, **context) -> None:
        """Task to export truth data from BigQuery to Parquet format."""
        config = context[consts.CONFIG]
        query = self.get_truthdata_sql(config[consts.DAG][consts.QUERY]['fraud_risk_truthdata'], config)
        logger.info("Running export_data_to_parquet_from_bigquery with query:")
        logger.info(query)
        run_bq_query(query).to_dataframe()
        logger.info("Export to parquet completed successfully")

    def update_kafka_publish_status(self, **context) -> Optional[str]:
        """Task to update KafkaPublishStatus from PENDING to COMPLETED."""
        try:
            config = context[consts.CONFIG]
            query = self.get_truthdata_sql(config[consts.DAG][consts.QUERY]['fraud_risk_truthdata_update'], config)
            logger.info("Running update kafka publish status with query:")
            logger.info(query)

            run_bq_query(query)
            logger.info("Successfully updated records from PENDING to COMPLETED")

            return "SUCCESS: Updated records from PENDING to COMPLETED"
        except Exception as e:
            logger.error(f"Failed to update KafkaPublishStatus: {str(e)}")
            raise AirflowFailException(f"Failed to update KafkaPublishStatus: {str(e)}")

    def get_truthdata_sql(self, sql_file_name: str, config: dict) -> str:
        """Read and format SQL file with environment substitution."""
        file_path = f'{settings.DAGS_FOLDER}/fraud_risk_truthdata_processing/sql/{sql_file_name}'
        sql = read_file_env(file_path, self.deploy_env)
        return sql

    def evaluate_truthdata_count(self, **context) -> Optional[str]:
        """Task to evaluate if there are pending truth data records to process."""
        config = context[consts.CONFIG]
        query = self.get_truthdata_sql(config[consts.DAG][consts.QUERY]['fraud_risk_truthdata_count'], config)
        logger.info(f"Running truth data count query: {query}")
        result_df = run_bq_query(query).to_dataframe()
        truthdata_count = result_df['truthdata_count'].iloc[0]
        logger.info(f"Total truth data records Count: {truthdata_count}")
        if truthdata_count > 0:
            return 'export_data_to_parquet_from_bigquery'
        else:
            return 'no_records_task'

    def build(self, dag_id: str, config: dict) -> DAG:
        """Build and return the Fraud Risk Truth Data Kafka processing DAG."""
        # Prepare default args
        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))

        # Create DAG
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            description=config[consts.DAG][consts.DESCRIPTION],
            schedule=config[consts.DAG]['schedule'],
            start_date=datetime(2026, 1, 1, tzinfo=self.local_tz),
            max_active_runs=config[consts.DAG]['max_active_runs'],
            catchup=config[consts.DEFAULT_ARGS]['catchup'],
            is_paused_upon_creation=config[consts.DAG]['is_paused_upon_creation'],
            dagrun_timeout=timedelta(hours=2),
            tags=config[consts.DAG][consts.TAGS],
            render_template_as_native_obj=config[consts.DAG].get('render_template_as_native_obj', True),
        )

        with dag:
            start_task = EmptyOperator(
                task_id=consts.START_TASK_ID,
            )

            clear_previous_extracted_files_task = PythonOperator(
                task_id='clear_previous_extracted_files',
                python_callable=self.clear_extracted_files,
                op_kwargs={consts.CONFIG: config}
            )

            evaluate_truthdata_count_task = BranchPythonOperator(
                task_id='evaluate_truthdata_count',
                python_callable=self.evaluate_truthdata_count,
                op_kwargs={consts.CONFIG: config}
            )

            export_data_to_parquet_from_bigquery_task = PythonOperator(
                task_id='export_data_to_parquet_from_bigquery',
                python_callable=self.export_data_to_parquet_from_bigquery,
                op_kwargs={consts.CONFIG: config}
            )

            no_records_task = PythonOperator(
                task_id='no_records_task',
                python_callable=lambda: logger.info("No truth data records found"),
            )

            truthdata_kafka_writer_dag = TriggerDagRunOperator(
                task_id=config[consts.DAG][consts.TASK_ID][consts.KAFKA_WRITER_TASK_ID],
                trigger_dag_id=config[consts.DAG][consts.DAG_ID][consts.KAFKA_TRIGGER_DAG_ID],
                conf={
                    consts.BUCKET: config[consts.DAG][consts.BUCKET],
                    consts.FOLDER_NAME: config[consts.DAG][consts.FOLDER_NAME],
                    consts.FILE_NAME: config[consts.DAG][consts.FILE_NAME],
                },
                wait_for_completion=config[consts.DAG][consts.WAIT_FOR_COMPLETION],
                poke_interval=config[consts.DAG][consts.POKE_INTERVAL],
                logical_date=datetime.now(tz=self.local_tz)
            )

            update_kafka_publish_status_task = PythonOperator(
                task_id='update_kafka_publish_status',
                python_callable=self.update_kafka_publish_status,
                op_kwargs={consts.CONFIG: config}
            )

            end_task = EmptyOperator(
                task_id=consts.END_TASK_ID,
                trigger_rule=TriggerRule.NONE_FAILED
            )

            # Define task dependencies
            start_task >> clear_previous_extracted_files_task >> evaluate_truthdata_count_task
            evaluate_truthdata_count_task >> export_data_to_parquet_from_bigquery_task >> truthdata_kafka_writer_dag >> update_kafka_publish_status_task >> end_task
            evaluate_truthdata_count_task >> no_records_task >> end_task

        return dag


# Create DAGs using the factory
globals().update(DAGFactory().create_dynamic_dags(
    FraudRiskTruthDataKafkaProcessorDagBuilder,
    config_filename=config_file_nm,
    config_dir=f'{settings.DAGS_FOLDER}/fraud_risk_truthdata_processing/config'
))
