from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowFailException
from google.cloud import bigquery
import logging

from util.smb_utils import SMBUtil
import util.constants as consts
from util.miscutils import (
    resolve_smb_server_config,
    get_pnp_env
)
from util.bq_utils import submit_transformation, create_external_table
from dag_factory.terminus_dag_factory import DAGFactory
from dag_factory.abc import BaseDagBuilder
from dag_factory.environment_config import EnvironmentConfig
from application_data_purging.purge_list_exclusion_file_validation import PurgeListExclusionFileValidation

logger = logging.getLogger(__name__)
config_file_nm = 'purgelist_exclusion_ctera_to_gcs_file_copy_config.yaml'


class PurgelistExclusionDagBuilder(BaseDagBuilder):
    def __init__(self, environment_config: EnvironmentConfig):
        super().__init__(environment_config)
        self.local_tz = environment_config.local_tz
        self.gcp_config = environment_config.gcp_config
        self.deploy_env = environment_config.deploy_env
        self.runtime_env = get_pnp_env(self.deploy_env)

    def build(self, dag_id: str, config: dict) -> DAG:
        """Build the DAG using the provided configuration"""
        default_args = {
            'owner': 'team-growth-and-sales-alerts',
            'capability': 'CustomerAcquisition',
            'severity': 'P3',
            'sub_capability': 'Applications',
            'business_impact': 'Compliance Issue if the data is not purged',
            'customer_impact': 'None',
            'depends_on_past': False,
            "email": [],
            "retries": 0,
            "email_on_failure": False,
            "email_on_retry": False
        }

        with DAG(
                dag_id=dag_id,
                default_args=default_args,
                start_date=datetime(2025, 8, 1, tzinfo=self.local_tz),
                dagrun_timeout=timedelta(minutes=15),
                schedule=None,
                max_active_tasks=3,
                max_active_runs=1,
                catchup=False,
                is_paused_upon_creation=True,
                tags=config.get(consts.TAGS, [])
        ) as dag:
            start_point = EmptyOperator(task_id='start')
            end_point = EmptyOperator(task_id='end')

            purgelist_file_copy = PythonOperator(
                task_id="ctera_to_gcs_file_copy",
                python_callable=self._ctera_to_gcs_file_copy_task,
                op_kwargs={'config': config}
            )

            # Add BigQuery validation task
            file_data_validation = PythonOperator(
                task_id="file_validation",
                python_callable=self._file_data_validation_task,
                op_kwargs={'config': config}
            )

            purgelist_load_task = TriggerDagRunOperator(
                task_id=config[consts.DAG][consts.TASK_ID].get('purgelist_load_task_id'),
                trigger_dag_id=config[consts.DAG][consts.DAG_ID].get('purgelist_exclusion_dag_id'),
                wait_for_completion=config[consts.DAG].get('wait_for_completion'),
                poke_interval=config[consts.DAG].get('poke_interval'),
                logical_date=datetime.now(tz=self.local_tz)
            )

            start_point >> purgelist_file_copy >> file_data_validation >> purgelist_load_task >> end_point

        return dag

    def _ctera_to_gcs_file_copy_task(self, **context):
        """Task to copy file from CTERA to GCS"""
        config = context['config']

        ctera_shared_folder = config[self.runtime_env]['ctera_shared_folder']
        file_name = config[self.runtime_env][consts.FILE_NAME]

        logger.info(f"{file_name} file copy is started!")

        bucket = config['gcs_bucket'].replace('{env}', self.deploy_env)
        folder = config['gcs_folder']

        source_info = config.get(self.runtime_env, {})
        server_ip, username, password = resolve_smb_server_config(source_info)
        ctera_util = SMBUtil(server_ip, username, password)
        if not ctera_util:
            raise AirflowFailException("CTERA connection not available")
        file_copy_status = ctera_util.copy_ctera_to_gcs(
            ctera_shared_folder, file_name, bucket, folder, file_name
        )
        logger.info(f"file_copy_status: {file_copy_status}")
        if file_copy_status == consts.CTERA_SUCCESS:
            status = f"{file_copy_status}:{file_name} is copied in gcs folder {folder}"
            logger.info(status)
            deletion_file_status = self._delete_file_from_ctera(file_name, ctera_shared_folder, folder, ctera_util,
                                                                server_ip)
            logger.info(f"File Delete Status: {deletion_file_status}")
        else:
            raise AirflowFailException(
                f"{file_copy_status}:{file_name} is not copied in gcs folder {folder}, check log for error")

    def _delete_file_from_ctera(self, file_name, ctera_shared_folder, gcs_folder, ctera_util, server_ip):
        """Delete file from CTERA after successful copy to GCS"""
        ctera_file_path = f"{ctera_shared_folder}/{file_name}"
        ctera_util.delete_file(ctera_file_path)

        status = f"SUCCESS:{file_name} archived in gcs_folder:{gcs_folder} and removed from //{server_ip}/{ctera_file_path} path"
        logger.info(f"File Delete status: {status}")

        return status

    def _file_data_validation_task(self, **context):
        """Task to create temporary external table and perform file and data validation"""
        config = context['config']
        file_name = config[self.runtime_env][consts.FILE_NAME]
        bucket = config['gcs_bucket'].replace('{env}', self.deploy_env)
        folder = config['gcs_folder']

        # Construct GCS URI
        gcs_uri = f"gs://{bucket}/{folder}/{file_name}"

        # Get project ID from GCP config
        project_id = self.gcp_config.get('processing_zone_project_id')

        # Create external table ID
        dataset_id = config['staging_config']['dataset_id']
        stg_table_name = config['staging_config']['table_name'] + f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        table_id = f"{project_id}.{dataset_id}.{stg_table_name}"

        logger.info(f"""Starting file data validation for: {file_name}
                        Creating external table: {table_id}
                        GCS URI: {gcs_uri}""")

        # Initialize validation utils
        file_validation = PurgeListExclusionFileValidation(project_id=project_id)

        try:
            # Create BigQuery client
            bq_client = bigquery.Client(project=project_id)

            file_format = config['staging_config']['file_format']
            # Create External table
            create_external_table(bigquery_client=bq_client, ext_table_id=table_id, file_uri=gcs_uri, file_format=file_format)

            logger.info(f"External table created successfully: {table_id}")

            # Define validation requirements
            required_columns = config['validation_config']['required_columns']
            mandatory_fields = config['validation_config']['mandatory_fields']
            dob_column = config['validation_config']['dob_column']
            dob_expected_format = config['validation_config']['dob_expected_format']

            # Perform file and data validation
            validation_result = file_validation.perform_file_validation(
                table_id=table_id,
                required_columns=required_columns,
                mandatory_fields=mandatory_fields,
                dob_column=dob_column,
                dob_expected_format=dob_expected_format,
                include_violation_details=True
            )

            # Log validation results
            logger.info(f"""Validation completed for table: {table_id}
                            Overall validation status: {validation_result['overall_valid']}""")

            if validation_result['column_validation']:
                logger.info(f"Column validation: {validation_result['column_validation']['is_valid']}")
                if not validation_result['column_validation']['is_valid']:
                    logger.error(f"Missing columns: {validation_result['column_validation']['missing_columns']}")

            if validation_result['field_validation']:
                logger.info(f"Field validation: {validation_result['field_validation']['is_valid']}")
                if not validation_result['field_validation']['is_valid']:
                    logger.error(f"""Total violations: {validation_result['field_validation']['total_violations']}
                                     Null violations: {validation_result['field_validation']['null_violations']}
                                     Empty violations: {validation_result['field_validation']['empty_violations']}""")

            if validation_result['dob_validation']:
                logger.info(f"DOB validation: {validation_result['dob_validation']['is_valid']}")
                if not validation_result['dob_validation']['is_valid']:
                    logger.error(f"DOB format violations: {validation_result['dob_validation']['invalid_format_count']}")

            # Determine final result
            if validation_result['overall_valid']:
                logger.info("BigQuery data validation passed successfully")
                return "SUCCESS: File data validation completed successfully"
            else:
                error_msg = "BigQuery data validation failed"
                if validation_result.get('column_validation', {}).get('error_message'):
                    error_msg += f" - {validation_result['column_validation']['error_message']}"
                if validation_result.get('field_validation', {}).get('error_message'):
                    error_msg += f" - {validation_result['field_validation']['error_message']}"
                if validation_result.get('dob_validation', {}).get('error_message'):
                    error_msg += f" - {validation_result['dob_validation']['error_message']}"

                logger.error(error_msg)
                raise AirflowFailException(error_msg)

        except Exception as e:
            logger.error(f"File data validation failed: {str(e)}")
            raise AirflowFailException(f"File data validation failed: {str(e)}")


# Generate DAGs
globals().update(DAGFactory().create_dynamic_dags(PurgelistExclusionDagBuilder, config_filename=config_file_nm))
