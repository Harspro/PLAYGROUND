import logging
from datetime import timedelta, datetime
from typing import Final

import pendulum
from airflow import DAG, settings
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import storage

from bigquery_db_connector_loader.bigquery_db_connector_base import BigQueryDbConnector

import util.constants as consts
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.gcs_utils import delete_folder, get_matching_files, count_rows_in_file
from util.logging_utils import build_spark_logging_info
from util.miscutils import read_yamlfile_env_suffix, read_variable_or_file, read_yamlfile_env, get_cluster_config_by_job_size
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

"""
Initial default args to be used, users of the class can override the default args as needed through configuration
placed in the tsys_pcb_mc_accttx_config.yaml file.
"""
INITIAL_DEFAULT_ARGS: Final = {
    'owner': 'TBD',
    'capability': 'TBD',
    'severity': 'P3',
    'sub_capability': 'TBD',
    'business_impact': 'TBD',
    'customer_impact': 'TBD',
    'depends_on_past': False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False
}


class FileKafkaConnectorWriter:
    """
    FileKafkaConnectorWriter class, creates Airflow DAGs for spinning up a DataProc cluster and executing a DataProc job
    to write events to Kafka topics. The dataproc job utilizes the file-kafka-connector.

     Methods:
        build_fetching_job(self, cluster_name: str, config: dict): Constructs details for dataproc job.
        create_dag(): Defines the structure for the dynamically created DAGs.
        create_dags(): Runs through provided configuration in tsys_pcb_mc_accttx_config.yaml and dynamically
                       creates DAGs based on them.
    """

    def __init__(self, config_filename: str, config_dir: str = None):
        """
        Constructor: sets up gcp configuration, dataproc configuration, and job configuration for class.

        :param config_filename: configuration file name.
        :type config_filename: str

        :param config_dir: configuration directory name, if none is provided will utilize the settings.DAGS_FOLDER.
        :type config_dir: str
        """
        self.default_args = INITIAL_DEFAULT_ARGS
        self.local_tz = pendulum.timezone('America/Toronto')

        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.deploy_env_suffix = self.gcp_config[consts.DEPLOY_ENV_STORAGE_SUFFIX]
        self.network_tag = self.gcp_config[consts.NETWORK_TAG]
        self.processing_zone_connection_id = self.gcp_config[consts.PROCESSING_ZONE_CONNECTION_ID]

        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)
        self.dataproc_location = self.dataproc_config[consts.LOCATION]
        self.dataproc_project_id = self.dataproc_config[consts.PROJECT_ID]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config/file_kafka_connector_writer_configs'

        self.config_dir = config_dir
        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)

    @staticmethod
    def count_file_rows(bucket: str, file_path_pattern: str, has_header: bool = True, file_type: str = None, ti=None, **context):
        """
        Count data rows in CSV or Parquet file(s) in GCS.

        File type detection priority:
        1. Explicit file_type parameter from config ('CSV' or 'PARQUET')
        2. File extension (.csv or .parquet)

        :param bucket: GCS bucket name.
        :param file_path_pattern: File path pattern (e.g., 'folder/*.parquet').
        :param has_header: For CSV, exclude header row if True (default).
        :param file_type: File type from config ('CSV' or 'PARQUET'). If not provided, uses extension.
        :param ti: Airflow TaskInstance for XCom operations.
        :return: Total data row count across all matching files.
        """
        logger.info(f"Counting rows in files: gs://{bucket}/{file_path_pattern} (file_type={file_type})")

        try:
            client = storage.Client()
            bucket_obj = client.bucket(bucket)

            # Get matching files without extension filtering (file_type from config will handle it)
            matching_files = get_matching_files(bucket_obj, file_path_pattern, file_extensions=None)

            if not matching_files:
                logger.warning(f"No files found matching pattern: gs://{bucket}/{file_path_pattern}")
                if ti:
                    ti.xcom_push(key='file_row_count', value=0)
                return 0

            logger.info(f"Found {len(matching_files)} file(s) to process")

            # Pass file_type to count_rows_in_file for each file
            total_rows = sum(
                count_rows_in_file(bucket_obj, file_path, has_header, file_type)
                for file_path in matching_files
            )

            logger.info(f"Total rows across all files: {total_rows}")

            if ti:
                ti.xcom_push(key='file_row_count', value=total_rows)

            return total_rows

        except Exception as e:
            logger.error(f"Error counting file rows: {str(e)}", exc_info=True)
            if ti:
                ti.xcom_push(key='file_row_count', value=0)
            raise

    @staticmethod
    def branch_skip_if_empty(skip_task_id: str, continue_task_id: str, ti=None, **context):
        """
        Branch decision callable to skip kafka writer task if file has 0 rows.

        Supports both CSV and Parquet files. XComs are automatically isolated by dag_id,
        execution_date, and task_id to prevent interference between DAGs and concurrent runs.

        :param skip_task_id: Task ID to execute if file is empty (skip branch).
        :param continue_task_id: Task ID to execute if file has data (continue branch).
        :param ti: Airflow TaskInstance for XCom operations.
        :return: Task ID to execute next.
        """
        logger.info("Evaluating branch: skip kafka writer if file is empty")

        try:
            # Pull row count from XCom (automatically scoped by dag_id, execution_date, and task_id)
            row_count = ti.xcom_pull(task_ids='count_file_rows', key='file_row_count')

            if row_count is None:
                logger.warning("Row count not found in XCom from 'count_file_rows' task. Defaulting to continue (run Kafka writer).")
                return continue_task_id

            logger.info(f"File row count from counting task: {row_count}")

            if row_count == 0:
                logger.info(f"Row count is 0. Skipping kafka writer task. Executing: {skip_task_id}")
                return skip_task_id
            else:
                logger.info(f"Row count is {row_count}. Proceeding with kafka writer task. Executing: {continue_task_id}")
                return continue_task_id

        except Exception as e:
            logger.error(f"Error during branch decision: {str(e)}", exc_info=True)
            logger.warning("Defaulting to continue (run Kafka writer) due to error.")
            return continue_task_id

    def _create_branch_operator_tasks(self, bucket: str, file_path_pattern: str, kafka_writer_task_id: str,
                                      has_header: bool = True, file_type: str = None):
        """
        Create the three tasks needed for branch operator functionality.

        File type detection priority:
        1. Explicit file_type from config ('CSV' or 'PARQUET')
        2. File extension (.csv or .parquet)

        :param bucket: GCS bucket name.
        :param file_path_pattern: File path pattern for files.
        :param kafka_writer_task_id: Task ID of the Kafka writer task.
        :param has_header: For CSV files, whether header row exists (default True).
        :param file_type: File type from config ('CSV' or 'PARQUET'). If not provided, uses extension.
        :return: Tuple of (count_task, branch_task, skip_task).
        """
        # Create row counting task (supports CSV and Parquet)
        count_file_rows_task = PythonOperator(
            task_id='count_file_rows',
            python_callable=FileKafkaConnectorWriter.count_file_rows,
            op_kwargs={
                'bucket': bucket,
                'file_path_pattern': file_path_pattern,
                'has_header': has_header,
                'file_type': file_type
            }
        )

        # Create branch operator
        branch_check_empty_task = BranchPythonOperator(
            task_id='branch_check_empty_file',
            python_callable=FileKafkaConnectorWriter.branch_skip_if_empty,
            op_kwargs={
                'skip_task_id': 'skip_kafka_writer_empty_file',
                'continue_task_id': kafka_writer_task_id
            }
        )

        # Create skip task for empty file
        skip_kafka_writer_task = EmptyOperator(
            task_id='skip_kafka_writer_empty_file',
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )

        return count_file_rows_task, branch_check_empty_task, skip_kafka_writer_task

    def build_fetching_job(self, cluster_name: str, config: dict):
        """
        Constructing details for dataproc job based on cluster name and job configuration provided.

        :param cluster_name: cluster name based on DAG id.
        :type cluster_name: str

        :param config: job configuration provided.
        :type config: dict
        """
        for k, v in consts.DEFAULT_KAFKA_SPARK_SETTINGS.items():
            if k == consts.JAR_FILE_URIS:
                v[0] = v[0].replace(consts.ENV_PLACEHOLDER, self.deploy_env)
            elif k == consts.FILE_URIS:
                v[0] = v[0].replace(consts.ENV_PLACEHOLDER, self.deploy_env)
                log4j_config = ""
                if self.deploy_env in ['dev', 'uat']:
                    v[1] = v[1].replace(consts.ENV_PLACEHOLDER, self.deploy_env)
                    log4j_config = consts.DEFAULT_KAFKA_SPARK_LOG4J_CONFIG
                else:
                    v = [v[0]]
            else:
                v = v.replace(consts.ENV_PLACEHOLDER, self.deploy_env)

            if k not in config.keys():
                config[k] = v
            else:
                if isinstance(v, list):
                    for item in v:
                        config[k].append(item)
                else:
                    config[k].append(v)

        default_properties = config.get(consts.PROPERTIES) or consts.DEFAULT_SPARK_SETTINGS
        kafka_spark_properties = consts.DEFAULT_KAFKA_SPARK_PROPERTIES

        for key, value in default_properties.items():
            if key in [consts.SPARK_DRIVER_EXTRAJAVAOPTIONS, consts.SPARK_EXECUTOR_EXTRAJAVAOPTIONS]:
                default_properties[key] = value + consts.DEFAULT_KAFKA_SPARK_DRIVER_EXTRAJAVAOPTIONS + log4j_config

        for key, value in kafka_spark_properties.items():
            if key not in default_properties:
                default_properties[key] = value

        gcs_config = config.get(consts.GCS)

        if not gcs_config:
            gcs_config = {
                consts.BUCKET: "{{ dag_run.conf['bucket'] }}",
                consts.FOLDER_NAME: "{{ dag_run.conf['folder_name'] }}",
                consts.FILE_NAME: "{{ dag_run.conf['file_name'] }}"
            }
        else:
            for key, value in gcs_config.items():
                gcs_config[key] = value.replace(consts.ENV_PLACEHOLDER, self.deploy_env)

        arg_list = [config[consts.APPLICATION_CONFIG]]

        if consts.ARGS in config:
            args_to_update = [consts.KAFKA_CONNECTOR_KEY_SERIALIZER_CLASS_CONFIG,
                              consts.KAFKA_CONNECTOR_VALUE_SERIALIZER_CLASS_CONFIG,
                              consts.KAFKA_CONNECTOR_MESSAGE_SCHEMA_TYPE]

            for k in args_to_update:
                if k not in config[consts.ARGS]:
                    if k != consts.KAFKA_CONNECTOR_MESSAGE_SCHEMA_TYPE:
                        config[consts.ARGS][k] = consts.KAFKA_CONNECTOR_SERIALIZER_CLASS
                    else:
                        config[consts.ARGS][k] = consts.KAFKA_CONNECTOR_AVRO_MESSAGE

            for k, v in config[consts.ARGS].items():
                if k == consts.KAFKA_CONNECTOR_FILE_PATH:
                    file_path = f'{k}=gs://{gcs_config.get(consts.BUCKET)}/{gcs_config.get(consts.FOLDER_NAME)}/' \
                                f'{gcs_config.get(consts.FILE_NAME)}'
                    if config[consts.ARGS][k].get(consts.FILE_PATH_FROM_PRECEDING_DAG) and \
                            config[consts.ARGS][k].get(consts.FILE_PATH_SUFFIX):
                        arg_list.append(f'{file_path}{config[consts.ARGS][k].get(consts.FILE_PATH_SUFFIX)}')
                    elif config[consts.ARGS][k].get(consts.FILE_SOURCE_BIGQUERY):
                        arg_list.append(f'{file_path}-*.parquet')
                    else:
                        arg_list.append(file_path)

                elif k == consts.KAFKA_CONNECTOR_FILTERED_FIELDS \
                        or k == consts.KAFKA_CONNECTOR_GROUP_BY_FIELDS \
                        or k == consts.KAFKA_CONNECTOR_SOURCE_ARRAY_FIELDS:
                    arg_list.append(
                        f'{k}={consts.COMMA.join(field.strip() for field in v)}'
                    )
                else:
                    arg_list.append(
                        f'{k}={v}'
                    )

        arg_list = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=self.default_args, arg_list=arg_list)

        return {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_project_id},
            consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: config[consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: config[consts.MAIN_CLASS],
                consts.FILE_URIS: config[consts.FILE_URIS],
                consts.ARGS: arg_list,
                consts.PROPERTIES: default_properties
            }
        }

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        """
        Defines structure for the dynamically created DAGs, made of 4 tasks:
        1. Empty Operator start point.
        2. Dataproc cluster creation, unique to each dag name.
        3. Dataproc submit job operator, submits dataproc job based on configuration provided.
        4. Empty Operator end point.

        :param dag_id: cluster name based on DAG id.
        :type dag_id: str

        :param config: job configuration provided.
        :type config: dict

        :return: Created dag.
        :rtype: DAG
        """

        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))

        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            description="DAG to utilize the file-kafka-connector spark module. This module facilitates the "
                        "ingestion of data from files and enables the creation of Kafka events using a Kafka producer.",
            render_template_as_native_obj=True,
            schedule=config[consts.DAG].get(consts.SCHEDULE_INTERVAL),
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
            max_active_runs=1,
            catchup=False,
            dagrun_timeout=timedelta(hours=24),
            is_paused_upon_creation=True,
            tags=config[consts.DAG].get(consts.TAGS)
        )

        with dag:
            if config[consts.DAG].get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(dag_id, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            # Empty Operator, used to indicate starting and ending tasks.
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            job_size = config[consts.DAG].get(consts.DATAPROC_JOB_SIZE)
            cluster_name = "{{ dag_run.conf.get('cluster_name')  or '" + \
                           str(config[consts.DAG].get(consts.DATAPROC_CLUSTER_NAME)) + "' }}"

            # Dataproc Cluster Creation Task, used to create Dataproc Clusters.
            cluster_creating_task = DataprocCreateClusterOperator(
                task_id=consts.CLUSTER_CREATING_TASK_ID,
                project_id=self.dataproc_project_id,
                cluster_config=get_cluster_config_by_job_size(self.deploy_env, self.network_tag, job_size),
                region=self.dataproc_location,
                cluster_name=cluster_name
            )

            # Check if branch operator for skipping empty parquet files is enabled
            enable_skip_if_empty = config[consts.DAG].get('skip_kafka_writer_if_empty', False)

            # Dataproc Submit Job Task, submits Kafka writer Dataproc jobs.
            kafka_writer_task = DataprocSubmitJobOperator(
                task_id=config[consts.DAG][consts.KAFKA_WRITER_TASK],
                job=self.build_fetching_job(cluster_name, config),
                region=self.dataproc_location,
                project_id=self.dataproc_project_id,
                gcp_conn_id=self.processing_zone_connection_id,
                # Set trigger rule to handle skipped upstream tasks if branch operator is enabled
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS if enable_skip_if_empty else TriggerRule.ALL_SUCCESS
            )

            # Determine if file source is from BigQuery.
            if consts.BIGQUERY in config and consts.GCS in config:
                project_id = config[consts.BIGQUERY].get(consts.PROJECT_ID)
                dataset_id = config[consts.BIGQUERY].get(consts.DATASET_ID)

                if config[consts.BIGQUERY].get(consts.TABLE_ID):
                    table_id = config[consts.BIGQUERY].get(consts.TABLE_ID).replace(
                        consts.CURRENT_DATE_PLACEHOLDER,
                        datetime.now(tz=self.local_tz).strftime('%Y_%m_%d')
                    )
                else:
                    table_id = None
                # Replacement dictionary to be applied on the query.
                if config[consts.BIGQUERY].get(consts.REPLACEMENTS):
                    replacements = config[consts.BIGQUERY].get(consts.REPLACEMENTS)
                else:
                    replacements = "{{ dag_run.conf.get('replacements')  or 'None' }}"

                env_specific_replacements = config[consts.BIGQUERY].get(consts.ENV_SPECIFIC_REPLACEMENTS)

                gcs_config = config[consts.GCS]
                bucket = gcs_config.get(consts.BUCKET)
                folder_name = gcs_config.get(consts.FOLDER_NAME)

                if config[consts.BIGQUERY].get(consts.QUERY):
                    query = config[consts.BIGQUERY].get(consts.QUERY)
                elif config[consts.BIGQUERY].get(consts.QUERY_FILE):
                    query_sql_file = f"{settings.DAGS_FOLDER}/" \
                                     f"{config[consts.BIGQUERY].get(consts.QUERY_FILE)}"
                    query = read_yamlfile_env_suffix(query_sql_file, self.deploy_env, self.deploy_env_suffix)
                else:
                    raise AirflowFailException("Please provide query, none found.")

                # Remove staging files from previous runs.
                delete_gcs_processing_obj_task = PythonOperator(
                    task_id=consts.DELETE_GCS_PROCESSING_OBJ_TASK_ID,
                    python_callable=delete_folder,
                    op_kwargs={consts.BUCKET_NAME: bucket,
                               consts.PREFIX_NAME: folder_name
                               }
                )

                # Query to use when generating Parquet files.
                bigquery_execute_query_task = PythonOperator(
                    task_id=consts.QUERY_BIGQUERY_TABLE_TASK_ID,
                    python_callable=BigQueryDbConnector.run_bigquery_staging_query,
                    op_kwargs={consts.QUERY: query,
                               consts.PROJECT_ID: project_id,
                               consts.DATASET_ID: dataset_id,
                               consts.TABLE_ID: table_id,
                               consts.REPLACEMENTS: replacements,
                               consts.ENV_SPECIFIC_REPLACEMENTS: env_specific_replacements,
                               consts.DEPLOY_ENV: self.deploy_env
                               }
                )

                # Setup task flow based on whether branch operator is enabled
                if enable_skip_if_empty:
                    # Get file type from config args (e.g., 'CSV' or 'PARQUET')
                    file_type = config.get(consts.ARGS, {}).get('pcb.file.kafka.connector.file.type')

                    # Create branch operator tasks
                    file_path_pattern = f"{folder_name}/{gcs_config.get(consts.FILE_NAME)}-*.parquet"
                    count_task, branch_task, skip_task = self._create_branch_operator_tasks(
                        bucket=bucket,
                        file_path_pattern=file_path_pattern,
                        kafka_writer_task_id=config[consts.DAG][consts.KAFKA_WRITER_TASK],
                        file_type=file_type
                    )

                    # Update end_point trigger rule to handle multiple upstream paths
                    end_point.trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS

                    # Set up task flow with branch
                    start_point >> cluster_creating_task >> delete_gcs_processing_obj_task >> bigquery_execute_query_task
                    bigquery_execute_query_task >> count_task >> branch_task
                    branch_task >> [kafka_writer_task, skip_task]
                    kafka_writer_task >> end_point
                    skip_task >> end_point
                else:
                    # Original flow without branch operator
                    start_point >> cluster_creating_task >> delete_gcs_processing_obj_task >> bigquery_execute_query_task >> kafka_writer_task >> end_point
            else:
                # Flow for passed parquet files (not generated by BigQuery)
                if enable_skip_if_empty:
                    gcs_config = config.get(consts.GCS)

                    if gcs_config:
                        bucket = gcs_config.get(consts.BUCKET)
                        folder_name = gcs_config.get(consts.FOLDER_NAME)
                        file_name = gcs_config.get(consts.FILE_NAME)

                        # Construct file path pattern
                        file_path_pattern = f"{folder_name}/{file_name}" if folder_name else file_name

                        # Get file type from config args (e.g., 'CSV' or 'PARQUET')
                        file_type = config.get(consts.ARGS, {}).get('pcb.file.kafka.connector.file.type')

                        # Create branch operator tasks
                        count_task, branch_task, skip_task = self._create_branch_operator_tasks(
                            bucket=bucket,
                            file_path_pattern=file_path_pattern,
                            kafka_writer_task_id=config[consts.DAG][consts.KAFKA_WRITER_TASK],
                            file_type=file_type
                        )

                        # Update end_point trigger rule to handle multiple upstream paths
                        end_point.trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS

                        # Set up task flow with branch
                        start_point >> cluster_creating_task >> count_task >> branch_task
                        branch_task >> [kafka_writer_task, skip_task]
                        kafka_writer_task >> end_point
                        skip_task >> end_point
                    else:
                        logger.warning(f"skip_kafka_writer_if_empty enabled but no GCS config found for DAG {dag_id}")
                        start_point >> cluster_creating_task >> kafka_writer_task >> end_point
                else:
                    # Original flow without branch operator
                    start_point >> cluster_creating_task >> kafka_writer_task >> end_point

        return add_tags(dag)

    def create_dags(self) -> dict:
        """
        Creates a dag for each provided configuration in the tsys_pcb_mc_accttx_config.yaml file.

        :return: created dags.
        :rtype: dict
        """
        dags = {}

        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)

        return dags
