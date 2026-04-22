# Standard library imports
from copy import deepcopy
from datetime import datetime, timedelta
import logging
from typing import Final
from airflow.operators.empty import EmptyOperator

# Third-party imports
from airflow import DAG, settings
import pendulum

# Local application imports
import util.constants as consts
from util.miscutils import read_variable_or_file, read_yamlfile_env, get_serverless_cluster_config, read_file_env

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.exceptions import AirflowFailException

logger = logging.getLogger(__name__)

"""
Initial default args to be used, users of the class can override the default args
as needed through configuration placed in the "".yaml file.
"""
INITIAL_DEFAULT_ARGS: Final = {
    'owner': 'sharing-reusable-components',
    'capability': 'multiple areas',
    'severity': 'P3',
    'sub_capability': 'multiple areas',
    'business_impact': 'not applicable',
    'customer_impact': 'not applicable',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False
}


class DataprocTaskExecutor:
    """Class to generate Airflow DAGs for Executing python callables based on YAML config"""

    def __init__(self, config_filename: str, config_dir: str = None):
        """
        Constructor: sets up gcp/job configuration for class.

        :param config_filename: configuration file name.
        :type config_filename: str

        :param config_dir: configuration directory name, if none is provided will utilize
            the settings.DAGS_FOLDER.
        :type config_dir: str
        """

        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)

        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.deploy_env_suffix = self.gcp_config[consts.DEPLOY_ENV_STORAGE_SUFFIX]
        self.network_tag = self.gcp_config[consts.NETWORK_TAG]
        self.processing_zone_connection_id = self.gcp_config[consts.PROCESSING_ZONE_CONNECTION_ID]

        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)
        self.dataproc_location = self.dataproc_config[consts.LOCATION]
        self.dataproc_project_id = self.dataproc_config[consts.PROJECT_ID]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config/dataproc_task_executor_configs'

        self.config_dir = config_dir
        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)

    def _create_dataproc_serverless_cluster_tasks(self, config: dict):
        """Create Dataproc Serverless Spark job task from config."""

        tasks = []

        # Empty Operator, used to indicate starting and ending tasks.
        start_point = EmptyOperator(task_id=consts.START_TASK_ID, dag=self.dag)
        end_point = EmptyOperator(task_id=consts.END_TASK_ID, dag=self.dag)

        tasks.append(start_point)

        # Validate config structure
        if not isinstance(config, dict):
            raise AirflowFailException("Invalid config for Dataproc Serverless task: expected a dict")

        spark_job_config = config.get(consts.SPARK_JOB)
        if not isinstance(spark_job_config, dict):
            raise AirflowFailException("Missing or invalid 'spark_job' section in config: expected a dict")

        # Validate required spark job keys (args are optional)
        required_keys = [consts.MAIN_CLASS, consts.JAR_FILE_URIS]
        missing = [k for k in required_keys if not spark_job_config.get(k)]
        if missing:
            raise AirflowFailException(f"Missing required spark_job fields: {', '.join(missing)}")

        # Get the cluster configuration
        subnetwork_uri, network_tags, service_account = get_serverless_cluster_config(self.deploy_env, self.network_tag)

        # Build args list (optional)
        raw_args = spark_job_config.get(consts.ARGS)
        if raw_args is None:
            args_list = []
        elif isinstance(raw_args, list):
            args_list = raw_args
        elif isinstance(raw_args, dict):
            # Create a copy to avoid modifying the original dict
            processed_args = dict(raw_args)

            # Handle query parameter similar to bigquery_task_executor pattern
            # Support both inline query and query_file (file path relative to DAGS_FOLDER)
            query_arg_key = 'pcb.spark.model.inference.input.query'
            query_file_arg_key = 'pcb.spark.model.inference.input.query_file'

            if query_arg_key in processed_args:
                # Inline query provided - use as is
                pass  # No processing needed
            elif query_file_arg_key in processed_args:
                # Query file provided - read from file
                query_file_path = processed_args[query_file_arg_key]
                sql_file_path = f"{settings.DAGS_FOLDER}/{query_file_path}"
                try:
                    query = read_file_env(sql_file_path, self.deploy_env)
                    # Replace query_file with query containing the file content
                    del processed_args[query_file_arg_key]
                    processed_args[query_arg_key] = query.strip()
                    logger.info(f"Loaded SQL query from file: {sql_file_path}")
                except Exception as e:
                    raise AirflowFailException(f"Failed to read SQL file {sql_file_path}: {str(e)}")
            # Note: We don't raise an error if neither is provided, as query might not be required for all jobs

            args_list = [f"{k}={v}" for k, v in processed_args.items()]
        else:
            raise AirflowFailException("'spark_job.args' must be a list or dict if provided")

        # Build spark_batch config
        spark_batch_config = {
            consts.MAIN_CLASS: spark_job_config.get(consts.MAIN_CLASS),
            consts.JAR_FILE_URIS: spark_job_config.get(consts.JAR_FILE_URIS),
            consts.ARGS: args_list,
        }

        # Only include file_uris if provided and not None/empty
        file_uris = spark_job_config.get(consts.FILE_URIS)
        if file_uris is not None and file_uris:
            spark_batch_config[consts.FILE_URIS] = file_uris

        batch_config = {
            consts.SPARK_BATCH: spark_batch_config,
            consts.ENVIRONMENT_CONFIG: {
                consts.EXECUTION_CONFIG: {
                    consts.SUBNETWORK_URI: subnetwork_uri,
                    consts.NETWORK_TAGS: network_tags,
                    consts.SERVICE_ACCOUNT: service_account
                },
            },
        }

        # Build runtime_config section
        # Supports both runtime version (for Spark/Java version) and properties
        runtime_config = {}

        # Add runtime version if specified (e.g., "3.0" for Spark 4.0 + Java 21)
        runtime_version = spark_job_config.get('runtime_version')
        if runtime_version is not None:
            runtime_config['version'] = str(runtime_version)

        # Convert all property values to strings as Dataproc API requires string values
        properties = spark_job_config.get(consts.PROPERTIES)
        if properties is not None and properties:
            # Convert all property values to strings
            properties_str = {k: str(v) for k, v in properties.items()}
            runtime_config[consts.PROPERTIES] = properties_str

        # Only include runtime_config if it has content
        if runtime_config:
            batch_config[consts.RUNTIME_CONFIG] = runtime_config

        # Generate batch_id: use config-provided prefix if available, otherwise use existing logic
        # Datetime format: %Y%m%d%H%M%S%f = 20 characters (YYYYMMDDHHMMSSffffff)
        # Batch ID pattern: [a-z0-9][a-z0-9\-]{2,61}[a-z0-9] (total 4-63 chars)
        # So prefix + "-" + datetime must be <= 63 chars, meaning prefix <= 42 chars
        batch_id_prefix = spark_job_config.get('batch_id_prefix')
        if batch_id_prefix:
            batch_id_prefix = str(batch_id_prefix).replace('_', '-')
            # Use config-provided prefix
            batch_id = f"{batch_id_prefix}-{datetime.now().astimezone(tz=self.local_tz).strftime('%Y%m%d%H%M%S%f')}"
        else:
            # Use existing logic: dag_id with underscores replaced by hyphens
            batch_id = f"{{{{ dag.dag_id.replace('_', '-') }}}}-{datetime.now().astimezone(tz=self.local_tz).strftime('%Y%m%d%H%M%S%f')}"
        execution_timeout = int(config[consts.DAG].get(consts.DAGRUN_TIMEOUT)) if config[consts.DAG].get(
            consts.DAGRUN_TIMEOUT) else 10080

        task = DataprocCreateBatchOperator(
            task_id='create_dataproc_serverless_cluster',
            batch=batch_config,
            region=self.dataproc_config.get(consts.LOCATION),
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
            execution_timeout=timedelta(minutes=execution_timeout),
            batch_id=batch_id
        )

        tasks.append(task)

        # Add end point task
        tasks.append(end_point)

        # Return list of tasks
        return tasks

    def create_dag(self, dag_id: str, config: dict) -> DAG:

        """Create and return an Airflow DAG based on config. This method is called once per DAG config in the
           config yaml file. This will create DAG and list of tasks within that DAG as per configuration
        """

        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))
        dag_timeout = int(config[consts.DAG].get(consts.DAGRUN_TIMEOUT)) if config[consts.DAG].get(
            consts.DAGRUN_TIMEOUT) else 10080
        max_active_runs = config[consts.DAG].get(consts.MAX_ACTIVE_RUNS, 1)

        self.dag = DAG(
            dag_id=dag_id,
            schedule=config[consts.DAG].get(consts.SCHEDULE_INTERVAL),
            start_date=datetime(2025, 1, 1, tzinfo=self.local_tz),
            catchup=False,
            description=config[consts.DAG].get(consts.DESCRIPTION),
            default_args=self.default_args,
            dagrun_timeout=timedelta(minutes=dag_timeout),
            is_paused_upon_creation=True,
            tags=config[consts.DAG].get(consts.TAGS),
            max_active_runs=max_active_runs
        )

        # Ensure tasks are created within DAG context so they attach to this DAG
        with self.dag:
            tasks = self._create_dataproc_serverless_cluster_tasks(config=config)

        # Set sequential dependencies
        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]

        return self.dag

    def create_dags(self) -> dict:
        """
        Creates a dag for each provided configuration in the dataproc_task_executor_config.yaml file.

        :return: created dags.
        :rtype: dict
        """

        if self.job_config:
            dags = {}
            for job_id, config in self.job_config.items():
                dags[job_id] = self.create_dag(job_id, config)
            return dags
        else:
            return {}
