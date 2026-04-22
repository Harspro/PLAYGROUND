# Standard Library Imports
import logging
from datetime import datetime, timedelta
from typing import Final, Dict
from copy import deepcopy

# Third-Party Imports
import pendulum

# Airflow Imports
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

# Internal/Project-Specific Imports
import util.constants as consts
from dag_factory.terminus_dag_factory import add_tags
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env_suffix
)

# Initial default arguments
INITIAL_DEFAULT_ARGS: Final = {
    'owner': 'TBD',
    'depends_on_past': False,
    'email': [],
    'severity': 'P3',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'capability': 'TBD',
    'sub_capability': 'TBD',
    'business_impact': 'TBD',
    'customer_impact': 'TBD'
}


class GcsToGcsTaskExecutor:
    """Class to generate Airflow DAGs for GCS to GCS file movement/copy based on YAML config."""

    def __init__(self, config_filename: str, config_dir: str = None):
        """
                Initialize with configuration from a YAML file with DAG names as top-level keys.

                :param config_filename: Name of the YAML configuration file.
                :type config_filename: str
                :param config_dir: Directory containing the config file; defaults to DAGS_FOLDER/config.
                :type config_dir: str, optional
        """
        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)
        self.local_tz = pendulum.timezone('America/Toronto')
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.deploy_env_suffix = self.gcp_config[consts.DEPLOY_ENV_STORAGE_SUFFIX]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config/gcs_to_gcs_task_executor_configs'

        self.config_dir = config_dir
        self.job_config = read_yamlfile_env_suffix(f'{config_dir}/{config_filename}', self.deploy_env, self.deploy_env_suffix)

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        """Build a single DAG from a workflow configuration."""

        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))
        gcp_conn_id = self.gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID)

        source_bucket = config[consts.DAG].get("source_bucket")
        source_object = config[consts.DAG].get("source_object", None)
        source_objects = config[consts.DAG].get("source_objects", None)
        destination_bucket = config[consts.DAG].get("destination_bucket")
        destination_object = config[consts.DAG].get("destination_object", None)
        match_glob = config[consts.DAG].get("match_glob", None)
        move_object = config[consts.DAG].get("move_object", True)
        replace = config[consts.DAG].get("replace", False)
        dag_timeout = int(config[consts.DAG].get(consts.DAGRUN_TIMEOUT)) if config[consts.DAG].get(
            consts.DAGRUN_TIMEOUT) else 10080
        execution_timeout = int(config[consts.DAG].get(consts.DAGRUN_TIMEOUT)) if config[consts.DAG].get(
            consts.DAGRUN_TIMEOUT) else 10080
        description = config[consts.DAG].get(consts.DESCRIPTION)
        max_active_runs = config[consts.DAG].get(consts.MAX_ACTIVE_RUNS, 1)

        self.dag = DAG(
            dag_id=dag_id,
            schedule=None,
            start_date=datetime(2025, 1, 1, tzinfo=self.local_tz),
            catchup=False,
            default_args=self.default_args,
            dagrun_timeout=timedelta(minutes=dag_timeout),
            description=description,
            is_paused_upon_creation=True,
            render_template_as_native_obj=True,
            tags=config[consts.DAG].get(consts.TAGS),
            max_active_runs=max_active_runs
        )

        # Empty Operator, used to indicate starting and ending tasks.
        start_point = EmptyOperator(task_id=consts.START_TASK_ID, dag=self.dag)
        end_point = EmptyOperator(task_id=consts.END_TASK_ID, dag=self.dag)

        # GCSToGCSOperator, used for move/copy file(s) between storage buckets.
        gcs_to_gcs = GCSToGCSOperator(
            task_id=f"{consts.GCS_TO_GCS}_task",
            gcp_conn_id=gcp_conn_id,
            source_bucket=source_bucket,
            source_object=source_object,
            source_objects=source_objects,
            destination_bucket=destination_bucket,
            destination_object=destination_object,
            match_glob=match_glob,
            move_object=move_object,
            replace=replace,
            execution_timeout=timedelta(minutes=execution_timeout),
            dag=self.dag
        )

        start_point >> gcs_to_gcs >> end_point
        return add_tags(self.dag)

    def create_dags(self) -> Dict[str, DAG]:
        """
        Creates a DAG for each configuration in the YAML file.

        :return: Dictionary mapping DAG IDs to DAG objects.
        :rtype: Dict[str, DAG]
        """
        if self.job_config:
            dags = {}
            for job_id, config in self.job_config.items():
                dags[job_id] = self.create_dag(job_id, config)
            return dags
        else:
            return {}
