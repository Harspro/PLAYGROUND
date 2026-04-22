# Standard library imports
from copy import deepcopy
from datetime import datetime, timedelta
import logging
from typing import Dict, Final, Optional

# Third-party imports
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import pendulum

# Local application imports
import util.constants as consts
from dag_factory.terminus_dag_factory import add_tags
from util.miscutils import read_variable_or_file, read_yamlfile_env
from util.deploy_utils import pause_unpause_dag

# Initial default arguments
INITIAL_DEFAULT_ARGS: Final = {
    'owner': 'team-defenders-alerts',
    'capability': 'risk-management',
    'severity': 'P3',
    'sub_capability': 'orchestration',
    'business_impact': 'Orchestration of dependent DAGs will be impacted, potentially causing delays in downstream processes',
    'customer_impact': 'Indirect impact through delayed processing of customer data and transactions',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

logger = logging.getLogger(__name__)


class GenericOrchestrator:
    """Class to generate Airflow DAGs for resolving dependencies between various sub-dags based on YAML config."""

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

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config/generic_orchestrator_configs'

        self.config_dir = config_dir
        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)
        self._deploy_config_path = f'{settings.DAGS_FOLDER}/config/deploy_config.yaml'
        self._deploy_settings: Optional[dict] = None

    def _load_deploy_settings(self) -> dict:
        """Load deploy_config.yaml once per instance"""
        if self._deploy_settings is None:
            self._deploy_settings = read_yamlfile_env(str(self._deploy_config_path), self.deploy_env)
        return self._deploy_settings

    def _get_pause_setting(self, dag_name: str) -> bool:
        """Return pause setting for the provided dag name using cached deploy config."""
        deploy_settings = self._load_deploy_settings()
        dag_settings = deploy_settings.get(dag_name) or deploy_settings.get('default', {})
        return dag_settings.get(self.deploy_env, True)

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        """Build a single DAG from a workflow configuration."""
        # Update default args with config if provided
        default_args_update = config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS)
        if not isinstance(default_args_update, dict):
            raise ValueError(f"Default args must be a dictionary, got {type(default_args_update)}")
        self.default_args = deepcopy(default_args_update)
        dag_timeout = int(config[consts.DAG].get(consts.DAGRUN_TIMEOUT)) if config[consts.DAG].get(consts.DAGRUN_TIMEOUT) else 10080
        sub_dag_trigger_rule = str(config[consts.DAG].get(consts.SUB_DAG_TRIGGER_RULE)) if str(config[consts.DAG].get(consts.SUB_DAG_TRIGGER_RULE)) in TriggerRule.all_triggers() else TriggerRule.ALL_SUCCESS
        max_active_runs = config[consts.DAG].get(consts.MAX_ACTIVE_RUNS, 1)

        # Create the DAG
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

        with self.dag:
            # Start and End tasks
            start_point = EmptyOperator(task_id=consts.START_TASK_ID, dag=self.dag)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID, dag=self.dag)
            execution_timeout = int(config[consts.DAG].get(consts.DAGRUN_TIMEOUT)) if config[consts.DAG].get(consts.DAGRUN_TIMEOUT) else 10080
            group_map = {}
            previous_group = start_point
            if config[consts.DAG].get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = self._get_pause_setting(dag_id)
                pause_unpause_dag(self.dag, is_paused)

            # Create tasks for each task group
            for group in config.get(consts.TASK_GROUPS, []):
                group_id = group['group_id']
                execution_mode = group.get('execution_mode', 'parallel')

                # Create a TaskGroup for the group
                with TaskGroup(group_id=group_id, dag=self.dag, tooltip=group.get('description', f'Task group {group_id}')) as task_group:
                    tasks = []

                    # Create TriggerDagRunOperator for each DAG in the group
                    for dag_entry in group.get('dags', []):
                        sub_dag_id = dag_entry if isinstance(dag_entry, str) else dag_entry.get('sub_dag_id')
                        sub_dag_params = dag_entry.get('params', {}) if isinstance(dag_entry, dict) else {}
                        if config[consts.DAG].get(consts.READ_PAUSE_DEPLOY_CONFIG):
                            is_paused = self._get_pause_setting(sub_dag_id)
                            pause_unpause_dag(sub_dag_id, is_paused)

                        task = TriggerDagRunOperator(
                            task_id=f'trigger_{sub_dag_id}',
                            trigger_dag_id=sub_dag_id,
                            conf=sub_dag_params,
                            logical_date=datetime.now(self.local_tz).strftime('%Y-%m-%d %H:%M:%S'),
                            wait_for_completion=True,
                            poke_interval=60,
                            execution_timeout=timedelta(minutes=execution_timeout),
                            trigger_rule=sub_dag_trigger_rule
                        )
                        tasks.append(task)

                    # Set up execution mode within the group
                    if execution_mode == 'sequential' and tasks:
                        for i in range(len(tasks) - 1):
                            tasks[i] >> tasks[i + 1]

                # Store the TaskGroup in group_map for dependency setup
                group_map[group_id] = task_group

                # Chain the current TaskGroup to the previous group or start_point
                previous_group >> task_group
                previous_group = task_group

            # Connect the last TaskGroup to end_point
            previous_group >> end_point

        return add_tags(self.dag)

    def create_dags(self) -> Dict[str, DAG]:
        """
        Creates a DAG for each configuration in the YAML file.

        :return: Dictionary mapping DAG IDs to DAG objects.
        :rtype: Dict[str, DAG]
        """
        dags = {}
        if self.job_config:
            for dag_id, config in self.job_config.items():
                dags[dag_id] = self.create_dag(dag_id, config)
        return dags
