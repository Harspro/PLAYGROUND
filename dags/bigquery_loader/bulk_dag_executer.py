import json
import logging
import pendulum
from typing import Final
from copy import deepcopy
from datetime import timedelta
import util.constants as consts
from airflow import DAG, settings
from datetime import datetime
from airflow.models import Variable, DagModel
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from util.deploy_utils import pause_unpause_dag
from util.miscutils import read_yamlfile_env, read_variable_or_file
from dag_factory.terminus_dag_factory import add_tags

INITIAL_DEFAULT_ARGS: Final = {
    'owner': 'team-ogres-alerts',
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

logger = logging.getLogger(__name__)


class BulkDagExecuter:
    def __init__(self, config_filename, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        self.config_dir = config_dir
        self.config_file_path = f'{config_dir}/{config_filename}'
        self.job_config = read_yamlfile_env(self.config_file_path, self.deploy_env)
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)
        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)

    def decide_branch(self, no_variable):
        if no_variable:
            return 'start'
        else:
            return 'update_airflow_variables'

    def update_date_variables(self, parent_dag_id, dag_names):
        try:
            date_variables_str = Variable.get(parent_dag_id)
            if not date_variables_str:
                raise ValueError(f"No date variables found for {parent_dag_id}")

            date_variables = json.loads(date_variables_str)
            start_date = date_variables.get("start_date")
            end_date = date_variables.get("end_date")
            overwrite_bq_table = date_variables.get("overwrite_bq_table", False)  # Default to False if not provided

            if start_date is None or end_date is None:
                raise ValueError(f"Start date or end date not found in the variables for {parent_dag_id}")

            for dag_name in dag_names:
                key = dag_name
                value = {
                    "overwrite_bq_table": overwrite_bq_table,
                    "start_date": start_date,
                    "end_date": end_date
                }
                Variable.set(key, json.dumps(value))
        except KeyError:
            raise Exception(f"Variable for {parent_dag_id} not found.")
        except json.JSONDecodeError:
            raise Exception(f"Error decoding JSON for {parent_dag_id}.")

    def remove_date_variables(self, dag_names):
        for dag_name in dag_names:
            Variable.delete(key=dag_name)

    def check_and_unpause_dag(self, dag_id, **context):
        dag_model = DagModel.get_current(dag_id)
        if dag_model is None:
            logger.error(f"DAG '{dag_id}' not found. Skipping...")
            raise AirflowSkipException(f"DAG '{dag_id}' is NOT present.")
        if dag_model.is_paused:
            logger.info(f"DAG '{dag_id}' is paused. Unpausing...")
            pause_unpause_dag(dag_id, False)

    def create_dag(self, parent_dag_id, dag_names, batch_size, no_variable, config):
        dag_default_args = deepcopy(self.default_args)
        if 'default_args' in config:
            # Only update keys that already exist in the default args
            dag_default_args.update(config['default_args'])
        dag = DAG(
            dag_id=parent_dag_id,
            default_args=dag_default_args,
            schedule=None,
            start_date=datetime(2024, 1, 1, tzinfo=self.local_tz),
            catchup=False,
            max_active_runs=1,
            dagrun_timeout=timedelta(days=10),
            is_paused_upon_creation=True,
            max_active_tasks=batch_size  # Use batch_size to set max_active_tasks
        )

        check_for_variable_branch = BranchPythonOperator(
            task_id='check_for_variable',
            python_callable=self.decide_branch,
            op_kwargs={
                'no_variable': no_variable
            },
            dag=dag
        )

        # Add initial task to update date variables
        update_variables_task = PythonOperator(
            task_id='update_airflow_variables',
            python_callable=self.update_date_variables,
            op_kwargs={
                'parent_dag_id': parent_dag_id,
                'dag_names': dag_names
            },
            dag=dag
        )

        # Add task to remove date variables at the end
        remove_variables_task = PythonOperator(
            task_id='remove_airflow_variables',
            python_callable=self.remove_date_variables,
            op_kwargs={
                'dag_names': dag_names
            },
            dag=dag,
            trigger_rule=TriggerRule.ALL_DONE  # Added trigger rule here
        )

        # Add delay task using TimeDeltaSensor
        delay_task = TimeDeltaSensor(
            task_id='wait_time_for_dags_update',
            delta=timedelta(minutes=2),
            dag=dag,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        # Skip to this task if no variables are provided
        start_task = EmptyOperator(task_id='start', dag=dag, trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

        # Fail DAG if any task fails
        end_task = EmptyOperator(task_id='end', dag=dag, trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

        # Split dag_names list into a list of DAG batches, with each batch containing up to batch_size DAGs
        dag_batches = [dag_names[_:_ + batch_size] for _ in range(0, len(dag_names), batch_size)]

        batch_groups = []

        for index, dag_batch in enumerate(dag_batches):
            with TaskGroup(group_id=f"{parent_dag_id}_{index}", dag=dag) as batch_group:

                for dag_id in dag_batch:
                    with TaskGroup(group_id=dag_id, dag=dag):
                        # Validate task
                        validate_task = PythonOperator(
                            task_id='validate',
                            python_callable=self.check_and_unpause_dag,
                            op_kwargs={'dag_id': dag_id},
                            dag=dag
                        )

                        # TriggerDagRunOperator
                        trigger_task = TriggerDagRunOperator(
                            task_id='execute',
                            trigger_dag_id=dag_id,
                            conf={"message": f"Triggered by {parent_dag_id}"},
                            wait_for_completion=True,
                            reset_dag_run=True,
                            retries=0,
                            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                            dag=dag
                        )

                        pause_task = PythonOperator(
                            task_id='pause',
                            python_callable=pause_unpause_dag,
                            op_kwargs={'dag': dag_id, 'paused_bool': True},
                            dag=dag
                        )

                        validate_task >> trigger_task >> pause_task

            batch_groups.append(batch_group)

        # Chain batch groups
        for previous_batch, next_batch in zip(batch_groups, batch_groups[1:]):
            previous_batch >> next_batch

        # Skip if batch_groups list is empty
        if batch_groups:
            check_for_variable_branch >> update_variables_task >> delay_task >> start_task
            check_for_variable_branch >> start_task
            start_task >> batch_groups[0]
            batch_groups[-1] >> remove_variables_task >> end_task

        return add_tags(dag)

    def create_dags(self):
        dags = {}
        if not self.job_config:
            logger.warning(f'Config file {self.config_file_path} is empty.')
            return dags

        for parent_dag_id, config in self.job_config.items():
            dag_names = config.get('dags', [])
            batch_size = config.get('batch_size', 3)  # Default batch size is 3 if not provided
            no_variable = config.get('no_variable', False)  # Default False if not provided
            unique_dag_names = sorted(list(set(dag_names)))  # Ensure uniqueness of DAG names and sort
            dags[parent_dag_id] = self.create_dag(parent_dag_id, unique_dag_names, batch_size, no_variable, config)
        return dags


# Update globals with the created DAGs
globals().update(BulkDagExecuter('bulk_dag_executer_config.yaml').create_dags())
