import logging
from typing import List, Dict, Optional, Union

from airflow.sensors.base import BaseSensorOperator
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from airflow import settings

logger = logging.getLogger(__name__)


class ExternalTaskPokeSensor(BaseSensorOperator):
    """
    ExternalTaskPokeSensor is a custom implementation of the ExternalTaskSensor which monitors
    the task(s) of external DAG(s) within a given time window.

    Supports two modes:
    1. Single pair mode: Provide `external_dag_id` and `external_task_id` for a single DAG/task pair
    2. Multiple pairs mode: Provide `external_tasks` as a list of dicts with 'dag_id' and 'task_id' keys

    Examples:
        # Single pair mode (backward compatible)
        sensor = ExternalTaskPokeSensor(
            task_id='wait_for_task',
            external_dag_id='my_dag',
            external_task_id='my_task',
            start_date_delta=timedelta(hours=-1),
            end_date_delta=timedelta(hours=1),
        )

        # Multiple pairs mode
        sensor = ExternalTaskPokeSensor(
            task_id='wait_for_tasks',
            external_tasks=[
                {'dag_id': 'dag_1', 'task_id': 'task_1'},
                {'dag_id': 'dag_2', 'task_id': 'task_2'},
            ],
            start_date_delta=timedelta(hours=-1),
            end_date_delta=timedelta(hours=1),
        )

    Args:
        external_dag_id: External DAG ID to monitor (single pair mode)
        external_task_id: External task ID to monitor (single pair mode)
        external_tasks: List of dicts with 'dag_id' and 'task_id' keys (multiple pairs mode)
        allowed_status: Allowed status for the task (default: SUCCESS)
        start_date_delta: Start of the time window relative to execution date
        end_date_delta: End of the time window relative to execution date
    """

    def __init__(
        self,
        external_dag_id: Optional[str] = None,
        external_task_id: Optional[str] = None,
        external_tasks: Optional[List[Dict[str, str]]] = None,
        allowed_status: Optional[List[str]] = None,
        start_date_delta=None,
        end_date_delta=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        # Validate inputs
        if external_tasks and (external_dag_id or external_task_id):
            raise ValueError(
                "Cannot specify both 'external_tasks' and 'external_dag_id'/'external_task_id'. "
                "Use either single pair mode OR multiple pairs mode."
            )

        if not external_tasks and not (external_dag_id and external_task_id):
            raise ValueError(
                "Must specify either 'external_tasks' list OR both 'external_dag_id' and 'external_task_id'."
            )

        # Normalize to a list of task pairs for unified processing
        if external_tasks:
            self._validate_external_tasks(external_tasks)
            self.external_tasks = external_tasks
        else:
            self.external_tasks = [{'dag_id': external_dag_id, 'task_id': external_task_id}]

        # Keep original attributes for backward compatibility
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.allowed_status = allowed_status or [State.SUCCESS]
        self.start_date_delta = start_date_delta
        self.end_date_delta = end_date_delta

    def _validate_external_tasks(self, external_tasks: List[Dict[str, str]]) -> None:
        """Validate the external_tasks list structure."""
        if not isinstance(external_tasks, list) or len(external_tasks) == 0:
            raise ValueError("'external_tasks' must be a non-empty list.")

        for idx, task_pair in enumerate(external_tasks):
            if not isinstance(task_pair, dict):
                raise ValueError(f"Item at index {idx} in 'external_tasks' must be a dict.")
            if 'dag_id' not in task_pair or 'task_id' not in task_pair:
                raise ValueError(
                    f"Item at index {idx} in 'external_tasks' must have 'dag_id' and 'task_id' keys."
                )

    def _check_single_task(
        self,
        session,
        dag_id: str,
        task_id: str,
        start_date,
        end_date
    ) -> bool:
        """
        Check if a single external task has succeeded within the time window.

        Returns:
            True if the task is in SUCCESS state, False otherwise.
        """
        dag_runs = DagRun.find(dag_id=dag_id, state=State.SUCCESS)
        filtered_dag_runs = sorted(
            [dag_run for dag_run in dag_runs if start_date <= dag_run.execution_date <= end_date],
            key=lambda run: run.execution_date,
            reverse=True
        )
        dag_run = filtered_dag_runs[0] if filtered_dag_runs else None

        if not dag_run:
            logging.info(f"No successful DAG run found for DAG '{dag_id}' in time window.")
            return False

        task_instance = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.task_id == task_id,
            TaskInstance.execution_date == dag_run.execution_date,
        ).first()

        if not task_instance:
            logging.info(
                f"No Task Instance found for task '{task_id}' in DAG '{dag_id}' "
                f"with execution date {dag_run.execution_date}."
            )
            return False

        if task_instance.state == State.SUCCESS:
            logging.info(
                f"Task '{task_id}' in DAG '{dag_id}' is in state {task_instance.state}"
            )
            return True
        else:
            logging.info(
                f"Task '{task_id}' in DAG '{dag_id}' is in state {task_instance.state}, waiting..."
            )
            return False

    def poke(self, context) -> bool:
        """
        Check if all external tasks have succeeded.

        Returns:
            True only if ALL specified external tasks are in SUCCESS state.
        """
        skip_sensor_task = context["dag_run"].conf.get("skip_sensor_task")
        if skip_sensor_task:
            logging.info(f"Skipping Sensor task: {skip_sensor_task}")
            return True

        execution_date = context['execution_date']
        logging.info(f"execution_date: {execution_date}")

        start_date = execution_date + self.start_date_delta
        end_date = execution_date + self.end_date_delta
        logging.info(f"start_date: {start_date}")
        logging.info(f"end_date: {end_date}")

        session = settings.Session()
        try:
            all_tasks_successful = True
            for task_pair in self.external_tasks:
                dag_id = task_pair['dag_id']
                task_id = task_pair['task_id']
                logging.info(f"Checking external task: DAG='{dag_id}', Task='{task_id}'")

                if not self._check_single_task(session, dag_id, task_id, start_date, end_date):
                    all_tasks_successful = False
                    # Continue checking remaining tasks to log their status

            if all_tasks_successful:
                logging.info("All external tasks have succeeded.")
            else:
                logging.info("Not all external tasks have succeeded yet, waiting...")

            return all_tasks_successful
        finally:
            session.close()
