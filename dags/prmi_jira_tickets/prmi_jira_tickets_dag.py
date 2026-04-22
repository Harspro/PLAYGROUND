"""
This module defines the DAG for PRMI Jira Tickets and its components.
"""

import logging
from dataclasses import dataclass
from datetime import timedelta, datetime
from typing import Tuple, Optional, Dict

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from prmi_jira_tickets.etl.extract import extract_prmi_tickets_from_jira_cloud, extract_last_successful_dag_run_time
from prmi_jira_tickets.etl.load import load_records
from prmi_jira_tickets.config import PRMIJiraTicketsConfig as Config
from prmi_jira_tickets.utils import as_naive_utc
from util.bq_utils import load_record_to_bq
from util.vault_util import VaultUtilBuilder
from dag_factory.terminus_dag_factory import add_tags


def get_jira_cloud_config() -> Dict[str, str]:
    """
    This function returns the configuration for the Jira cloud.

    :return: The configuration for Jira Cloud in python dictionary format.
    """
    return {
        "url": Config.JIRA_CLOUD_URL,
        "username": Config.USER_NAME,
        "password": None,  # Password shall NEVER, EVER be placed in the return value of a task.
        "cloud": True,
        "advanced_mode": False,
        "verify_ssl": False,
    }


def get_delta_run_timestamp() -> Tuple[Optional[str], Optional[str]]:
    """
    This function extracts the timestamp for the last successful DAG run if needed
    and records the current time stamp, such that all extraction tasks for the PRMI
    Jira Tickets will have the same cutoff time.

    The timestamp is in string %Y-%m-%d format for date filtering.

    :return: A tuple, ([last successful DAG run time, determined cutoff time])
    """
    if Config.UPDATES_ONLY:
        # Extract the time stamp for the last successful DAG run.
        last_successful_dag_run_time = extract_last_successful_dag_run_time()
        if last_successful_dag_run_time is not None:
            # Format for JQL: "YYYY-MM-DD HH:MM" (remove timezone for JQL compatibility)
            last_successful_dag_run_time = datetime.strftime(
                last_successful_dag_run_time, "%Y-%m-%d %H:%M"
            )
    else:
        last_successful_dag_run_time = None

    # Default to last DEFAULT_DAYS_BACK if no previous run
    if last_successful_dag_run_time is None:
        default_start = datetime.now(tz=pendulum.timezone("America/Toronto")) - timedelta(
            days=Config.DEFAULT_DAYS_BACK
        )
        last_successful_dag_run_time = datetime.strftime(
            default_start, "%Y-%m-%d %H:%M"
        )

    update_end_time = datetime.strftime(
        datetime.now(tz=pendulum.timezone("America/Toronto")), "%Y-%m-%d %H:%M"
    )
    logging.info(
        f"DELTA run times are BEGIN=({last_successful_dag_run_time}) and END=({update_end_time})"
    )
    return last_successful_dag_run_time, update_end_time


def load_prmi_tickets(
    start_date: str, end_date: str, jira_config: Dict[str, str]
) -> None:
    """
    This function extracts the PRMI tickets data from the JIRA cloud and loads
    it to the landing zone.

    :param start_date: The start date for filtering PRMI tickets.
    :param end_date: The end date for filtering PRMI tickets.
    :param jira_config: The configuration for Jira Cloud in python dictionary format.
    """
    start, chunk = 0, 0
    jira_config["password"] = VaultUtilBuilder.build().get_secret(
        Config.PASSWORD_PATH, Config.PASSWORD_KEY
    )

    while True:
        tickets, total = extract_prmi_tickets_from_jira_cloud(
            jira_config=jira_config,
            start_date=start_date,
            end_date=end_date,
            limit=Config.CHUNK_SIZE,
        )

        if len(tickets) > 0:
            load_records(
                records=tickets, table_id=Config.LANDING_TABLES.PRMI_TICKET, chunk=chunk
            )

        logging.info(f"Loaded {total} PRMI tickets from jira starting from {start}.")
        chunk += 1
        start += total

        if total == 0 or len(tickets) == 0:
            break


def load_dag_run_time(current_dag_run_time: str) -> None:
    """
    This function performs the upload of current dag completion time to the bq
    """

    @dataclass(init=False)
    class PRMILoadRecord:
        """
        The PRMILoadRecord class defines the schema of the table storing the successful JQL execution history
        of the PRMI Jira Tickets DAG.
        """

        # The timestamp at which the JQL was queried.
        dag_run_time: datetime

        def __init__(self, dag_run_time: str):
            self.dag_run_time = as_naive_utc(
                datetime.strptime(dag_run_time, "%Y-%m-%d %H:%M")
            )

    load_record_to_bq(
        table_id=Config.LANDING_TABLES.DAG_HISTORY,
        o=PRMILoadRecord(dag_run_time=current_dag_run_time),
    )


with DAG(
    dag_id="prmi_jira_tickets_dag",
    start_date=datetime(2023, 1, 1, tzinfo=pendulum.timezone("America/Toronto")),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "team-growth-and-sales-alerts",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "capability": "Terminus Data Platform",
        "severity": "P4",
        "sub_capability": "TBD",
        "business_impact": "PRMI tickets data loaded for analytics",
        "customer_impact": "TBD",
        "retry_delay": timedelta(seconds=10)
    },
    schedule=Config.UPDATE_CRON_SCHEDULE,
    is_paused_upon_creation=True,
    render_template_as_native_obj=True,
) as dag:
    add_tags(dag)
    start_task = EmptyOperator(task_id="start", dag=dag)
    end_task = EmptyOperator(task_id="end", dag=dag)

    with TaskGroup(
        group_id="setup_stage",
    ) as setup_stage:
        get_dag_time_task = PythonOperator(
            task_id="get_dag_time_task", python_callable=get_delta_run_timestamp
        )

        get_jira_cloud_config_task = PythonOperator(
            task_id="get_jira_cloud_config_task", python_callable=get_jira_cloud_config
        )

    with TaskGroup(
        group_id="extract_load_stage",
    ) as extract_load_stage:
        op_kwargs = {
            "start_date": "{{ti.xcom_pull(task_ids='setup_stage.get_dag_time_task')[0]}}",
            "end_date": "{{ti.xcom_pull(task_ids='setup_stage.get_dag_time_task')[1]}}",
            "jira_config": "{{ti.xcom_pull(task_ids='setup_stage.get_jira_cloud_config_task')}}",
        }

        get_prmi_tickets_task = PythonOperator(
            task_id="get_prmi_tickets_task",
            op_kwargs=op_kwargs,
            python_callable=load_prmi_tickets,
            retry_delay=timedelta(seconds=30),
        )

        load_dag_time_to_landing_task = PythonOperator(
            task_id="load_dag_time_to_landing_task",
            op_kwargs={
                "current_dag_run_time": "{{(ti.xcom_pull(task_ids='setup_stage.get_dag_time_task'))[1]}}"
            },
            python_callable=load_dag_run_time,
        )

        get_prmi_tickets_task >> load_dag_time_to_landing_task

    # Tasks Connection
    (
        start_task
        >> setup_stage
        >> extract_load_stage
        >> end_task
    )
