"""
This module defines the DAG for Jira Analytics and its components.
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

from jira_analytics.etl.extract import extract_closed_sprints_issues, extract_sprint, extract_active_sprint_issues, extract_bugs
from jira_analytics.utils.utils import handle_epic_key_change, extract_csv_to_bq
from jira_analytics.config import JiraAnalyticsConfig as Config
from jira_analytics.etl import (
    extract_from_jira_cloud,
    extract_last_successful_dag_run_time,
    load_records,
    curate_changelogs,
)
from jira_analytics.model.jira.response import (
    InitiativeResponse,
    EpicResponse,
    StoryResponse,
)
from jira_analytics.model.terminus.issue import Initiative, Epic, Story
from jira_analytics.utils import as_naive_utc, execute_sql_file
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
    and records the current time stamp, such that all extraction tasks for the Jira
    Analytics will have the same cutoff time.

    The timestamp is in string %Y-%m-%d %H:%M format, as required by Jira Cloud.

    :return: A tuple, ([last successful DAG run time, determined cutoff time])
    """
    if Config.UPDATES_ONLY:
        # Extract the time stamp for the last successful DAG run.
        last_successful_dag_run_time = extract_last_successful_dag_run_time()
        if last_successful_dag_run_time is not None:
            last_successful_dag_run_time = datetime.strftime(
                last_successful_dag_run_time, "%Y-%m-%d %H:%M%z"
            )
    else:
        last_successful_dag_run_time = None
    update_end_time = datetime.strftime(
        datetime.now(tz=pendulum.timezone("America/Toronto")), "%Y-%m-%d %H:%M%z"
    )
    logging.info(
        f"DELTA run times are BEGIN=({last_successful_dag_run_time}) and END=({update_end_time})"
    )
    return last_successful_dag_run_time, update_end_time


def set_outcome_teams() -> None:
    """
    This functions loads the outcome team csv into the bq table
    """
    csv_path = Config.OUTCOME_TEAMS_CSV_PATH
    table_ref = Config.LANDING_TABLES.OUTCOME_TEAM

    extract_csv_to_bq(csv_path, table_ref)


def load_jira_initiatives(last_dag_run_time: str, cutoff_time: str, jira_config: Dict[str, str]) -> None:
    """
    This function extracts the initiatives data from the JIRA cloud and loads
    it to the landing zone.

    :param last_dag_run_time: The time stamp for the last successful DAG run.
    :param cutoff_time: The cutoff time for the current DAG run.
    :param jira_config: The configuration for Jira Cloud in python dictionary format.
    """
    start, chunk = 0, 0
    next_page_token = None
    jira_config["password"] = VaultUtilBuilder.build().get_secret(
        Config.PASSWORD_PATH, Config.PASSWORD_KEY
    )

    while True:
        (
            issues,
            changelog_counts,
            changelog_events,
            total,
            next_page_token,
        ) = extract_from_jira_cloud(
            jira_config=jira_config,
            response_class=InitiativeResponse,
            terminus_class=Initiative,
            next_page_token=next_page_token,
            limit=Config.CHUNK_SIZE,
            update_window_start=last_dag_run_time,
            update_window_end=cutoff_time,
        )
        load_records(
            records=issues, table_id=Config.LANDING_TABLES.INITIATIVE, chunk=chunk
        )
        load_records(
            records=changelog_events,
            table_id=Config.LANDING_TABLES.CHANGELOG_EVENT,
            chunk=chunk,
        )
        load_records(
            records=changelog_counts,
            table_id=Config.LANDING_TABLES.CHANGELOG_COUNT,
            chunk=chunk,
        )

        logging.info(f"Loaded {total} initiatives from jira starting from {start} .")
        chunk += 1
        start += total

        if next_page_token is None:
            break


def load_jira_epics(
    last_dag_run_time: str, cutoff_time: str, jira_config: Dict[str, str]
) -> None:
    """
    This function extracts the epics data from the JIRA cloud and loads
    it to the landing zone.

    :param last_dag_run_time: The time stamp for the last successful DAG run.
    :param cutoff_time: The cutoff time for the current DAG run.
    :param jira_config: The configuration for Jira Cloud in python dictionary format.
    """
    start, chunk = 0, 0
    next_page_token = None
    jira_config["password"] = VaultUtilBuilder.build().get_secret(
        Config.PASSWORD_PATH, Config.PASSWORD_KEY
    )
    while True:
        (
            issues,
            changelog_counts,
            changelog_events,
            total,
            next_page_token,
        ) = extract_from_jira_cloud(
            jira_config=jira_config,
            response_class=EpicResponse,
            terminus_class=Epic,
            next_page_token=next_page_token,
            limit=Config.CHUNK_SIZE,
            update_window_start=last_dag_run_time,
            update_window_end=cutoff_time,
        )
        load_records(records=issues, table_id=Config.LANDING_TABLES.EPIC, chunk=chunk)
        load_records(
            records=changelog_events,
            table_id=Config.LANDING_TABLES.CHANGELOG_EVENT,
            chunk=chunk,
        )
        load_records(
            records=changelog_counts,
            table_id=Config.LANDING_TABLES.CHANGELOG_COUNT,
            chunk=chunk,
        )

        logging.info(f"Loaded {start} of {total} epics from jira.")
        start += total
        chunk += 1

        # Check for epic key changes
        for changelog_event in changelog_events:
            if changelog_event.field == "Key" and changelog_event.field_type == "jira":
                old_key, new_key = (
                    changelog_event.from_string,
                    changelog_event.to_string,
                )
                logging.info(f"Epic key change process for {old_key} to {new_key}")
                handle_epic_key_change(old_key)

        if next_page_token is None:
            break


def load_jira_stories(
    last_dag_run_time: str, cutoff_time: str, jira_config: Dict[str, str]
) -> None:
    """
    This function extracts the stories data from the JIRA cloud and loads
    it to the landing zone.

    :param last_dag_run_time: The time stamp for the last successful DAG run.
    :param cutoff_time: The cutoff time for the current DAG run.
    :param jira_config: The configuration for Jira Cloud in python dictionary format.
    """
    start, chunk = 0, 0
    next_page_token = None
    jira_config["password"] = VaultUtilBuilder.build().get_secret(
        Config.PASSWORD_PATH, Config.PASSWORD_KEY
    )
    while True:
        (
            issues,
            changelog_counts,
            changelog_events,
            total,
            next_page_token,
        ) = extract_from_jira_cloud(
            jira_config=jira_config,
            response_class=StoryResponse,
            terminus_class=Story,
            next_page_token=next_page_token,
            limit=Config.CHUNK_SIZE,
            update_window_start=last_dag_run_time,
            update_window_end=cutoff_time,
        )
        load_records(records=issues, table_id=Config.LANDING_TABLES.STORY, chunk=chunk)
        load_records(
            records=changelog_events,
            table_id=Config.LANDING_TABLES.CHANGELOG_EVENT,
            chunk=chunk,
        )
        load_records(
            records=changelog_counts,
            table_id=Config.LANDING_TABLES.CHANGELOG_COUNT,
            chunk=chunk,
        )
        # start = end
        logging.info(f"Loaded {start} of {total} stories from jira.")
        start += total
        chunk += 1
        if next_page_token is None:
            break


def load_sprint_info(jira_config: Dict[str, str]) -> None:
    """
    This function loads the sprint data into bq

    :param jira_config: The configuration for Jira Cloud in python dictionary format.
    """
    jira_config["password"] = VaultUtilBuilder.build().get_secret(Config.PASSWORD_PATH, Config.PASSWORD_KEY)
    sprints = extract_sprint(jira_config=jira_config)

    load_records(records=sprints, table_id=Config.LANDING_TABLES.SPRINT, chunk=0, force_truncate=True)


def load_sprint_issues(jira_config: Dict[str, str]) -> None:
    """
    This function loads the issue and sprint data into bq

    :param jira_config: The configuration for Jira Cloud in python dictionary format.
    """
    jira_config["password"] = VaultUtilBuilder.build().get_secret(Config.PASSWORD_PATH, Config.PASSWORD_KEY)

    # retrieving active and closed issues
    active_issues = extract_active_sprint_issues(jira_config=jira_config)
    load_records(records=active_issues, table_id=Config.LANDING_TABLES.ISSUES_IN_ACTIVE_SPRINTS, chunk=0, force_truncate=True)

    closed_issues = extract_closed_sprints_issues(jira_config=jira_config)
    load_records(records=closed_issues, table_id=Config.LANDING_TABLES.ISSUES_IN_CLOSED_SPRINTS, chunk=0, force_truncate=True)


def load_bug_issues(jira_config: Dict[str, str]) -> None:
    """
    This function loads the bug  data into bq

    :param jira_config: The configuration for Jira Cloud in python dictionary format.
    """
    jira_config["password"] = VaultUtilBuilder.build().get_secret(Config.PASSWORD_PATH, Config.PASSWORD_KEY)

    # retrieving Bug  issues
    bug_issues = extract_bugs(jira_config=jira_config)
    load_records(records=bug_issues, table_id=Config.LANDING_TABLES.BUG, chunk=0, force_truncate=True)


def load_dag_run_time(current_dag_run_time: str) -> None:
    """
    This function performs the upload of current dag completion time to the bq
    """

    @dataclass(init=False)
    class JiraLoadRecord:
        """
        The JiraLoadRecord class defines the schema of the table storing the successful JQL execution history
        of the Jira Analytics DAG.
        """

        # The timestamp at which the JQL was queried.
        dag_run_time: datetime

        def __init__(self, dag_run_time: str):
            self.dag_run_time = as_naive_utc(
                datetime.strptime(dag_run_time, "%Y-%m-%d %H:%M%z")
            )

    load_record_to_bq(
        table_id=Config.LANDING_TABLES.DAG_HISTORY,
        o=JiraLoadRecord(dag_run_time=current_dag_run_time),
    )


with DAG(
    dag_id="jira_load_dag",
    start_date=datetime(2023, 1, 1, tzinfo=pendulum.timezone("America/Toronto")),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "team-centaurs",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "capability": "Terminus Data Platform",
        "severity": "P4",
        "sub_capability": "TBD",
        "business_impact": "Jira data loaded for Jira anlytics",
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

        create_landing_tables_task = PythonOperator(
            task_id="create_landing_tables_task",
            op_kwargs={"file_name": "ddl_queries/create_landing_tables.sql"},
            python_callable=execute_sql_file,
        )

        get_jira_cloud_config_task = PythonOperator(
            task_id="get_jira_cloud_config_task", python_callable=get_jira_cloud_config
        )

        set_outcome_teams = PythonOperator(
            task_id="set_outcome_teams_task",
            python_callable=set_outcome_teams
        )

    with TaskGroup(
        group_id="extract_load_stage",
    ) as extract_load_stage:
        op_kwargs = {
            "last_dag_run_time": "{{ti.xcom_pull(task_ids='setup_stage.get_dag_time_task')[0]}}",
            "cutoff_time": "{{ti.xcom_pull(task_ids='setup_stage.get_dag_time_task')[1]}}",
            "jira_config": "{{ti.xcom_pull(task_ids='setup_stage.get_jira_cloud_config_task')}}",
        }

        get_jira_initiatives_task = PythonOperator(
            task_id="get_jira_initiatives_task",
            op_kwargs=op_kwargs,
            python_callable=load_jira_initiatives,
            retry_delay=timedelta(seconds=30),
        )

        get_jira_epics_task = PythonOperator(
            task_id="get_jira_epics_task",
            op_kwargs=op_kwargs,
            python_callable=load_jira_epics,
            retry_delay=timedelta(seconds=30),
        )

        get_jira_stories_task = PythonOperator(
            task_id="get_jira_stories_task",
            op_kwargs=op_kwargs,
            python_callable=load_jira_stories,
            retry_delay=timedelta(seconds=30),
        )

        get_sprint_info_task = PythonOperator(
            task_id="get_sprint_info_task",
            op_kwargs=op_kwargs,
            python_callable=load_sprint_info,
            retry_delay=timedelta(seconds=30)
        )

        get_sprint_issues_task = PythonOperator(
            task_id="get_sprint_issues_task",
            op_kwargs=op_kwargs,
            python_callable=load_sprint_issues,
            retry_delay=timedelta(seconds=30)
        )

        get_bug_issues_task = PythonOperator(
            task_id="get_bug_issues_task",
            op_kwargs=op_kwargs,
            python_callable=load_bug_issues,
            retry_delay=timedelta(seconds=30)
        )

        load_dag_time_to_landing_task = PythonOperator(
            task_id="load_dag_time_to_landing_task",
            op_kwargs={
                "current_dag_run_time": "{{(ti.xcom_pull(task_ids='setup_stage.get_dag_time_task'))[1]}}"
            },
            python_callable=load_dag_run_time,
        )

        jira_upstream_tasks = [
            get_jira_initiatives_task,
            get_jira_epics_task,
            get_jira_stories_task,
            get_sprint_info_task
        ]
        jira_upstream_tasks >> get_sprint_issues_task >> get_bug_issues_task >> load_dag_time_to_landing_task

    with TaskGroup(
        group_id="curation_stage_1",
    ) as curation_stage_1:
        # This creates or replace all the curated tables.
        create_curated_tables_task = PythonOperator(
            task_id="create_curated_tables_task",
            op_kwargs={"file_name": "ddl_queries/create_curated_tables.sql"},
            python_callable=execute_sql_file,
        )

        # This curates the unique_issues table
        curate_unique_issues_task = PythonOperator(
            task_id="curate_unique_issues_task",
            op_kwargs={"file_name": "transformations/curate_unique_issues.sql"},
            python_callable=execute_sql_file,
        )

        # This curates the three changelog tables (STATUS, T_SHIRT_SIZE, STORY_POINT)
        curate_changelogs_task = PythonOperator(
            task_id="curate_changelogs_task", python_callable=curate_changelogs
        )

        (
            create_curated_tables_task
            >> curate_unique_issues_task
            >> curate_changelogs_task
        )

    with TaskGroup(
        group_id="curation_stage_2",
    ) as curation_stage_2:
        # This curates the first_open_date column in unique_issues table
        curate_first_open_date_task = PythonOperator(
            task_id="curate_first_open_date_task",
            op_kwargs={"file_name": "transformations/curate_first_open_date.sql"},
            python_callable=execute_sql_file,
        )

        # This curates the final_close_date column in unique_issues table
        curate_final_close_date_task = PythonOperator(
            task_id="curate_final_close_date_task",
            op_kwargs={"file_name": "transformations/curate_final_close_date.sql"},
            python_callable=execute_sql_file,
        )

        # This curates the mean_size column in unique_issues table
        # and from_mean_size and to_mean_size columns in T_SHIRT_SIZE_CHANGELOG table
        curate_mean_size_task = PythonOperator(
            task_id="curate_mean_size_task",
            op_kwargs={"file_name": "transformations/curate_mean_size.sql"},
            python_callable=execute_sql_file,
        )

        # This curates the story allocation table
        curate_outcome_teams_story_allocation_task = PythonOperator(
            task_id="curate_outcome_teams_story_allocation_task",
            op_kwargs={"file_name": "transformations/curate_outcome_teams_story_allocation.sql"},
            python_callable=execute_sql_file,
        )

        # This curates outcome team bug table
        curate_outcome_teams_bug_task = PythonOperator(
            task_id="curate_outcome_teams_bug_task",
            op_kwargs={"file_name": "transformations/curate_outcome_teams_bug.sql"},
            python_callable=execute_sql_file,
        )

    with TaskGroup(
        group_id="curation_stage_3",
    ) as curation_stage_3:
        # This curates the burndown_event table
        curate_burndown_event_task = PythonOperator(
            task_id="curate_burndown_event_task",
            op_kwargs={"file_name": "transformations/curate_burndown_event.sql"},
            python_callable=execute_sql_file,
        )

        # This curates the total_size column in unique_issues table
        curate_total_size_task = PythonOperator(
            task_id="curate_total_size_task",
            op_kwargs={"file_name": "transformations/curate_total_size.sql"},
            python_callable=execute_sql_file,
        )

        # This curates the total_completed_size column in unique_issues table
        curate_completed_size = PythonOperator(
            task_id="curate_completed_size",
            op_kwargs={"file_name": "transformations/curate_completed_size.sql"},
            python_callable=execute_sql_file,
        )

        # This curates the target_start_date column in unique_issues table
        curate_target_start_date_task = PythonOperator(
            task_id="curate_target_start_date_task",
            op_kwargs={"file_name": "transformations/curate_target_start_date.sql"},
            python_callable=execute_sql_file,
        )

    with TaskGroup(
        group_id="curation_stage_4",
    ) as curation_stage_4:
        # This curates the target_start_date column in unique_issues table
        curate_lead_time_task = PythonOperator(
            task_id="curate_lead_time_task",
            op_kwargs={"file_name": "transformations/curate_lead_time.sql"},
            python_callable=execute_sql_file,
        )

        # This curates the target_start_date column in unique_issues table
        curate_target_completion_date_task = PythonOperator(
            task_id="curate_target_completion_date_task",
            op_kwargs={
                "file_name": "transformations/curate_target_completion_date.sql"
            },
            python_callable=execute_sql_file,
        )

        # This curates the target_burndown table
        curate_target_burndown_task = PythonOperator(
            task_id="curate_target_burndown_task",
            op_kwargs={"file_name": "transformations/curate_target_burndown.sql"},
            python_callable=execute_sql_file,
        )

        # This curates the forecasted_burndown_rate column in unique_issues table
        curate_forecasted_burndown_rate_task = PythonOperator(
            task_id="curate_forecasted_burndown_rate_task",
            op_kwargs={
                "file_name": "transformations/curate_forecasted_burndown_rate.sql"
            },
            python_callable=execute_sql_file,
        )

        # This curates the forecasted_completion_date column in unique_issues table
        curate_forecasted_completion_date_task = PythonOperator(
            task_id="curate_forecasted_completion_date_task",
            op_kwargs={
                "file_name": "transformations/curate_forecasted_completion_date.sql"
            },
            python_callable=execute_sql_file,
        )

        # This curates the forecasted_slack column in unique_issues table
        curate_forecasted_slack_task = PythonOperator(
            task_id="curate_forecasted_slack_task",
            op_kwargs={"file_name": "transformations/curate_forecasted_slack.sql"},
            python_callable=execute_sql_file,
        )

        # This curates the forecasted_burndown table
        curate_forecasted_burndown_task = PythonOperator(
            task_id="curate_forecasted_burndown_task",
            op_kwargs={"file_name": "transformations/curate_forecasted_burndown.sql"},
            python_callable=execute_sql_file,
        )

        curate_target_completion_date_task >> [
            curate_forecasted_slack_task,
            curate_target_burndown_task
        ]

        (
            curate_forecasted_burndown_rate_task
            >> curate_forecasted_completion_date_task
            >> curate_forecasted_burndown_task
            >> curate_forecasted_slack_task
        )

    with TaskGroup(
        group_id="looker_curation_stage",
    ) as looker_curation_stage:
        # This curates the looker_epic_burndown_events table
        curate_looker_epic_burndown_events_task = PythonOperator(
            task_id="curate_looker_epic_burndown_events_task",
            op_kwargs={"file_name": "looker_views/looker_epic_burndown_events.sql"},
            python_callable=execute_sql_file,
        )

        # This curates the looker_initiative_burndown_chart table
        curate_looker_initiative_burndown_chart_task = PythonOperator(
            task_id="curate_looker_initiative_burndown_chart_task",
            op_kwargs={
                "file_name": "looker_views/looker_initiative_burndown_chart.sql"
            },
            python_callable=execute_sql_file,
        )

        # This curates the looker_cumulative_flow table
        curate_looker_cumulative_flow_task = PythonOperator(
            task_id="curate_looker_cumulative_flow_task",
            op_kwargs={"file_name": "looker_views/looker_cumulative_flow.sql"},
            python_callable=execute_sql_file,
        )

        # This curates the looker_outcome_teams_completed_story_points table
        curate_looker_outcome_teams_completed_story_points = PythonOperator(
            task_id='curate_looker_outcome_teams_completed_story_points',
            op_kwargs={"file_name": 'looker_views/looker_outcome_teams_completed_story_points.sql'},
            python_callable=execute_sql_file
        )

        # This curates the looker_story_point_burndown_chart table
        curate_looker_story_point_burndown_chart_task = PythonOperator(
            task_id="curate_looker_story_point_burndown_chart_task",
            op_kwargs={
                "file_name": "looker_views/looker_story_point_burndown_chart.sql"
            },
            python_callable=execute_sql_file,
        )

        # This curates the looker_active_sprint_allocation table
        curate_looker_active_sprint_allocation_task = PythonOperator(
            task_id="curate_looker_active_sprint_allocation_task",
            op_kwargs={
                "file_name": "looker_views/looker_active_sprint_allocation.sql"
            },
            python_callable=execute_sql_file,
        )

        # This curates the looker_combined_initiative_allocation table
        curate_looker_combined_initiative_allocation_task = PythonOperator(
            task_id="curate_looker_combined_initiative_allocation_task",
            op_kwargs={
                "file_name": "looker_views/looker_combined_initiative_allocation.sql"
            },
            python_callable=execute_sql_file,
        )

        # This curates the tableau_story_allocation table
        curate_tableau_story_allocation_task = PythonOperator(
            task_id="curate_tableau_story_allocation_task",
            op_kwargs={
                "file_name": "looker_views/tableau_story_allocation.sql"
            },
            python_callable=execute_sql_file
        )

    # Tasks Connection
    (
        start_task
        >> setup_stage
        >> extract_load_stage
        >> curation_stage_1
        >> curation_stage_2
        >> curation_stage_3
        >> curation_stage_4
        >> looker_curation_stage
        >> end_task
    )
