"""
This module defines the helper functions used in Jira Analytics.
"""

import logging
from datetime import datetime
from datetime import timedelta
from typing import List, Optional, Tuple

import pendulum
import pytz
from airflow.exceptions import AirflowFailException
from airflow.settings import DAGS_FOLDER
from atlassian import Jira
from dateutil.parser import isoparse
from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter, QueryJobConfig
from jinja2 import Template

from jira_analytics.config import JiraAnalyticsConfig as Config
from jira_analytics.model.terminus.bug import Bug
from jira_analytics.model.terminus.sprint_issues import SprintIssue
from util.bq_utils import run_bq_query, run_bq_query_with_params


def execute_sql_file(file_name: str, param_object=None, verbose=True) -> None:
    """
    This function run the queries in a .sql file on BigQuery.
    Replaces parameter placeholders in the query strings using Jinja2 rendering.

    :param file_name: Name of the file with .sql extension located in jira_analytics/etl/sql_queries
    :param param_object: A class containing attributes of type string where the attribute
     name is the parameter name and the attribute value is the parameter value
    :param verbose: If True, logs the query sent to BigQuery
    """

    query_path = f"{DAGS_FOLDER}/jira_analytics/etl/sql_queries/{file_name}"

    with open(query_path) as sql_file:
        query = sql_file.read()

    query_template = Template(query)

    rendered_query = query_template.render(
        curated=Config.CURATED_TABLES, landing=Config.LANDING_TABLES, params=param_object
    )

    if verbose:
        logging.info("Running query:\n" + rendered_query)

    run_bq_query(rendered_query)


def as_naive_utc(dt: datetime) -> datetime:
    """
    This function converts the argument to UTC and strips the timezone information.
    This is used to mitigate problems when converting and uploading datetime values
    into a BigQuery Timestamp field.

    :param dt: Any datetime object to be converted
    :return: The argument's UTC time as a naive datetime object
    """
    return dt.astimezone(pendulum.timezone("UTC")).replace(tzinfo=None)


def handle_epic_key_change(old_epic_key: str) -> None:
    """
    Handles the update process when an epic key changes, including updating changelog events,
    changelog counts, stories, and removing old epic data from relevant tables.

    :param old_epic_key: The old epic key that needs to be replaced.
    :return: None
    """
    try:
        # Define the tables from the configuration and their respective key fields
        tables = {
            "changelog_count": (Config.LANDING_TABLES.CHANGELOG_COUNT, "issue_key"),
            "changelog_event": (Config.LANDING_TABLES.CHANGELOG_EVENT, "issue_key"),
            "epic": (Config.LANDING_TABLES.EPIC, "issue_key"),
            "story": (Config.LANDING_TABLES.STORY, "epic_key"),
        }

        query_params = [ScalarQueryParameter("old_epic_key", "STRING", old_epic_key)]
        job_config = QueryJobConfig(query_parameters=query_params)

        # Execute all delete queries
        for name, (table, key_field) in tables.items():
            query = f"DELETE FROM `{table}` WHERE {key_field} = @old_epic_key;"
            run_bq_query_with_params(query, job_config)
            logging.debug(f"Deleted {old_epic_key} from {name} table.")

    except Exception as e:
        logging.error(
            f"An error occurred while processing the key change for {old_epic_key}: {e}"
        )
        raise


def extract_csv_to_bq(csv_path: str, table_ref: str) -> None:
    """
    This function populates a table in BigQuery with the values from a CSV located at the provided path csv_path,
    It truncates and overwrites the table if it exists with the new values from the CSV.

    :param csv_path: The path to the CSV file.
    :param table_ref: The BigQuery reference
    """
    bq_client = bigquery.Client()

    # Configure the job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skipping header row
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite existing table data
        autodetect=True,
    )

    try:
        with open(csv_path, "rb") as csv_file:
            load_job = bq_client.load_table_from_file(
                csv_file, table_ref, job_config=job_config
            )

        load_job.result()  # Waits for the job to complete.
        logging.info(f"Loaded {load_job.output_rows} rows into {table_ref}.")

    except FileNotFoundError:
        raise AirflowFailException(f"The file {csv_path} was not found.")
    except Exception as e:
        raise AirflowFailException(f"An unexpected error occurred: {e}")


def convert_utc_to_est(utc_datetime):
    """
    Convert a UTC datetime object to US Eastern Time (EST/EDT).

    :param utc_datetime: The datetime object in UTC.
    :returns: The converted datetime in US Eastern Time, or None if conversion fails.
    """
    try:
        # Ensure the datetime is timezone-aware
        if utc_datetime.tzinfo is None:
            # Assume UTC if no timezone info is present
            utc_timezone = pytz.timezone("UTC")
            utc_datetime = utc_timezone.localize(utc_datetime)
        else:
            # Convert to UTC if it's timezone-aware but not UTC
            utc_datetime = utc_datetime.astimezone(pytz.UTC)

        # Define Eastern Timezone
        eastern_timezone = pytz.timezone("America/Toronto")

        # Convert UTC to Eastern Time
        est_time = utc_datetime.astimezone(eastern_timezone)

        return est_time

    except Exception as e:
        logging.error(f"Error converting time: {e}")
        return None


def get_env(envs: []) -> str:
    result_env = ""
    if envs is not None:
        for env in envs:
            result_env = env.get("value", "")
            if result_env == "Prod":
                break
    return result_env


def fetch_sprint_issues(
    jira_client: Jira,
    sprint_ids: List[str],
    jql_template: str,
    date_params: Optional[Tuple[str, str]] = None,
) -> List[SprintIssue]:
    """
    Fetches issues from sprints using the provided JQL template and date parameters.

    :param jira_client: Initialized Jira client.
    :param sprint_ids: List of sprint IDs to query.
    :param jql_template: JQL query template.
    :param date_params: Optional tuple of (start_date, end_date) for date filtering.
    :return: List of SprintIssue objects.
    """
    all_sprint_issues: List[SprintIssue] = []

    for sprint_id in sprint_ids:
        if date_params:
            # Unpack date parameters if provided
            start_date, end_date = date_params
            updated_jql = jql_template.format(sprint_id, start_date, end_date)
        else:
            updated_jql = jql_template.format(sprint_id)
        try:
            response = jira_client.enhanced_jql(updated_jql, limit=1000)
            issues = response.get("issues", [])
        except Exception as e:
            logging.error(f"Error executing JQL for sprint '{sprint_id}': {e}")
            continue
        total_story_points = 0
        for issue in issues:
            fields = issue.get("fields", {})
            issue_type = fields.get("issuetype", {}).get("name", "")
            story_points = fields.get(Config.STORY_POINT_FIELD, 0) or 0

            # Extract bug type if available, default to UAT
            if issue_type == "Bug":
                # Retrieve bug environment
                envs = issue["fields"].get(Config.BUG_TYPE_FIELD)
                issue_type = "UAT Bug"
                env_name = get_env(envs)
                if env_name == "Prod":
                    issue_type = "Prod Bug"

            logging.info(
                f"{issue['key']}: {fields.get('summary', 'No Summary')} - Story Points: {story_points}"
            )

            new_data = SprintIssue(
                issue_key=issue["key"],
                summary=fields.get("summary", ""),
                story_points=story_points,
                sprint_id=sprint_id,
                issue_type=issue_type,
            )

            all_sprint_issues.append(new_data)
            total_story_points += story_points

        logging.info(f"{len(issues)} and total story points: {total_story_points}")

    return all_sprint_issues


def fetch_bug_issues(
    jira_client: Jira,
    project_ids: List[str],
    jql_template: str,
) -> List[Bug]:
    """
    Fetches Bugs based on params passed

    :param jira_client: Initialized Jira client.
    :param project_ids: List of project keys to query.
    :param jql_template: JQL query template.
    :return: List of Bug objects.
    """
    bug_fields = (
        "created, summary,status, parent,issuetype,priority,{},{},{},{}".format(
            Config.TEAM_FIELD,
            Config.BUG_TYPE_FIELD,
            Config.STORY_POINT_FIELD,
            Config.SPRINT_FIELD,
        )
    )

    all_bugs: List[Bug] = []
    project_ids_str = ",".join(project_ids)

    one_year_back_date = (
        datetime.now() - timedelta(days=366)
    ).strftime("%Y-%m-%d")

    updated_jql = jql_template.format(project_ids_str, one_year_back_date)
    logging.info(f"JQL for fetching Bugs {updated_jql}")

    is_last = False
    next_page_token = None

    while not is_last:
        try:
            response = jira_client.enhanced_jql(updated_jql, bug_fields, next_page_token, Config.MAX_RESULTS_BUGS)
            is_last = response.get('isLast', None)
            if is_last is False:
                next_page_token = response.get('nextPageToken', None)
            jira_bugs = response.get("issues", [])

            for jira_bug in jira_bugs:
                bug_key = jira_bug["key"]
                fields = jira_bug.get("fields", {})
                parent = fields.get("parent", {}).get("name", "")
                story_points = fields.get(Config.STORY_POINT_FIELD, 0)
                team_name = (
                    fields.get(Config.TEAM_FIELD).get("name", "")
                    if fields.get(Config.TEAM_FIELD) is not None
                    else ""
                )
                summary = fields.get("summary", "")
                state = fields.get("status", {}).get("name", "")
                priority = fields.get("priority", {}).get("name", "")
                sprint_name = (
                    fields.get(Config.SPRINT_FIELD, [])[0].get("name", "")
                    if fields.get(Config.SPRINT_FIELD) is not None
                    else ""
                )
                env_name = get_env(fields.get(Config.BUG_TYPE_FIELD))
                created_date = isoparse(fields.get("created"))
                bug = Bug(
                    bug_key,
                    summary,
                    state,
                    parent,
                    story_points,
                    sprint_name,
                    env_name,
                    team_name,
                    priority,
                    created_date,
                )
                all_bugs.append(bug)
        except Exception as e:
            logging.error(f"Error executing JQL{updated_jql} for fetching Bugs ': {e}")

    return all_bugs
