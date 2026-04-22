"""
The class defines methods related to extraction of data from BigQuery's landing
zone and JIRA Cloud.
"""

import logging
from dataclasses import fields
from datetime import datetime, timedelta
from time import time, sleep
from requests.exceptions import ConnectionError
from typing import Dict, Union, Optional, List, Type, Tuple

from atlassian import Jira
from dacite import from_dict
from dateutil.parser import isoparse
from google.cloud.bigquery import Client
from google.cloud.exceptions import NotFound
from pandas import isna, to_datetime
from util.bq_utils import run_bq_query_with_params
from jira_analytics.utils.utils import convert_utc_to_est, fetch_sprint_issues, fetch_bug_issues

from jira_analytics.config import JiraAnalyticsConfig as Config
from jira_analytics.model.jira.response import Response, ChangelogResponse
from jira_analytics.model.terminus.changelog_count import ChangelogCount
from jira_analytics.model.terminus.changelog_event import ChangelogEvent
from jira_analytics.model.terminus.sprint import Sprint
from jira_analytics.model.terminus.sprint_issues import SprintIssue
from jira_analytics.model.terminus.issue import Issue, Initiative, Epic, Story
from jira_analytics.model.terminus.terminus import transform_issue_to_terminus, transform_changelog_to_terminus
from jira_analytics.model.terminus.bug import Bug


def extract_changelog_from_jira_cloud(jira_client: Jira,
                                      issue_key: str,
                                      terminus_class: Type[Issue],
                                      start: int = 0,
                                      end: int = 0) -> List[ChangelogEvent]:
    """
    This function calls a GET Method to JIRA Cloud /rest/api/3/issue/{issueIdOrKey}/changelog
    endpoint to fetch a list of changelogs for a specific issue_key.

    :param jira_client: Jira Client object.
    :param issue_key: A target JIRA issue key to extract changelogs from.
    :param terminus_class: The class of the Terminus objects to extract changelogs from.
    :param start: The starting index of the changelog to extract changelogs from.
    :param end: The ending index of the changelog to extract changelogs from.
    """

    # Fetch the changelog from JIRA cloud from the starting to ending index.
    new_event_count: int = end - start
    max_results_list: List[int] = [100] * int(new_event_count / 100) + [new_event_count % 100]

    responses: List[ChangelogResponse] = []
    for max_results in max_results_list:
        jira_data = jira_client.get(
            path=f"/rest/api/3/issue/{issue_key}/changelog?maxResults={max_results}&startAt={start}"
        )
        changelog = from_dict(ChangelogResponse, jira_data)
        event_count: int = len(changelog.values)

        responses.append(changelog)
        start += event_count

    return transform_changelog_to_terminus(responses=responses,
                                           terminus_class=terminus_class,
                                           issue_key=issue_key)


def extract_from_jira_cloud(jira_config: Dict[str, str],
                            response_class: Type[Response],
                            terminus_class: Type[Issue],
                            next_page_token: str,
                            limit: Union[float, int] = float("Inf"),
                            update_window_start: Optional[str] = None,
                            update_window_end: Optional[str] = None) \
        -> Tuple[List[Issue], List[ChangelogCount], List[ChangelogEvent], int, str]:
    """
    This function executes the jql_query, extracts the required field from the result,
    and returns it as a Pandas data frame.

    :param jira_config: The configuration for Jira Cloud client in Python dictionary format.
    :param response_class: The class of type Response to be extracted.
    :param terminus_class: The class of the Terminus objects to be returned.
    :param next_page_token: The starting page token of issue in Jira Database to query from. Will be None for the first call.
    :param limit: The maximum number of records to return.
    :param update_window_start: The start of the time window where issues updated in shall be extracted.
    :param update_window_end: The end of the time window where issues updated in shall be extracted.
    :return: The parsed tables from Jira Cloud.
    """
    # Create a Jira client for fetching issues.
    jira_client = Jira(**jira_config)

    # The JQLs for different issue types.
    jql: Dict[str, str] = {
        Initiative.__name__: "project = PCBINIT AND issuetype = Initiative",
        Epic.__name__: f"project IN {Config.PROJECTS} AND issuetype = Epic",
        Story.__name__: f"project IN {Config.PROJECTS} AND "
                        f"issuetype IN (Story, 'Technical Bug', Bug, Spike, Task)"
    }
    assert terminus_class.__name__ in jql and update_window_end is not None

    # Create the JQL query.
    jql_query = jql[terminus_class.__name__]
    if Config.UPDATES_ONLY and update_window_start:
        jql_query = f"{jql_query} AND updated >= \"{update_window_start[:-5]}\" AND updated < \"{update_window_end[:-5]}\""
    else:
        jql_query = f"{jql_query} AND created < \"{update_window_end[:-5]}\""

    # Fetch the data from JIRA cloud until the number of issues read the limit.
    total = 0
    responses: List[Response] = []
    batch_size = int(limit) if limit < float("inf") else 1000

    issue_fields = [
        f.name for f in fields(response_class.__annotations__['issues'].__args__[0].__annotations__['fields'])
    ]
    logging.info(f"Extracting issue fields {str(issue_fields)}.")
    logging.info(f"Executing JQL: \n{jql_query}")
    is_last_page = False
    page_size = 100
    jira_data = None
    logging.info(f"batch_size is {batch_size} ")
    while True:
        start_time = time()
        try:
            jira_data = jira_client.enhanced_jql(jql=jql_query,
                                                 fields=issue_fields,
                                                 nextPageToken=next_page_token,
                                                 limit=page_size,
                                                 expand="changelog")

        except ConnectionError as e:
            logging.info(f"Connection Error - {e}, Failed Time - {start_time}, Duration - {time() - start_time:.3f}s, Batch Size - {batch_size}")
            sleep(1)
            continue

        logging.info(f"Extracted {len(jira_data['issues'])} issues from Jira Cloud in {time() - start_time:.3f}s.")
        start_time = time()
        responses.append(from_dict(response_class, jira_data))
        logging.info(f"Formatted {len(responses[-1].issues)} issue records extracted in {time() - start_time:.3f}s.")

        total += len(responses[-1].issues)
        page_size = min(page_size, limit - total)

        is_last_page = jira_data.get("isLast", None)
        next_page_token = None
        if not is_last_page:
            next_page_token = jira_data.get("nextPageToken", None)

        if total >= limit or is_last_page is True:
            logging.info(f"Total is : {total}, Limit is : {limit}, is_last_page is {is_last_page}")
            break

    if len(responses) == 0 or len(responses[-1].issues) == 0:
        return [], [], [], total, next_page_token
    else:
        start_time = time()

        all_issues, all_changelog_counts = transform_issue_to_terminus(responses, terminus_class)
        logging.info(f"Transformed {len(all_issues)} issue properties to dataframe "
                     f"in {time() - start_time:.3f}s.")
        logging.info(f"Transformed {len(all_changelog_counts)} changelog counts to dataframe "
                     f"in {time() - start_time: .3f}s.")

    # Retrieve changelog count for each issue from BigQuery Landing Zone
    table_id = Config.LANDING_TABLES.CHANGELOG_COUNT
    bq_client = Client()
    changelog_count_df = bq_client.query_and_wait(f"""
    SELECT issue_key,
           count
    FROM (SELECT issue_key,
                 count,
                 ROW_NUMBER() OVER (PARTITION BY issue_key ORDER BY query_time DESC) row_num
          FROM {table_id})
    WHERE row_num = 1;
    """).to_dataframe()

    start_time = time()
    updated_changelog_counts: List[ChangelogCount] = []
    all_changelog_events: List[ChangelogEvent] = []
    # Only fetch changelog for issues that have new changelog events
    for changelog_count in all_changelog_counts:
        # Search if issue_key's previous changelog count is already in the CHANGELOG_COUNT table
        record_df = changelog_count_df[changelog_count_df["issue_key"] == changelog_count.issue_key]

        if len(record_df) == 0:  # Not found from the table
            if changelog_count.count != 0:  # Has at least one changelog
                all_changelog_events += extract_changelog_from_jira_cloud(
                    jira_client=jira_client,
                    issue_key=changelog_count.issue_key,
                    terminus_class=terminus_class,
                    start=0,
                    end=changelog_count.count
                )
                updated_changelog_counts.append(changelog_count)
        else:
            prev_count = record_df["count"].values[0]
            if prev_count < changelog_count.count:  # New changelog events
                all_changelog_events += extract_changelog_from_jira_cloud(
                    jira_client=jira_client,
                    issue_key=changelog_count.issue_key,
                    terminus_class=terminus_class,
                    start=prev_count,
                    end=changelog_count.count
                )
                updated_changelog_counts.append(changelog_count)

    logging.info(f"Transformed {len(all_changelog_events)} changelog events to dataframe "
                 f"in {time() - start_time:.3f}s.")

    return all_issues, updated_changelog_counts, all_changelog_events, total, next_page_token


def extract_last_successful_dag_run_time() -> Optional[datetime]:
    """
    This function retrieves the time of the last successful DAG run's time.

    :returns: The time stamp of the last successful DAG run. If there is not a successful
        DAG run before, this function will return None.
    """
    try:
        table_id = Config.LANDING_TABLES.DAG_HISTORY
        client = Client()
        run_history = client.query(f"SELECT MAX(dag_run_time) AS dag_run_time FROM {table_id}") \
            .result() \
            .to_dataframe()
        run_history = run_history \
            .assign(dag_run_time=lambda x: to_datetime(x.dag_run_time, utc=True).astype("datetime64[ns, US/Eastern]"))
        last_run_time = run_history["dag_run_time"].max()
        return None if isna(last_run_time) else last_run_time
    except NotFound:
        return None


def extract_sprint(jira_config: Dict[str, str]) -> List[Sprint]:
    """
    This function extracts all sprints from the JIRA API using the list of projects in BigQuery.

    :param jira_config: The configuration for Jira Cloud client in Python dictionary format.
    :returns: A list of Sprint objects.
    """

    jira_client = Jira(**jira_config)
    query = f"""
        SELECT board_id, team_name
        FROM `{Config.LANDING_TABLES.OUTCOME_TEAM}`
        WHERE board_id IS NOT NULL
    """

    # Run query and convert to DataFrame
    board_ids_df = run_bq_query_with_params(query).result().to_dataframe()
    all_sprints: List[Sprint] = []

    for index, row in board_ids_df.iterrows():
        board = row['board_id']
        team_name = row['team_name']
        start_at = 0
        total = 1

        while start_at < total:
            params = {
                'startAt': start_at,
                'maxResults': Config.MAX_RESULTS
            }

            logging.info(f"Fetching sprints for board {board} with startAt={start_at}")

            try:
                jira_data = jira_client.get(
                    path=f"/rest/agile/1.0/board/{board}/sprint",
                    params=params
                )
            except Exception as e:
                logging.error(f"Failed to fetch sprints for board {board}: {e}")
                break

            total = jira_data.get('total', 0)
            values = jira_data.get('values', [])

            # Filter to exclude future sprints and those without start/end dates
            filtered_data = [
                sprint for sprint in values
                if sprint.get('state') != "future" and 'startDate' in sprint and 'endDate' in sprint
                and team_name in sprint.get('name')
            ]

            # Manually construct the Sprint objects
            for sprint_data in filtered_data:
                # Parse start and end dates
                start_date = isoparse(sprint_data['startDate'])
                end_date = isoparse(sprint_data['endDate'])

                # Set date to completion date if closed sprint
                if sprint_data['state'] == 'closed':
                    end_date = isoparse(sprint_data['completeDate'])

                sprint = Sprint(
                    id=sprint_data['id'],
                    name=sprint_data['name'],
                    state=sprint_data['state'],
                    start_date=start_date,
                    end_date=end_date,
                    board_id=board
                )
                all_sprints.append(sprint)

            # Increment start_at for the next batch
            start_at += Config.MAX_RESULTS

    logging.info(f"Total sprints extracted: {len(all_sprints)}")
    return all_sprints


def extract_active_sprint_issues(jira_config: Dict[str, str]) -> List[SprintIssue]:
    """
    Extracts issues from active sprints.

    :param jira_config: Configuration dictionary for Jira client.
    :return: List of SprintIssue objects from active sprints.
    """
    # Initialize Jira client
    jira_client = Jira(**jira_config, timeout=60)

    # Fetch active sprint IDs
    query = f"""
        SELECT DISTINCT id
        FROM `{Config.LANDING_TABLES.SPRINT}`
        WHERE state = 'active'
    """

    sprint_df = run_bq_query_with_params(query).result().to_dataframe()
    sprint_ids = sprint_df['id'].tolist()

    # Use the helper function without date parameters
    return fetch_sprint_issues(
        jira_client=jira_client,
        sprint_ids=sprint_ids,
        jql_template=Config.ACTIVE_ISSUES_JQL
    )


def extract_closed_sprints_issues(jira_config: Dict[str, str]) -> List[SprintIssue]:
    """
    Extracts issues from closed sprints.

    :param jira_config: Configuration dictionary for Jira client.
    :return: List of SprintIssue objects from closed sprints.
    """
    # Initialize Jira client
    jira_client = Jira(**jira_config, timeout=60)

    # JQL to return all sprints with a 'closed' status that have ended in the
    # timeframe specified by the configuration.
    all_closed_sprints_query = f"""
        SELECT DISTINCT id, start_date, end_date
        FROM `{Config.LANDING_TABLES.SPRINT}`
        WHERE state = 'closed' and
        end_date > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL {Config.MONTHS_OF_CLOSED_SPRINTS_TO_LOAD} MONTH))
    """
    all_sprint_df = run_bq_query_with_params(all_closed_sprints_query).result().to_dataframe()

    all_sprint_issues: List[SprintIssue] = []

    for _, row in all_sprint_df.iterrows():
        sprint_id = row['id']
        start_utc = row['start_date']
        end_utc = row['end_date']

        # Convert and adjust dates
        try:
            formatted_start_date = convert_utc_to_est(start_utc) - timedelta(minutes=1)
            formatted_end_date = convert_utc_to_est(end_utc) + timedelta(minutes=1)
            final_start_str = formatted_start_date.strftime(Config.JIRA_SPRINT_DATE_FORMAT)
            final_end_str = formatted_end_date.strftime(Config.JIRA_SPRINT_DATE_FORMAT)
        except Exception as e:
            logging.error(f"Failed to process dates for sprint '{sprint_id}': {e}")
            continue

        # Use the helper function with date parameters
        sprint_issues = fetch_sprint_issues(
            jira_client=jira_client,
            sprint_ids=[sprint_id],
            jql_template=Config.DONE_ISSUES_JQL,
            date_params=(final_start_str, final_end_str),
        )
        all_sprint_issues.extend(sprint_issues)
        logging.info(f"Processed sprint '{sprint_id}' with {len(sprint_issues)} issues.")

    return all_sprint_issues


# get All bugs from Jira
def extract_bugs(jira_config: Dict[str, str]) -> List[Bug]:
    """
    This function extracts all Bugs  from the JIRA API using the list of projects in BigQuery.

    :param jira_config: The configuration for Jira Cloud client in Python dictionary format.
    :returns: A list of Bugs objects.
    """
    jira_client = Jira(**jira_config)
    query = f"""
        SELECT DISTINCT project
        FROM `{Config.LANDING_TABLES.OUTCOME_TEAM}`
        WHERE project IS NOT NULL
    """

    # Run query and convert to DataFrame
    project_ids_df = run_bq_query_with_params(query).result().to_dataframe()
    project_ids = project_ids_df['project'].tolist()
    all_bugs: List[Bug] = []

    # Use the helper function without date parameters
    all_bugs = fetch_bug_issues(
        jira_client=jira_client,
        project_ids=project_ids,
        jql_template=Config.BUG_ISSUES_JQL
    )
    return all_bugs
