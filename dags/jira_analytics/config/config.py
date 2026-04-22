"""
This module defines the JiraAnalyticsConfig class.
"""

from typing import Tuple, Final

from airflow.settings import DAGS_FOLDER

from jira_analytics.config.terminus_table_config import CuratedTables, LandingTables
from util.constants import (
    GCP_CONFIG,
    DEPLOYMENT_ENVIRONMENT_NAME,
    LANDING_ZONE_PROJECT_ID,
    CURATED_ZONE_PROJECT_ID,
    PROD_ENV
)
from util.miscutils import read_variable_or_file, read_yamlfile_env


class JiraAnalyticsConfig:
    """
    This class serves as the configurations for the Jira Analytics DAG that are not a part of the
    business logic.
    """

    # The deployment environment in GCP (dev, uat, or prod).
    DEPLOY_ENV: Final[str] = read_variable_or_file(GCP_CONFIG)[
        DEPLOYMENT_ENVIRONMENT_NAME
    ]

    # The URL to the JIRA Cloud API.
    JIRA_CLOUD_URL: Final[str] = "https://pc-technology.atlassian.net"

    # The username for JIRA Cloud API.
    USER_NAME: Final[str] = "svc_DevOpsTools@pcbank.ca"

    # The path to where the password was stored in Vault.
    PASSWORD_PATH: Final[str] = f"/{'' if DEPLOY_ENV == PROD_ENV else f'{DEPLOY_ENV}-'}secret/data/jira-analytics"

    # The key for JIRA Cloud API password in the vault.
    PASSWORD_KEY: Final[str] = "api_password"

    # The landing zone project ID and dataset name for JIRA analytics.
    LANDING_ZONE: Final[str] = (
        f"{read_variable_or_file(GCP_CONFIG)[LANDING_ZONE_PROJECT_ID]}.domain_project_artifacts"
    )

    # The curated zone project ID and dataset name for JIRA analytics.
    CURATED_ZONE: Final[str] = (
        f"{read_variable_or_file(GCP_CONFIG)[CURATED_ZONE_PROJECT_ID]}.domain_project_artifacts"
    )

    # The projects that issues from which will be included on the JIRA analytics dashboard.
    PROJECTS: Final[Tuple[str]] = (
        "PCCS",
        "PCCOM",
        "PCBIV",
        "DATA",
        "PCBRM",
        "PCBDA",
        "PCMM",
        "PCOC",
        "PCGS",
        "PCBDF",
        "PCBTD",
        "PCCORE",
        "PLATFORMS",
        "PCBAI",
        "PRPI",
        "PCBJAT",
        "GOCP",
        "PCIPS",
        "PMP",
        "TMSP",
        "PLATFORMS",
        "PRDS",
        "PCBSS",
        "PCBCORE",
        "PCBEX"
    )

    # The field value for sprint points in Jira api.
    STORY_POINT_FIELD = "customfield_10117"

    # The field value for bug type  in Jira api.
    BUG_TYPE_FIELD = "customfield_11401"

    # The Jira date format
    JIRA_SPRINT_DATE_FORMAT = "%Y/%m/%d %H:%M"

    # The JQL to fetch all closed sprint specific issues formatted with start and end dates.
    DONE_ISSUES_JQL = (
        "issuetype in (Story, Bug, Spike, Task) AND sprint in ({}) AND STATUS in (Closed, Done)"
        "AND STATUS CHANGED AFTER '{}' BEFORE '{}' to Done ORDER BY key DESC"
    )

    # The JQL to fetch all active sprint specific issues
    ACTIVE_ISSUES_JQL = (
        "issuetype in (Story, Bug, Spike, Task) AND sprint in ({})"
    )

    # The JQL to fetch all Bugs for all projects for last based on created date
    BUG_ISSUES_JQL = (
        "issuetype in (Bug) AND resolution not in (\"Won't Do\",Duplicate) AND project in ({}) AND createdDate >= {}"
    )

    # The field value for team/pod name  in Jira api.
    TEAM_FIELD = "customfield_10114"

    # The field value for Sprint name  in Jira api.
    SPRINT_FIELD = "customfield_10115"

    # The maximum results(100) that can be fetched from the JIRA API in one fetch.
    MAX_RESULTS_BUGS: Final[int] = 100

    # If we want to fetch the updates only or get the whole historic data.
    UPDATES_ONLY: Final[bool] = True

    # The maximum results that can be fetched from the JIRA API in one fetch.
    MAX_RESULTS: Final[int] = 50

    # The update frequency.
    UPDATE_CRON_SCHEDULE: Final[str] = '0 */2 * * *'

    # The size of chunk when extracting data from Jira Cloud.
    CHUNK_SIZE: Final[int] = 1000

    # The parameters used to extract data from Jira changelog.
    CHANGELOG_FIELDS = read_yamlfile_env(
        f"{DAGS_FOLDER}/jira_analytics/config/changelog_fields.yaml"
    )

    # A dataclass for table names of Landing Zone.
    LANDING_TABLES = LandingTables(LANDING_ZONE)

    # A dataclass for table names of Curated Zone.
    CURATED_TABLES = CuratedTables(CURATED_ZONE)

    # A constant for the outcome team csv file path
    OUTCOME_TEAMS_CSV_PATH: Final = f'{DAGS_FOLDER}/jira_analytics/resources/outcome_teams.csv'

    # A constant for defining months to load closed sprints
    MONTHS_OF_CLOSED_SPRINTS_TO_LOAD: Final[int] = 12
