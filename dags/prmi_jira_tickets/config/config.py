"""
This module defines the PRMIJiraTicketsConfig class.
"""

from typing import Final
import os
from datetime import datetime, timedelta

from prmi_jira_tickets.config.terminus_table_config import LandingTables
from util.constants import (
    GCP_CONFIG,
    DEPLOYMENT_ENVIRONMENT_NAME,
    LANDING_ZONE_PROJECT_ID,
    PROD_ENV
)
from util.miscutils import read_variable_or_file


class PRMIJiraTicketsConfig:
    """
    This class serves as the configurations for the PRMI Jira Tickets DAG.
    """

    # The deployment environment in GCP (dev, uat, or prod).
    _gcp_config = read_variable_or_file(GCP_CONFIG)
    DEPLOY_ENV: Final[str] = (
        _gcp_config.get(DEPLOYMENT_ENVIRONMENT_NAME)
        or os.environ.get(DEPLOYMENT_ENVIRONMENT_NAME)
        or "dev"
    )

    # The URL to the JIRA Cloud API.
    JIRA_CLOUD_URL: Final[str] = "https://pc-technology.atlassian.net"

    # The username for JIRA Cloud API.
    USER_NAME: Final[str] = "svc_DevOpsTools@pcbank.ca"

    # The path to where the password was stored in Vault.
    PASSWORD_PATH: Final[str] = f"/{'' if DEPLOY_ENV == PROD_ENV else f'{DEPLOY_ENV}-'}secret/data/jira-analytics"

    # The key for JIRA Cloud API password in the vault.
    PASSWORD_KEY: Final[str] = "api_password"

    _landing_zone_project_id = (
        _gcp_config.get(LANDING_ZONE_PROJECT_ID)
        or os.environ.get(LANDING_ZONE_PROJECT_ID)
    )
    if not _landing_zone_project_id:
        raise ValueError(
            "Missing landing zone project id. Set env var 'landing_zone_project_id' "
            "or provide it in gcp_config."
        )

    # The landing zone project ID and dataset name for PRMI Jira Tickets.
    LANDING_ZONE: Final[str] = f"{_landing_zone_project_id}.domain_project_artifacts"

    # If we want to fetch the updates only or get the whole historic data.
    UPDATES_ONLY: Final[bool] = True

    # The size of chunk when extracting data from Jira Cloud.
    CHUNK_SIZE: Final[int] = 1000

    # The update frequency (every 10 minutes).
    UPDATE_CRON_SCHEDULE: Final[str] = '*/10 * * * *'

    # A dataclass for table names of Landing Zone.
    LANDING_TABLES = LandingTables(LANDING_ZONE)

    # Default date range for PRMI tickets (last 30 days)
    DEFAULT_DAYS_BACK: Final[int] = 30
