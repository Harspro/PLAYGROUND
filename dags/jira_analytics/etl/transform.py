"""
This module handles the transformation of data to the format needed by Looker
"""

from dataclasses import dataclass
from typing import Union, Tuple

from google.cloud import bigquery

from jira_analytics.config import JiraAnalyticsConfig as Config
from jira_analytics.utils import execute_sql_file


def curate_changelogs() -> None:
    """
    This function curates combined changelogs from CHANGELOG_EVENT to separated changelog tables.
    """

    @dataclass
    class ChangeLogParams:
        # destination table name
        destination_table_name: str
        # target issue_type (either str or tuple of strings)
        target_issue_types: Union[str, Tuple[str, ...]]
        # part 1 of the key of the changelog_event field to use in the calculation
        clog_event_field: str
        # part 2 of the key of the changelog_event field to use in the calculation
        clog_event_field_type: str
        # part 3 of the key of the changelog_event field to use in the calculation
        clog_event_field_id: str
        # column name of the changelog_event field (to_column_name, from_column_name)
        column_name: str
        # BigQuery data type (bigquery.enums.SqlTypeNames) of the column
        bq_data_type: str

    # Status
    params = ChangeLogParams(destination_table_name=Config.CURATED_TABLES.STATUS_CHANGELOG,
                             target_issue_types=("initiative", "epic", "story"),
                             clog_event_field="status",
                             clog_event_field_type="jira",
                             clog_event_field_id="status",
                             column_name="status",
                             bq_data_type=bigquery.enums.SqlTypeNames.STRING.name)

    execute_sql_file("transformations/curate_changelogs.sql", param_object=params)

    # T-Shirt Size
    params = ChangeLogParams(destination_table_name=Config.CURATED_TABLES.T_SHIRT_SIZE_CHANGELOG,
                             target_issue_types="epic",
                             clog_event_field="T-Shirt Size",
                             clog_event_field_type="custom",
                             clog_event_field_id="customfield_13165",
                             column_name="t_shirt_size",
                             bq_data_type=bigquery.enums.StandardSqlTypeNames.STRING.name)

    execute_sql_file("transformations/curate_changelogs.sql", param_object=params)

    # Story points
    params = ChangeLogParams(destination_table_name=Config.CURATED_TABLES.STORY_POINT_CHANGELOG,
                             target_issue_types="story",
                             clog_event_field="Story Points",
                             clog_event_field_type="custom",
                             clog_event_field_id="customfield_10117",
                             column_name="story_point",
                             bq_data_type=bigquery.enums.StandardSqlTypeNames.FLOAT64.name)

    execute_sql_file("transformations/curate_changelogs.sql", param_object=params)
