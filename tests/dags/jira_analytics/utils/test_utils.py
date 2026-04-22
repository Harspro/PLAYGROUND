from dataclasses import dataclass
from datetime import datetime
from typing import Final, List, Tuple, Union
from unittest import TestCase
from unittest.mock import patch, Mock, mock_open

import pendulum
import pytest
from airflow.exceptions import AirflowFailException
from airflow.settings import DAGS_FOLDER
from google.cloud.bigquery.enums import SqlTypeNames

from jira_analytics.utils.utils import as_naive_utc, convert_utc_to_est, execute_sql_file, extract_csv_to_bq, get_env, \
    handle_epic_key_change, fetch_bug_issues


class TestDatetimeUtils:
    AS_NAIVE_UTC_TIMEZONE_TEST_CASES: Final[List[Tuple[datetime, datetime]]] = [
        # EDT → UTC
        (datetime(2025, 4, 3, 15, 30, tzinfo=pendulum.timezone("America/Toronto")),
         datetime(2025, 4, 3, 19, 30)),

        # Already UTC
        (datetime(2025, 4, 3, 15, 30, tzinfo=pendulum.timezone("UTC")),
         datetime(2025, 4, 3, 15, 30)),

        # Naive datetime - GitLab runners and Airflow use UTC by default so this should not change
        (datetime(2025, 4, 3, 15, 30),
         datetime(2025, 4, 3, 15, 30))
    ]

    CONVERT_UTC_TO_EST: Final[List[Tuple[datetime, datetime]]] = [
        # Summer Naive datetime -> EDT (UTC-4)
        (datetime(2025, 6, 1, 15, 30),
         datetime(2025, 6, 1, 11, 30, tzinfo=pendulum.timezone("America/Toronto"))),

        # Winter Naive datetime -> EST (UTC-5)
        (datetime(2025, 1, 1, 15, 30),
         datetime(2025, 1, 1, 10, 30, tzinfo=pendulum.timezone("America/Toronto"))),

        # Already UTC -> EDT
        (datetime(2025, 6, 1, 15, 30, tzinfo=pendulum.timezone("UTC")),
         datetime(2025, 6, 1, 11, 30, tzinfo=pendulum.timezone("America/Toronto"))),

        # Already UTC -> EST
        (datetime(2025, 1, 1, 15, 30, tzinfo=pendulum.timezone("UTC")),
         datetime(2025, 1, 1, 10, 30, tzinfo=pendulum.timezone("America/Toronto"))),

        # Another timezone -> EDT
        (datetime(2025, 6, 1, 15, 30, tzinfo=pendulum.timezone("America/Los_Angeles")),
         datetime(2025, 6, 1, 18, 30, tzinfo=pendulum.timezone("America/Toronto")))
    ]

    @pytest.mark.parametrize("input_dt, expected_dt", AS_NAIVE_UTC_TIMEZONE_TEST_CASES)
    def test_as_naive_utc(
            self,
            input_dt,
            expected_dt
    ):
        assert as_naive_utc(input_dt) == expected_dt

    @pytest.mark.parametrize("input_dt, expected_dt", CONVERT_UTC_TO_EST)
    def test_convert_utc_to_est(
            self,
            input_dt,
            expected_dt
    ):
        assert convert_utc_to_est(input_dt) == expected_dt

    def test_convert_utc_to_est_invalid(
            self
    ):
        assert convert_utc_to_est("2025-01-01") is None

    # Currently this behaves as above. We should modify convert_utc_to_est to handle None type specifically.
    def test_convert_utc_to_est_none(
            self
    ):
        assert convert_utc_to_est(None) is None


class TestGeneralUtils:
    # Define local dataclass again here to mimic same flow.
    @dataclass
    class ChangeLogParams:
        destination_table_name: str
        target_issue_types: Union[str, Tuple[str, ...]]
        clog_event_field: str
        clog_event_field_type: str
        clog_event_field_id: str
        column_name: str
        bq_data_type: str

    TEST_INPUT_SQL_STRING: Final[str] = "SELECT * FROM {{ landing.source_table }} JOIN {{ params.destination_table_name }}"
    TEST_RENDERED_SQL_STRING: Final[str] = "SELECT * FROM pcb-dev-landing.domain_project_artifacts.SOURCE_TABLE JOIN pcb-dev-curated.domain_project_artifacts.DESTINATION_TABLE"
    TEST_INPUT_SQL_PATH: Final[str] = f"{DAGS_FOLDER}/jira_analytics/etl/sql_queries/mock_file.sql"

    PARAMS: Final[ChangeLogParams] = ChangeLogParams(
        destination_table_name="pcb-dev-curated.domain_project_artifacts.DESTINATION_TABLE",
        target_issue_types=("story",),
        clog_event_field="status",
        clog_event_field_type="jira",
        clog_event_field_id="status",
        column_name="status",
        bq_data_type=SqlTypeNames.STRING.name
    )

    @patch("jira_analytics.utils.utils.run_bq_query")
    @patch("jira_analytics.utils.utils.logging")
    @patch("jira_analytics.utils.utils.Template")
    @patch("builtins.open", new_callable=mock_open, read_data=TEST_INPUT_SQL_STRING)
    @patch("jira_analytics.utils.utils.Config")
    def test_execute_sql_file(
            self,
            mock_config,
            mock_open_file,
            mock_template,
            mock_logging,
            mock_run_bq_query
    ):

        # Setup Config mocks
        mock_config.CURATED_TABLES = Mock()
        mock_config.LANDING_TABLES = Mock()

        # Setup template rendering
        mock_render = Mock(return_value=self.TEST_RENDERED_SQL_STRING)
        mock_template.return_value.render = mock_render

        # Call the function
        execute_sql_file("mock_file.sql", param_object=self.PARAMS, verbose=True)

        # Check file was read
        mock_open_file.assert_called_once_with(self.TEST_INPUT_SQL_PATH)

        # Check Jinja render was called with correct params
        mock_render.assert_called_once_with(
            curated=mock_config.CURATED_TABLES,
            landing=mock_config.LANDING_TABLES,
            params=self.PARAMS
        )

        # Check logging was called
        mock_logging.info.assert_called_once_with(f"Running query:\n{self.TEST_RENDERED_SQL_STRING}")

        # Check run_bq_query was called with rendered SQL
        mock_run_bq_query.assert_called_once_with(self.TEST_RENDERED_SQL_STRING)


class TestExtractToCSV:
    TEST_INPUT_CSV: Final[str] = """
        project,project_name,team_name,board_id,\n
        PCBDA,PCB: Digital Adoption,Bees,548,\n
        PCBDA,PCB: Digital Adoption,Spiders,555
    """

    @patch("builtins.open", new_callable=mock_open, read_data=TEST_INPUT_CSV)
    @patch("google.cloud.bigquery.Client")
    def test_successful_csv_load(
            self,
            mock_bq_client,
            mock_open_file
    ):

        mock_load_job = Mock()
        mock_load_job.result.return_value = None  # Simulate successful job completion
        mock_load_job.output_rows = 2
        mock_bq_client.return_value.load_table_from_file.return_value = mock_load_job

        extract_csv_to_bq("mock_path.csv", "mock_table")

        mock_open_file.assert_called_once_with("mock_path.csv", "rb")
        mock_bq_client.return_value.load_table_from_file.assert_called_once()
        assert mock_load_job.output_rows == 2

    @patch("google.cloud.bigquery.Client")
    def test_file_not_found_error(
            self,
            mock_bq_client
    ):
        with pytest.raises(AirflowFailException):
            extract_csv_to_bq("non_existent.csv", "mock_table")

        mock_bq_client.return_value.load_table_from_file.assert_not_called()

    @patch("builtins.open", side_effect=Exception("Unexpected error"))
    @patch("google.cloud.bigquery.Client")
    def test_unexpected_error(
            self,
            mock_bq_client,
            mock_open_file
    ):
        with pytest.raises(AirflowFailException):
            extract_csv_to_bq("unexpected.csv", "mock_table")

        mock_open_file.assert_called_once_with("unexpected.csv", "rb")
        mock_bq_client.return_value.load_table_from_file.assert_not_called()


class TestGetEnv:
    VALID_ENV_LIST: Final[dict[str, str]] = [
        {
            "self": "https://pc-technology.atlassian.net/rest/api/2/customFieldOption/12898",
            "value": "UAT",
            "id": "12898"
        },
        {
            "self": "https://pc-technology.atlassian.net/rest/api/2/customFieldOption/12898",
            "value": "Prod",
            "id": "12898"
        },
        {
            "self": "https://pc-technology.atlassian.net/rest/api/2/customFieldOption/12898",
            "value": "UAT",
            "id": "12898"
        }
    ]

    def test_get_env_none(
            self
    ):
        assert get_env(None) == ""

    def test_get_env_empty_list(
            self
    ):
        assert get_env([]) == ""

    def test_get_env_valid_list(
            self
    ):
        assert get_env(self.VALID_ENV_LIST) == "Prod"


class TestHandleEpicKeyChange(TestCase):
    LANDING_DATASET_PATH: Final[str] = "pcb-dev-landing.domain_project_artifacts"
    LANDING_TABLE_NAMES_AND_KEY: Final[List[Tuple[str, str]]] = [
        ("CHANGELOG_COUNT", "issue_key"),
        ("CHANGELOG_EVENT", "issue_key"),
        ("EPIC", "issue_key"),
        ("STORY", "epic_key")
    ]
    OLD_EPIC_KEY: Final[str] = "EPIC-123"

    def setUp(self):
        patcher = patch("jira_analytics.utils.utils.Config")
        self.addCleanup(patcher.stop)  # Ensures patch is stopped after each test
        self.mock_config = patcher.start()
        self.mock_config.LANDING_TABLES = Mock()
        for table, _ in self.LANDING_TABLE_NAMES_AND_KEY:
            setattr(self.mock_config.LANDING_TABLES, table, f"{self.LANDING_DATASET_PATH}.{table}")

    @patch("jira_analytics.utils.utils.run_bq_query_with_params")
    @patch("jira_analytics.utils.utils.logging")
    def test_handle_epic_key_change_success(
        self,
        mock_logging,
        mock_run_bq_query,
    ):
        handle_epic_key_change(self.OLD_EPIC_KEY)

        # Verify query calls for all tables
        expected_queries = [
            f"DELETE FROM `{self.LANDING_DATASET_PATH}.{table}` WHERE {key} = @old_epic_key;"
            for table, key in self.LANDING_TABLE_NAMES_AND_KEY
        ]

        # Assert that the correct queries were called
        assert mock_run_bq_query.call_count == len(expected_queries)

        # Collect calls made to mock_run_bq_query
        calls = mock_run_bq_query.call_args_list

        for i, expected_query in enumerate(expected_queries):
            actual_query, actual_job_config = calls[i][0]

            # Assert the query
            assert actual_query == expected_query

            # Assert the job config's parameters
            assert actual_job_config.query_parameters[0].name == "old_epic_key"
            assert actual_job_config.query_parameters[0].value == self.OLD_EPIC_KEY

        mock_logging.debug.assert_called()

    @patch("jira_analytics.utils.utils.run_bq_query_with_params", side_effect=Exception("Query failed"))
    @patch("jira_analytics.utils.utils.logging")
    def test_handle_epic_key_change_query_failure(
            self,
            mock_logging,
            mock_run_bq_query
    ):

        with pytest.raises(Exception, match="Query failed"):
            handle_epic_key_change(self.OLD_EPIC_KEY)

        # Ensure logging error is called
        mock_logging.error.assert_called_with(
            f"An error occurred while processing the key change for {self.OLD_EPIC_KEY}: Query failed"
        )
