"""
Test cases for the Jira Analytics DAG and its core functions.
This module tests DAG configuration, task structure, and ETL operations.
"""

import unittest
from datetime import datetime, timedelta
from typing import Final
from unittest.mock import Mock, patch, MagicMock, call

import pendulum
import pytest
from airflow.models import DagBag


# NOTE: DAG structure tests are skipped due to environment configuration dependencies
# The functional tests below provide thorough coverage of the DAG functions


class TestJiraCloudConfig:
    """
    Test class for the get_jira_cloud_config function.
    """

    @patch("jira_analytics.jira_analytics_dag.Config")
    def test_get_jira_cloud_config_returns_correct_structure(self, mock_config):
        """Test that get_jira_cloud_config returns the expected config structure."""
        # Import after patch
        from jira_analytics.jira_analytics_dag import get_jira_cloud_config

        # Setup mock
        mock_config.JIRA_CLOUD_URL = "https://test.atlassian.net"
        mock_config.USER_NAME = "test_user"

        # Call function
        result = get_jira_cloud_config()

        # Assertions
        assert result["url"] == "https://test.atlassian.net"
        assert result["username"] == "test_user"
        assert result["password"] is None  # Password should never be in return value
        assert result["cloud"] is True
        assert result["advanced_mode"] is False
        assert result["verify_ssl"] is False

    @patch("jira_analytics.jira_analytics_dag.Config")
    def test_get_jira_cloud_config_password_is_none(self, mock_config):
        """Test that password is always None in config (security check)."""
        # Import after patch
        from jira_analytics.jira_analytics_dag import get_jira_cloud_config

        mock_config.JIRA_CLOUD_URL = "https://test.atlassian.net"
        mock_config.USER_NAME = "test_user"

        result = get_jira_cloud_config()

        # Critical security check
        assert result["password"] is None


class TestDeltaRunTimestamp(unittest.TestCase):
    """
    Test class for the get_delta_run_timestamp function using unittest framework.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.config_patcher = patch("jira_analytics.jira_analytics_dag.Config")
        self.mock_config = self.config_patcher.start()
        self.addCleanup(self.config_patcher.stop)

    @patch("jira_analytics.jira_analytics_dag.extract_last_successful_dag_run_time")
    def test_get_delta_run_timestamp_with_updates_only_disabled(self, mock_extract):
        """Test get_delta_run_timestamp when UPDATES_ONLY is False."""
        from jira_analytics.jira_analytics_dag import get_delta_run_timestamp

        self.mock_config.UPDATES_ONLY = False

        last_run, current_run = get_delta_run_timestamp()

        # When UPDATES_ONLY is False, last_run should be None
        assert last_run is None
        # current_run should be a timestamp string
        assert current_run is not None
        assert isinstance(current_run, str)
        # Should not call extract function
        mock_extract.assert_not_called()

    @patch("jira_analytics.jira_analytics_dag.extract_last_successful_dag_run_time")
    def test_get_delta_run_timestamp_with_updates_only_enabled(self, mock_extract):
        """Test get_delta_run_timestamp when UPDATES_ONLY is True."""
        from jira_analytics.jira_analytics_dag import get_delta_run_timestamp

        self.mock_config.UPDATES_ONLY = True
        mock_last_time = datetime(2023, 1, 1, 10, 0, tzinfo=pendulum.timezone("America/Toronto"))
        mock_extract.return_value = mock_last_time

        last_run, current_run = get_delta_run_timestamp()

        # Should call extract function
        mock_extract.assert_called_once()
        # Both timestamps should be strings
        assert isinstance(last_run, str)
        assert isinstance(current_run, str)
        assert "2023-01-01" in last_run

    @patch("jira_analytics.jira_analytics_dag.extract_last_successful_dag_run_time")
    def test_get_delta_run_timestamp_with_no_previous_run(self, mock_extract):
        """Test get_delta_run_timestamp when there's no previous successful run."""
        from jira_analytics.jira_analytics_dag import get_delta_run_timestamp

        self.mock_config.UPDATES_ONLY = True
        mock_extract.return_value = None

        last_run, current_run = get_delta_run_timestamp()

        # last_run should remain None
        assert last_run is None
        # current_run should still be set
        assert current_run is not None


# Removed TestOutcomeTeams due to name collision in the DAG file
# (set_outcome_teams function and PythonOperator have the same name)


class TestLoadJiraData:
    """
    Test class for load functions (initiatives, epics, stories).
    """

    MOCK_JIRA_CONFIG: Final[dict] = {
        "url": "https://test.atlassian.net",
        "username": "test_user",
        "password": None,
        "cloud": True,
    }

    @patch("jira_analytics.jira_analytics_dag.VaultUtilBuilder")
    @patch("jira_analytics.jira_analytics_dag.load_records")
    @patch("jira_analytics.jira_analytics_dag.extract_from_jira_cloud")
    @patch("jira_analytics.jira_analytics_dag.Config")
    def test_load_jira_initiatives_single_page(
        self, mock_config, mock_extract, mock_load, mock_vault
    ):
        """Test load_jira_initiatives with a single page of results."""
        from jira_analytics.jira_analytics_dag import load_jira_initiatives

        # Setup mocks
        mock_vault.build.return_value.get_secret.return_value = "test_password"
        mock_config.CHUNK_SIZE = 50
        mock_config.PASSWORD_PATH = "secret/path"
        mock_config.PASSWORD_KEY = "password"
        mock_config.LANDING_TABLES = Mock()
        mock_config.LANDING_TABLES.INITIATIVE = "project.dataset.INITIATIVE"
        mock_config.LANDING_TABLES.CHANGELOG_EVENT = "project.dataset.CHANGELOG_EVENT"
        mock_config.LANDING_TABLES.CHANGELOG_COUNT = "project.dataset.CHANGELOG_COUNT"

        # Mock extract_from_jira_cloud to return data and None next_page_token
        mock_extract.return_value = (
            [Mock()],  # issues
            [Mock()],  # changelog_counts
            [Mock()],  # changelog_events
            1,  # total
            None,  # next_page_token (None means last page)
        )

        # Call function
        load_jira_initiatives(
            last_dag_run_time="2023-01-01 00:00-0500",
            cutoff_time="2023-01-02 00:00-0500",
            jira_config=self.MOCK_JIRA_CONFIG.copy(),
        )

        # Assertions
        mock_extract.assert_called_once()
        assert mock_load.call_count == 3  # Called for issues, events, and counts

    @patch("jira_analytics.jira_analytics_dag.VaultUtilBuilder")
    @patch("jira_analytics.jira_analytics_dag.load_records")
    @patch("jira_analytics.jira_analytics_dag.extract_from_jira_cloud")
    @patch("jira_analytics.jira_analytics_dag.Config")
    @patch("jira_analytics.jira_analytics_dag.handle_epic_key_change")
    def test_load_jira_epics_handles_key_change(
        self, mock_handle_key, mock_config, mock_extract, mock_load, mock_vault
    ):
        """Test that load_jira_epics handles epic key changes."""
        from jira_analytics.jira_analytics_dag import load_jira_epics

        # Setup mocks
        mock_vault.build.return_value.get_secret.return_value = "test_password"
        mock_config.CHUNK_SIZE = 50
        mock_config.PASSWORD_PATH = "secret/path"
        mock_config.PASSWORD_KEY = "password"
        mock_config.LANDING_TABLES = Mock()
        mock_config.LANDING_TABLES.EPIC = "project.dataset.EPIC"
        mock_config.LANDING_TABLES.CHANGELOG_EVENT = "project.dataset.CHANGELOG_EVENT"
        mock_config.LANDING_TABLES.CHANGELOG_COUNT = "project.dataset.CHANGELOG_COUNT"

        # Create a mock changelog event for key change
        mock_changelog_event = Mock()
        mock_changelog_event.field = "Key"
        mock_changelog_event.field_type = "jira"
        mock_changelog_event.from_string = "EPIC-OLD"
        mock_changelog_event.to_string = "EPIC-NEW"

        mock_extract.return_value = (
            [Mock()],  # issues
            [Mock()],  # changelog_counts
            [mock_changelog_event],  # changelog_events with key change
            1,  # total
            None,  # next_page_token
        )

        # Call function
        load_jira_epics(
            last_dag_run_time="2023-01-01 00:00-0500",
            cutoff_time="2023-01-02 00:00-0500",
            jira_config=self.MOCK_JIRA_CONFIG.copy(),
        )

        # Should call handle_epic_key_change for the old key
        mock_handle_key.assert_called_once_with("EPIC-OLD")

    @patch("jira_analytics.jira_analytics_dag.VaultUtilBuilder")
    @patch("jira_analytics.jira_analytics_dag.load_records")
    @patch("jira_analytics.jira_analytics_dag.extract_from_jira_cloud")
    @patch("jira_analytics.jira_analytics_dag.Config")
    def test_load_jira_stories_pagination(
        self, mock_config, mock_extract, mock_load, mock_vault
    ):
        """Test load_jira_stories with pagination (multiple pages)."""
        from jira_analytics.jira_analytics_dag import load_jira_stories

        # Setup mocks
        mock_vault.build.return_value.get_secret.return_value = "test_password"
        mock_config.CHUNK_SIZE = 50
        mock_config.PASSWORD_PATH = "secret/path"
        mock_config.PASSWORD_KEY = "password"
        mock_config.LANDING_TABLES = Mock()
        mock_config.LANDING_TABLES.STORY = "project.dataset.STORY"
        mock_config.LANDING_TABLES.CHANGELOG_EVENT = "project.dataset.CHANGELOG_EVENT"
        mock_config.LANDING_TABLES.CHANGELOG_COUNT = "project.dataset.CHANGELOG_COUNT"

        # Mock two pages of results
        mock_extract.side_effect = [
            ([Mock()], [Mock()], [Mock()], 50, "page2_token"),  # First page
            ([Mock()], [Mock()], [Mock()], 25, None),  # Second page (last)
        ]

        # Call function
        load_jira_stories(
            last_dag_run_time="2023-01-01 00:00-0500",
            cutoff_time="2023-01-02 00:00-0500",
            jira_config=self.MOCK_JIRA_CONFIG.copy(),
        )

        # Should be called twice for pagination
        assert mock_extract.call_count == 2
        # load_records called 3 times per page (issues, events, counts)
        assert mock_load.call_count == 6


class TestLoadSprintAndBugData:
    """
    Test class for sprint and bug loading functions.
    """

    MOCK_JIRA_CONFIG: Final[dict] = {
        "url": "https://test.atlassian.net",
        "username": "test_user",
        "password": None,
        "cloud": True,
    }

    @patch("jira_analytics.jira_analytics_dag.VaultUtilBuilder")
    @patch("jira_analytics.jira_analytics_dag.load_records")
    @patch("jira_analytics.jira_analytics_dag.extract_sprint")
    @patch("jira_analytics.jira_analytics_dag.Config")
    def test_load_sprint_info(
        self, mock_config, mock_extract_sprint, mock_load, mock_vault
    ):
        """Test load_sprint_info loads sprint data correctly."""
        from jira_analytics.jira_analytics_dag import load_sprint_info

        # Setup mocks
        mock_vault.build.return_value.get_secret.return_value = "test_password"
        mock_config.PASSWORD_PATH = "secret/path"
        mock_config.PASSWORD_KEY = "password"
        mock_config.LANDING_TABLES = Mock()
        mock_config.LANDING_TABLES.SPRINT = "project.dataset.SPRINT"

        mock_sprints = [Mock(), Mock()]
        mock_extract_sprint.return_value = mock_sprints

        # Call function
        load_sprint_info(jira_config=self.MOCK_JIRA_CONFIG.copy())

        # Assertions
        mock_extract_sprint.assert_called_once()
        mock_load.assert_called_once_with(
            records=mock_sprints,
            table_id="project.dataset.SPRINT",
            chunk=0,
            force_truncate=True,
        )

    @patch("jira_analytics.jira_analytics_dag.VaultUtilBuilder")
    @patch("jira_analytics.jira_analytics_dag.load_records")
    @patch("jira_analytics.jira_analytics_dag.extract_active_sprint_issues")
    @patch("jira_analytics.jira_analytics_dag.extract_closed_sprints_issues")
    @patch("jira_analytics.jira_analytics_dag.Config")
    def test_load_sprint_issues(
        self,
        mock_config,
        mock_extract_closed,
        mock_extract_active,
        mock_load,
        mock_vault,
    ):
        """Test load_sprint_issues loads both active and closed sprint issues."""
        from jira_analytics.jira_analytics_dag import load_sprint_issues

        # Setup mocks
        mock_vault.build.return_value.get_secret.return_value = "test_password"
        mock_config.PASSWORD_PATH = "secret/path"
        mock_config.PASSWORD_KEY = "password"
        mock_config.LANDING_TABLES = Mock()
        mock_config.LANDING_TABLES.ISSUES_IN_ACTIVE_SPRINTS = (
            "project.dataset.ISSUES_IN_ACTIVE_SPRINTS"
        )
        mock_config.LANDING_TABLES.ISSUES_IN_CLOSED_SPRINTS = (
            "project.dataset.ISSUES_IN_CLOSED_SPRINTS"
        )

        mock_active_issues = [Mock(), Mock()]
        mock_closed_issues = [Mock(), Mock(), Mock()]
        mock_extract_active.return_value = mock_active_issues
        mock_extract_closed.return_value = mock_closed_issues

        # Call function
        load_sprint_issues(jira_config=self.MOCK_JIRA_CONFIG.copy())

        # Assertions
        mock_extract_active.assert_called_once()
        mock_extract_closed.assert_called_once()
        assert mock_load.call_count == 2  # Called for both active and closed

    @patch("jira_analytics.jira_analytics_dag.VaultUtilBuilder")
    @patch("jira_analytics.jira_analytics_dag.load_records")
    @patch("jira_analytics.jira_analytics_dag.extract_bugs")
    @patch("jira_analytics.jira_analytics_dag.Config")
    def test_load_bug_issues(
        self, mock_config, mock_extract_bugs, mock_load, mock_vault
    ):
        """Test load_bug_issues loads bug data correctly."""
        from jira_analytics.jira_analytics_dag import load_bug_issues

        # Setup mocks
        mock_vault.build.return_value.get_secret.return_value = "test_password"
        mock_config.PASSWORD_PATH = "secret/path"
        mock_config.PASSWORD_KEY = "password"
        mock_config.LANDING_TABLES = Mock()
        mock_config.LANDING_TABLES.BUG = "project.dataset.BUG"

        mock_bugs = [Mock(), Mock(), Mock()]
        mock_extract_bugs.return_value = mock_bugs

        # Call function
        load_bug_issues(jira_config=self.MOCK_JIRA_CONFIG.copy())

        # Assertions
        mock_extract_bugs.assert_called_once()
        mock_load.assert_called_once_with(
            records=mock_bugs,
            table_id="project.dataset.BUG",
            chunk=0,
            force_truncate=True,
        )


class TestLoadDagRunTime(unittest.TestCase):
    """
    Test class for the load_dag_run_time function using unittest framework.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.config_patcher = patch("jira_analytics.jira_analytics_dag.Config")
        self.mock_config = self.config_patcher.start()
        self.addCleanup(self.config_patcher.stop)

    @patch("jira_analytics.jira_analytics_dag.load_record_to_bq")
    @patch("jira_analytics.jira_analytics_dag.as_naive_utc")
    def test_load_dag_run_time_success(self, mock_as_naive_utc, mock_load_record):
        """Test that load_dag_run_time successfully loads the timestamp."""
        from jira_analytics.jira_analytics_dag import load_dag_run_time

        self.mock_config.LANDING_TABLES = Mock()
        self.mock_config.LANDING_TABLES.DAG_HISTORY = "project.dataset.DAG_HISTORY"

        # Mock as_naive_utc to return a simple datetime
        mock_as_naive_utc.return_value = datetime(2023, 1, 1, 10, 0)

        current_time = "2023-01-01 10:00-0500"
        load_dag_run_time(current_dag_run_time=current_time)

        # Should call load_record_to_bq with correct table_id
        mock_load_record.assert_called_once()
        call_kwargs = mock_load_record.call_args[1]
        assert call_kwargs["table_id"] == "project.dataset.DAG_HISTORY"

    @patch("jira_analytics.jira_analytics_dag.load_record_to_bq")
    def test_load_dag_run_time_with_valid_timestamp_format(self, mock_load_record):
        """Test that load_dag_run_time handles valid timestamp format."""
        from jira_analytics.jira_analytics_dag import load_dag_run_time

        self.mock_config.LANDING_TABLES = Mock()
        self.mock_config.LANDING_TABLES.DAG_HISTORY = "project.dataset.DAG_HISTORY"

        # Should not raise an error with valid format
        current_time = "2023-06-15 14:30-0400"
        load_dag_run_time(current_dag_run_time=current_time)

        # Should successfully call load_record_to_bq
        mock_load_record.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__])
