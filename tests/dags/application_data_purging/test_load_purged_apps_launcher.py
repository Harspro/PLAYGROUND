import pytest
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowException
from application_data_purging.load_purged_apps_launcher import validate_purge_reports


@pytest.fixture
def mock_context():
    """Fixture to provide mock Airflow context."""
    return {
        'dag_run': MagicMock(conf={'report_timestamp': '202104280101010101'})
    }


@pytest.fixture
def mock_gcs_utils():
    with patch('util.gcs_utils.read_file') as mock_read_file:
        yield mock_read_file


def test_validate_purge_reports_success(mock_context, mock_gcs_utils):
    """Test case where purge is successful."""
    # Mock read_file to return a well-formatted CSV report
    mock_gcs_utils.return_value = "header\n1,2,3,0"

    validate_purge_reports(**mock_context)  # This should pass without exceptions


def test_validate_purge_reports_failure(mock_context, mock_gcs_utils):
    """Test case where purge is incomplete (rows remaining)."""
    # Mock read_file to return a report with remaining rows
    mock_gcs_utils.return_value = "header\n1,2,3,1"

    with pytest.raises(AirflowException, match=r"Please purge them and rerun"):
        validate_purge_reports(**mock_context)
