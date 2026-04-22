from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from prmi_jira_tickets.etl.extract import _parse_datetime_field


@patch("prmi_jira_tickets.etl.extract.parse", side_effect=ValueError("bad"))
@patch("prmi_jira_tickets.etl.extract.isna", return_value=False)
@patch("prmi_jira_tickets.etl.extract.to_datetime")
@patch("prmi_jira_tickets.etl.extract.as_naive_utc")
def test_parse_datetime_field_uses_fallback(
    mock_as_naive_utc,
    mock_to_datetime,
    _mock_isna,
    _mock_parse,
):
    fallback_dt = datetime(2025, 1, 2, 3, 4, 5)
    fallback_value = Mock()
    fallback_value.to_pydatetime.return_value = fallback_dt
    mock_to_datetime.return_value = fallback_value
    mock_as_naive_utc.side_effect = lambda dt: dt

    result = _parse_datetime_field("not-a-date", "PRMI-1", "incident_start_date")

    assert result == fallback_dt
    mock_to_datetime.assert_called_once()


@patch("prmi_jira_tickets.etl.extract.parse", side_effect=ValueError("bad"))
@patch("prmi_jira_tickets.etl.extract.isna", return_value=True)
@patch("prmi_jira_tickets.etl.extract.to_datetime")
def test_parse_datetime_field_critical_logs_error(
    _mock_to_datetime,
    _mock_isna,
    _mock_parse,
    caplog,
):
    with caplog.at_level("ERROR"):
        result = _parse_datetime_field("bad", "PRMI-2", "incident_end_date")

    assert result is None
    assert any("Failed to parse incident_end_date" in rec.message for rec in caplog.records)


@patch("prmi_jira_tickets.etl.extract.parse", side_effect=ValueError("bad"))
@patch("prmi_jira_tickets.etl.extract.isna", return_value=True)
@patch("prmi_jira_tickets.etl.extract.to_datetime")
def test_parse_datetime_field_noncritical_logs_warning(
    _mock_to_datetime,
    _mock_isna,
    _mock_parse,
    caplog,
):
    with caplog.at_level("WARNING"):
        result = _parse_datetime_field("bad", "PRMI-3", "banner_end_time")

    assert result is None
    assert any("Failed to parse banner_end_time" in rec.message for rec in caplog.records)
