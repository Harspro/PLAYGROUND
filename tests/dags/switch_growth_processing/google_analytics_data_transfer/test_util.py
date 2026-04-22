"""
Tests for util module: build_table_ref, append_datestamp, build_chunk_date_ranges,
build_iteration_items, check_table_exists, get_row_count, check_table_and_fetch_row_count,
check_source_table_and_fetch_row_count_callable, replicate_and_validate_table_callable,
get_validation_status, has_successful_target_replication.
"""
import pytest
from unittest.mock import patch, MagicMock
import pendulum

from dags.switch_growth_processing.google_analytics_data_transfer.utils.util import (
    build_table_ref,
    append_datestamp,
    build_chunk_date_ranges,
    build_iteration_items,
    check_table_exists,
    get_row_count,
    check_table_and_fetch_row_count,
    check_source_table_and_fetch_row_count_callable,
    replicate_and_validate_table_callable,
    get_validation_status,
    has_successful_target_replication,
    is_final_scheduled_run,
    extract_and_validate_config
)
from airflow.exceptions import AirflowException, AirflowSkipException


class TestExtractAndValidateConfig:

    def test_skip_vendor_replication_default_false(self):
        dag_config = {
            "bigquery": {
                "audit_table_ref": "proj.dataset.audit",
                "service_account": "svc-{env}",
                "source": {"dev": {"project_id": "p1", "dataset_id": "d1"}},
                "landing": {"project_id": "p2", "dataset_id": "d2"},
                "vendor": {"dev": {"project_id": "p3", "dataset_id": "d3"}},
            }
        }

        config = extract_and_validate_config(dag_config, "dev")

        assert config["skip_vendor_replication"] is False

    def test_skip_vendor_replication_true(self):
        dag_config = {
            "skip_vendor_replication": True,
            "bigquery": {
                "audit_table_ref": "proj.dataset.audit",
                "service_account": "svc-{env}",
                "source": {"dev": {"project_id": "p1", "dataset_id": "d1"}},
                "landing": {"project_id": "p2", "dataset_id": "d2"},
                "vendor": {"dev": {"project_id": "p3", "dataset_id": "d3"}},
            }
        }

        config = extract_and_validate_config(dag_config, "dev")

        assert config["skip_vendor_replication"] is True


class TestBuildTableRef:

    def test_build_table_ref_success(self):
        """Test building table reference from project, dataset, table."""
        result = build_table_ref('proj', 'ds', 'tbl')
        assert result == 'proj.ds.tbl'

    def test_build_table_ref_with_dashes(self):
        """Test table reference with project/dataset names containing dashes."""
        result = build_table_ref('my-project', 'my_dataset', 'my_table')
        assert result == 'my-project.my_dataset.my_table'


class TestAppendDatestamp:

    def test_append_datestamp_with_date_success(self):
        """Test appending datestamp when date is provided."""
        d = pendulum.date(2025, 12, 1)
        result = append_datestamp('events', d)
        assert result == 'events_20251201'

    def test_append_datestamp_with_datetime_date_success(self):
        """Test appending datestamp when date comes from datetime."""
        d = pendulum.datetime(2025, 6, 15, 12, 0, 0).date()
        result = append_datestamp('CALENDAR', d)
        assert result == 'CALENDAR_20250615'

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.pendulum')
    def test_append_datestamp_daily_none_uses_yesterday(self, mock_pendulum):
        """Test that date=None uses yesterday (daily load)."""
        mock_now = MagicMock()
        mock_now.subtract.return_value = MagicMock(strftime=lambda fmt: '20260122')
        mock_pendulum.now.return_value = mock_now
        mock_pendulum.timezone.return_value = MagicMock()

        result = append_datestamp('events', None)
        assert result == 'events_20260122'


class TestBuildChunkDateRanges:

    def test_build_chunk_date_ranges_single_day(self):
        """Test chunk range when start equals end."""
        start = pendulum.date(2025, 12, 1)
        end = pendulum.date(2025, 12, 1)
        ranges = list(build_chunk_date_ranges(start, end, 1))
        assert len(ranges) == 1
        assert ranges[0] == (start, end)

    def test_build_chunk_date_ranges_multiple_chunks(self):
        """Test chunk ranges with chunk_size 2."""
        start = pendulum.date(2025, 12, 1)
        end = pendulum.date(2025, 12, 5)
        ranges = list(build_chunk_date_ranges(start, end, 2))
        assert len(ranges) == 3
        assert ranges[0] == (pendulum.date(2025, 12, 1), pendulum.date(2025, 12, 2))
        assert ranges[1] == (pendulum.date(2025, 12, 3), pendulum.date(2025, 12, 4))
        assert ranges[2] == (pendulum.date(2025, 12, 5), pendulum.date(2025, 12, 5))

    def test_build_chunk_date_ranges_chunk_size_one_multiple_days(self):
        """Test chunk_size=1 creates one chunk per day."""
        start = pendulum.date(2025, 12, 1)
        end = pendulum.date(2025, 12, 5)
        ranges = list(build_chunk_date_ranges(start, end, 1))
        assert len(ranges) == 5
        assert ranges[0] == (pendulum.date(2025, 12, 1), pendulum.date(2025, 12, 1))
        assert ranges[1] == (pendulum.date(2025, 12, 2), pendulum.date(2025, 12, 2))
        assert ranges[2] == (pendulum.date(2025, 12, 3), pendulum.date(2025, 12, 3))
        assert ranges[3] == (pendulum.date(2025, 12, 4), pendulum.date(2025, 12, 4))
        assert ranges[4] == (pendulum.date(2025, 12, 5), pendulum.date(2025, 12, 5))

    def test_build_chunk_date_ranges_chunk_size_equals_range(self):
        """Test chunk_size exactly equals date range returns single chunk."""
        start = pendulum.date(2025, 12, 1)
        end = pendulum.date(2025, 12, 5)
        ranges = list(build_chunk_date_ranges(start, end, 5))
        assert len(ranges) == 1
        assert ranges[0] == (start, end)

    def test_build_chunk_date_ranges_chunk_size_larger_than_range(self):
        """Test chunk_size larger than date range returns single chunk."""
        start = pendulum.date(2025, 12, 1)
        end = pendulum.date(2025, 12, 3)
        ranges = list(build_chunk_date_ranges(start, end, 10))
        assert len(ranges) == 1
        assert ranges[0] == (start, end)

    def test_build_chunk_date_ranges_two_days_chunk_size_one(self):
        """Test two-day range with chunk_size=1."""
        start = pendulum.date(2025, 12, 1)
        end = pendulum.date(2025, 12, 2)
        ranges = list(build_chunk_date_ranges(start, end, 1))
        assert len(ranges) == 2
        assert ranges[0] == (pendulum.date(2025, 12, 1), pendulum.date(2025, 12, 1))
        assert ranges[1] == (pendulum.date(2025, 12, 2), pendulum.date(2025, 12, 2))

    def test_build_chunk_date_ranges_two_days_chunk_size_two(self):
        """Test two-day range with chunk_size=2."""
        start = pendulum.date(2025, 12, 1)
        end = pendulum.date(2025, 12, 2)
        ranges = list(build_chunk_date_ranges(start, end, 2))
        assert len(ranges) == 1
        assert ranges[0] == (start, end)

    def test_build_chunk_date_ranges_odd_chunk_size(self):
        """Test range with odd chunk_size that doesn't divide evenly."""
        start = pendulum.date(2025, 12, 1)
        end = pendulum.date(2025, 12, 7)
        ranges = list(build_chunk_date_ranges(start, end, 3))
        assert len(ranges) == 3
        assert ranges[0] == (pendulum.date(2025, 12, 1), pendulum.date(2025, 12, 3))
        assert ranges[1] == (pendulum.date(2025, 12, 4), pendulum.date(2025, 12, 6))
        assert ranges[2] == (pendulum.date(2025, 12, 7), pendulum.date(2025, 12, 7))

    def test_build_chunk_date_ranges_month_boundary(self):
        """Test chunk ranges crossing month boundary."""
        start = pendulum.date(2025, 1, 30)
        end = pendulum.date(2025, 2, 2)
        ranges = list(build_chunk_date_ranges(start, end, 2))
        assert len(ranges) == 2
        assert ranges[0] == (pendulum.date(2025, 1, 30), pendulum.date(2025, 1, 31))
        assert ranges[1] == (pendulum.date(2025, 2, 1), pendulum.date(2025, 2, 2))

    def test_build_chunk_date_ranges_year_boundary(self):
        """Test chunk ranges crossing year boundary."""
        start = pendulum.date(2025, 12, 30)
        end = pendulum.date(2026, 1, 2)
        ranges = list(build_chunk_date_ranges(start, end, 2))
        assert len(ranges) == 2
        assert ranges[0] == (pendulum.date(2025, 12, 30), pendulum.date(2025, 12, 31))
        assert ranges[1] == (pendulum.date(2026, 1, 1), pendulum.date(2026, 1, 2))

    def test_build_chunk_date_ranges_leap_year_february(self):
        """Test chunk ranges in leap year February."""
        start = pendulum.date(2024, 2, 28)
        end = pendulum.date(2024, 3, 1)
        ranges = list(build_chunk_date_ranges(start, end, 1))
        assert len(ranges) == 3
        assert ranges[0] == (pendulum.date(2024, 2, 28), pendulum.date(2024, 2, 28))
        assert ranges[1] == (pendulum.date(2024, 2, 29), pendulum.date(2024, 2, 29))
        assert ranges[2] == (pendulum.date(2024, 3, 1), pendulum.date(2024, 3, 1))

    def test_build_chunk_date_ranges_no_gaps_or_overlaps(self):
        """Test that chunks have no gaps or overlaps - consecutive chunks."""
        start = pendulum.date(2025, 12, 1)
        end = pendulum.date(2025, 12, 10)
        ranges = list(build_chunk_date_ranges(start, end, 3))

        # Verify no gaps: each chunk should start one day after previous chunk ends
        for i in range(len(ranges) - 1):
            prev_end = ranges[i][1]
            next_start = ranges[i + 1][0]
            assert prev_end.add(days=1) == next_start, \
                f"Gap between chunk {i} and {i+1}: {prev_end} -> {next_start}"

        # Verify all dates are covered
        all_dates = set()
        for chunk_start, chunk_end in ranges:
            current = chunk_start
            while current <= chunk_end:
                all_dates.add(current)
                current = current.add(days=1)

        expected_dates = set()
        current = start
        while current <= end:
            expected_dates.add(current)
            current = current.add(days=1)

        assert all_dates == expected_dates, "Not all dates are covered"

    def test_build_chunk_date_ranges_large_chunk_size(self):
        """Test with large chunk_size relative to range."""
        start = pendulum.date(2025, 12, 1)
        end = pendulum.date(2025, 12, 10)
        ranges = list(build_chunk_date_ranges(start, end, 7))
        assert len(ranges) == 2
        assert ranges[0] == (pendulum.date(2025, 12, 1), pendulum.date(2025, 12, 7))
        assert ranges[1] == (pendulum.date(2025, 12, 8), pendulum.date(2025, 12, 10))

    def test_build_chunk_date_ranges_chunk_size_seven_days(self):
        """Test chunk_size=7 (one week) with multiple weeks."""
        start = pendulum.date(2025, 12, 1)  # Monday
        end = pendulum.date(2025, 12, 15)
        ranges = list(build_chunk_date_ranges(start, end, 7))
        assert len(ranges) == 3
        assert ranges[0] == (pendulum.date(2025, 12, 1), pendulum.date(2025, 12, 7))
        assert ranges[1] == (pendulum.date(2025, 12, 8), pendulum.date(2025, 12, 14))
        assert ranges[2] == (pendulum.date(2025, 12, 15), pendulum.date(2025, 12, 15))

    def test_build_chunk_date_ranges_chunk_size_one_less_than_range(self):
        """Test chunk_size exactly one less than range results in 2 chunks."""
        start = pendulum.date(2025, 12, 1)
        end = pendulum.date(2025, 12, 6)  # 6 days total
        ranges = list(build_chunk_date_ranges(start, end, 5))  # chunk_size=5
        assert len(ranges) == 2
        assert ranges[0] == (pendulum.date(2025, 12, 1), pendulum.date(2025, 12, 5))
        assert ranges[1] == (pendulum.date(2025, 12, 6), pendulum.date(2025, 12, 6))

    def test_build_chunk_date_ranges_chunk_size_two_less_than_range(self):
        """Test chunk_size exactly two less than range results in 2 chunks with last chunk being 2 days."""
        start = pendulum.date(2025, 12, 1)
        end = pendulum.date(2025, 12, 6)  # 6 days total
        ranges = list(build_chunk_date_ranges(start, end, 4))  # chunk_size=4
        assert len(ranges) == 2
        assert ranges[0] == (pendulum.date(2025, 12, 1), pendulum.date(2025, 12, 4))
        assert ranges[1] == (pendulum.date(2025, 12, 5), pendulum.date(2025, 12, 6))

    def test_build_chunk_date_ranges_very_large_range(self):
        """Test with very large date range (1 year) and chunk_size=30."""
        start = pendulum.date(2025, 1, 1)
        end = pendulum.date(2025, 12, 31)  # 365 days
        ranges = list(build_chunk_date_ranges(start, end, 30))
        # Should have 13 chunks: 12 full chunks of 30 days + 1 final chunk of 5 days
        assert len(ranges) == 13
        assert ranges[0] == (pendulum.date(2025, 1, 1), pendulum.date(2025, 1, 30))
        # Chunk 11: starts at 2025-11-27 (after 11 chunks of 30 days from 2025-01-01)
        assert ranges[11] == (pendulum.date(2025, 11, 27), pendulum.date(2025, 12, 26))
        # Chunk 12: final chunk from 2025-12-27 to 2025-12-31 (5 days)
        assert ranges[12] == (pendulum.date(2025, 12, 27), pendulum.date(2025, 12, 31))
        # Verify no gaps
        for i in range(len(ranges) - 1):
            assert ranges[i][1].add(days=1) == ranges[i + 1][0]

    def test_build_chunk_date_ranges_february_non_leap_year(self):
        """Test February 28 in non-leap year (2025) to ensure no Feb 29 issues."""
        start = pendulum.date(2025, 2, 27)
        end = pendulum.date(2025, 3, 1)
        ranges = list(build_chunk_date_ranges(start, end, 1))
        assert len(ranges) == 3
        assert ranges[0] == (pendulum.date(2025, 2, 27), pendulum.date(2025, 2, 27))
        assert ranges[1] == (pendulum.date(2025, 2, 28), pendulum.date(2025, 2, 28))
        assert ranges[2] == (pendulum.date(2025, 3, 1), pendulum.date(2025, 3, 1))
        # Verify Feb 29 is not included (2025 is not a leap year)

    def test_build_chunk_date_ranges_exactly_two_chunks(self):
        """Test range that results in exactly 2 chunks with equal-sized chunks."""
        start = pendulum.date(2025, 12, 1)
        end = pendulum.date(2025, 12, 4)  # 4 days
        ranges = list(build_chunk_date_ranges(start, end, 2))
        assert len(ranges) == 2
        assert ranges[0] == (pendulum.date(2025, 12, 1), pendulum.date(2025, 12, 2))
        assert ranges[1] == (pendulum.date(2025, 12, 3), pendulum.date(2025, 12, 4))

    def test_build_chunk_date_ranges_chunk_size_three_with_four_day_range(self):
        """Test chunk_size=3 with 4-day range results in 2 chunks."""
        start = pendulum.date(2025, 12, 1)
        end = pendulum.date(2025, 12, 4)  # 4 days
        ranges = list(build_chunk_date_ranges(start, end, 3))
        assert len(ranges) == 2
        assert ranges[0] == (pendulum.date(2025, 12, 1), pendulum.date(2025, 12, 3))
        assert ranges[1] == (pendulum.date(2025, 12, 4), pendulum.date(2025, 12, 4))


class TestBuildIterationItems:

    def test_build_iteration_items_daily_single_table_success(self):
        """Test daily load with one table (table_name from dag_config['bigquery'])."""
        dag_config = {'load_type': 'daily', 'bigquery': {'table_name': 'events'}}
        items = build_iteration_items(dag_config)
        assert len(items) == 1
        assert items[0]['task_group_id'] == 'events'
        assert items[0]['table_name'] == 'events'
        assert items[0]['date'] is None

    def test_build_iteration_items_one_time_requires_chunk_size(self):
        """Test one_time without chunk_size raises."""
        dag_config = {
            'load_type': 'one_time',
            'bigquery': {'table_name': 'events'},
            'start_date': '2025-12-01',
            'end_date': '2025-12-03',
        }
        with pytest.raises(AirflowException, match='chunk_size is required'):
            build_iteration_items(dag_config)

    def test_build_iteration_items_one_time_single_day_chunk_size_one(self):
        """Test one_time for single date with chunk_size 1."""
        dag_config = {
            'load_type': 'one_time',
            'bigquery': {'table_name': 'events'},
            'start_date': '2025-12-01',
            'end_date': '2025-12-01',
            'chunk_size': 1,
        }
        items = build_iteration_items(dag_config)
        assert len(items) == 1
        assert items[0]['task_group_id'] == '20251201_20251201'
        assert items[0]['table_name'] == 'events'
        assert items[0]['chunk_start_date'] == '2025-12-01'
        assert items[0]['chunk_end_date'] == '2025-12-01'

    def test_build_iteration_items_one_time_date_range_with_chunks(self):
        """Test one_time for date range with chunk_size 2."""
        dag_config = {
            'load_type': 'one_time',
            'bigquery': {'table_name': 'events'},
            'start_date': '2025-12-01',
            'end_date': '2025-12-05',
            'chunk_size': 2,
        }
        items = build_iteration_items(dag_config)
        assert len(items) == 3
        assert items[0]['chunk_start_date'] == '2025-12-01'
        assert items[0]['chunk_end_date'] == '2025-12-02'
        assert items[1]['chunk_start_date'] == '2025-12-03'
        assert items[1]['chunk_end_date'] == '2025-12-04'
        assert items[2]['chunk_start_date'] == '2025-12-05'
        assert items[2]['chunk_end_date'] == '2025-12-05'

    def test_build_iteration_items_one_time_missing_start_end_raises(self):
        """Test one_time without start_date or end_date raises."""
        dag_config = {'load_type': 'one_time', 'bigquery': {'table_name': 'events'}, 'end_date': '2025-12-01'}
        with pytest.raises(AirflowException, match='Missing start_date or end_date'):
            build_iteration_items(dag_config)

    def test_build_iteration_items_one_time_start_after_end_raises(self):
        """Test one_time with start_date > end_date raises."""
        dag_config = {
            'load_type': 'one_time',
            'bigquery': {'table_name': 'events'},
            'start_date': '2025-12-02',
            'end_date': '2025-12-01',
            'chunk_size': 1,
        }
        with pytest.raises(AirflowException, match='start_date.*must be <= end_date'):
            build_iteration_items(dag_config)

    def test_build_iteration_items_missing_table_name_raises(self):
        """Test missing table_name in bigquery config raises."""
        dag_config = {'load_type': 'daily', 'bigquery': {}}
        with pytest.raises(AirflowException, match='Missing.*table_name'):
            build_iteration_items(dag_config)

    def test_build_iteration_items_missing_bigquery_uses_empty_dict(self):
        """Test missing bigquery key uses empty dict and raises for table_name."""
        dag_config = {'load_type': 'daily'}
        with pytest.raises(AirflowException, match='Missing.*table_name'):
            build_iteration_items(dag_config)

    def test_build_iteration_items_one_time_chunk_size_exceeds_range_raises(self):
        """Test one_time with chunk_size > date range raises."""
        dag_config = {
            'load_type': 'one_time',
            'bigquery': {'table_name': 'events'},
            'start_date': '2025-12-01',
            'end_date': '2025-12-03',
            'chunk_size': 5,
        }
        with pytest.raises(AirflowException, match='chunk_size.*cannot exceed date range'):
            build_iteration_items(dag_config)

    def test_build_iteration_items_one_time_single_day_chunk_size_not_one_raises(self):
        """Test one_time single day with chunk_size != 1 raises."""
        dag_config = {
            'load_type': 'one_time',
            'bigquery': {'table_name': 'events'},
            'start_date': '2025-12-01',
            'end_date': '2025-12-01',
            'chunk_size': 2,
        }
        with pytest.raises(AirflowException, match='chunk_size must be 1'):
            build_iteration_items(dag_config)

    def test_build_iteration_items_unknown_load_type_raises(self):
        """Test unknown load_type raises."""
        dag_config = {'load_type': 'weekly', 'bigquery': {'table_name': 'events'}}
        with pytest.raises(AirflowException, match='Unknown load_type'):
            build_iteration_items(dag_config)


class TestCheckTableExists:

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.bigquery.Client')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.impersonated_credentials.Credentials')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.google.auth.default')
    def test_check_table_exists_true(self, mock_auth, mock_creds, mock_client_class):
        """Test table exists returns True."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_auth.return_value = (MagicMock(), None)
        result = check_table_exists('proj.ds.tbl', 'svc@proj.iam.gserviceaccount.com')
        assert result is True

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.bigquery.Client')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.impersonated_credentials.Credentials')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.google.auth.default')
    def test_check_table_exists_false(self, mock_auth, mock_creds, mock_client_class):
        """Test table not found returns False."""
        from google.cloud.exceptions import NotFound
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_table.side_effect = NotFound('table')
        mock_auth.return_value = (MagicMock(), None)
        result = check_table_exists('proj.ds.tbl', 'svc@proj.iam.gserviceaccount.com')
        assert result is False


class TestGetRowCount:

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.run_bq_query')
    def test_get_row_count_success(self, mock_run_bq):
        """Test get row count returns integer."""
        mock_run_bq.return_value.result.return_value = [{'row_count': 42}]
        result = get_row_count('proj.ds.tbl', target_service_account='svc@proj.iam.gserviceaccount.com')
        assert result == 42

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.run_bq_query')
    def test_get_row_count_failure_returns_none(self, mock_run_bq):
        """Test get row count on exception returns None."""
        mock_run_bq.side_effect = Exception('query failed')
        result = get_row_count('proj.ds.tbl', target_service_account='svc@proj.iam.gserviceaccount.com')
        assert result is None


class TestCheckTableAndFetchRowCount:

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.get_row_count')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.check_table_exists')
    def test_check_table_and_fetch_row_count_success(self, mock_exists, mock_get_count):
        """Test successful check returns (table_ref, row_count)."""
        mock_exists.return_value = True
        mock_get_count.return_value = 100
        ref, cnt = check_table_and_fetch_row_count('proj.ds.tbl', 'svc@proj.iam.gserviceaccount.com')
        assert ref == 'proj.ds.tbl'
        assert cnt == 100

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.check_table_exists')
    def test_check_table_and_fetch_row_count_not_exists_raises(self, mock_exists):
        """Test table not existing raises AirflowException."""
        mock_exists.return_value = False
        with pytest.raises(AirflowException, match='Source table does not exist'):
            check_table_and_fetch_row_count('proj.ds.tbl', 'svc@proj.iam.gserviceaccount.com')


class TestCheckSourceTableAndFetchRowCountCallable:

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.append_datestamp')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.check_table_and_fetch_row_count')
    def test_check_callable_single_table_success(self, mock_check, mock_append_datestamp):
        """Test single-table mode (daily) returns source_table_ref, source_row_count, landing_table_ref, vendor_table_ref."""
        mock_append_datestamp.return_value = 'tbl_20260103'
        mock_check.return_value = ('proj.ds.tbl_20260103', 50)
        context = {'ti': MagicMock(), 'dag_run': MagicMock(conf={'load_date': '2026-01-03'})}  # Fixed conf

        result = check_source_table_and_fetch_row_count_callable(
            'svc@proj.iam.gserviceaccount.com',
            table_name='tbl',
            source_project='proj',
            source_dataset='ds',
            landing_project='land',
            landing_dataset='ds',
            vendor_project='vend',
            vendor_dataset='ds',
            **context,
        )

        assert result['source_table_ref'] == 'proj.ds.tbl_20260103'
        assert result['source_row_count'] == 50
        assert result['landing_table_ref'] == 'land.ds.tbl_20260103'
        assert result['vendor_table_ref'] == 'vend.ds.tbl_20260103'

    def test_check_callable_single_table_missing_source_ref_and_projects_raises(self):
        """Test daily mode without required table_name/projects/datasets raises."""
        with pytest.raises(AirflowException, match='table_name.*project.*dataset are required'):
            check_source_table_and_fetch_row_count_callable(
                'svc@proj.iam.gserviceaccount.com',
                chunk_start_date=None,
                chunk_end_date=None,
            )

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.check_table_and_fetch_row_count')
    def test_check_callable_chunk_mode_pushes_chunk_source_row_counts(self, mock_check):
        """Test chunk mode pushes chunk_source_row_counts to xcom."""
        mock_check.side_effect = [
            ('proj.ds.events_20251201', 10),
            ('proj.ds.events_20251202', 20),
        ]
        context = {'ti': MagicMock()}

        result = check_source_table_and_fetch_row_count_callable(
            'svc@proj.iam.gserviceaccount.com',
            chunk_start_date='2025-12-01',
            chunk_end_date='2025-12-02',
            table_name='events',
            source_project='proj',
            source_dataset='ds',
            **context,
        )

        assert '20251201' in result
        assert result['20251201']['source_table_ref'] == 'proj.ds.events_20251201'
        assert result['20251201']['source_row_count'] == 10
        assert '20251202' in result
        context['ti'].xcom_push.assert_called_once_with(key='chunk_source_row_counts', value=result)

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.check_table_and_fetch_row_count')
    def test_check_callable_daily_proceeds_when_no_skip_check(
        self, mock_check
    ):
        """Test daily path: check_source_table_and_fetch_row_count_callable no longer has skip logic - it's moved to replicate_and_validate_table_callable."""
        expected_ref = 'proj.ds.events_20260103'
        mock_check.return_value = (expected_ref, 50)
        context = {
            'ti': MagicMock(),
            'dag_run': MagicMock(dag_id='ga4_events_daily', run_id='scheduled__2026-02-17T08:30:00+00:00', conf={'load_date': '2026-01-03'}),  # Fixed conf
        }
        kwargs = {
            'service_account': 'svc@proj.iam.gserviceaccount.com',
            'table_name': 'events',
            'source_project': 'proj',
            'source_dataset': 'ds',
            'landing_project': 'land',
            'landing_dataset': 'ds',
            'vendor_project': 'vend',
            'vendor_dataset': 'ds',
            **context,
        }

        result = check_source_table_and_fetch_row_count_callable(**kwargs)

        mock_check.assert_called_once()
        assert result['source_table_ref'] == expected_ref

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.append_datestamp')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.check_table_and_fetch_row_count')
    def test_check_callable_daily_proceeds_when_no_prior_run_success(
        self, mock_check, mock_append_datestamp
    ):
        """Test daily path: check_source_table_and_fetch_row_count_callable proceeds without skip check (skip logic moved to replicate_and_validate_table_callable)."""
        mock_append_datestamp.return_value = 'events_20260103'
        expected_ref = 'proj.ds.events_20260103'
        mock_check.return_value = (expected_ref, 50)
        context = {
            'ti': MagicMock(),
            'dag_run': MagicMock(dag_id='ga4_events_daily', run_id='scheduled__2026-01-03T08:30:00+00:00', conf={'load_date': '2026-01-03'}),  # Fixed conf
        }
        result = check_source_table_and_fetch_row_count_callable(
            'svc@proj.iam.gserviceaccount.com',
            table_name='events',
            source_project='proj',
            source_dataset='ds',
            landing_project='land',
            landing_dataset='ds',
            vendor_project='vend',
            vendor_dataset='ds',
            **context,
        )

        mock_check.assert_called_once()
        assert result['source_table_ref'] == expected_ref
        assert result['source_row_count'] == 50

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.check_table_and_fetch_row_count')
    def test_check_callable_daily_run_with_params_uses_load_date(self, mock_check):
        """Runs with load_date in conf use that date for table refs."""
        expected_ref = 'proj.ds.events_20260103'
        mock_check.return_value = (expected_ref, 50)
        context = {
            'ti': MagicMock(),
            'dag_run': MagicMock(dag_id='ga4_events_daily', run_id='manual__2026-02-17T12:00:00+00:00', conf={'load_date': '2026-01-03'}),  # Fixed conf
        }
        result = check_source_table_and_fetch_row_count_callable(
            'svc@proj.iam.gserviceaccount.com',
            table_name='events',
            source_project='proj',
            source_dataset='ds',
            landing_project='land',
            landing_dataset='ds',
            vendor_project='vend',
            vendor_dataset='ds',
            **context,
        )
        mock_check.assert_called_once_with(expected_ref, 'svc@proj.iam.gserviceaccount.com')
        assert result['source_table_ref'] == expected_ref

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.check_table_and_fetch_row_count')
    def test_check_callable_daily_uses_conf_load_date(self, mock_check):
        """Test daily path with dag_run.conf load_date uses that date for table refs."""
        mock_check.return_value = ('proj.ds.events_20260302', 100)
        context = {
            'ti': MagicMock(),
            'dag_run': MagicMock(conf={'load_date': '2026-03-02'}),  # Fixed conf
        }
        result = check_source_table_and_fetch_row_count_callable(
            'svc@proj.iam.gserviceaccount.com',
            source_table_ref=None,
            audit_table_ref='proj.ds.audit',
            table_name='events',
            source_project='proj',
            source_dataset='ds',
            landing_project='land',
            landing_dataset='ds',
            vendor_project='vend',
            vendor_dataset='ds',
            **context,
        )
        mock_check.assert_called_once_with('proj.ds.events_20260302', 'svc@proj.iam.gserviceaccount.com')
        assert result['source_table_ref'] == 'proj.ds.events_20260302'
        assert result['source_row_count'] == 100
        assert result['landing_table_ref'] == 'land.ds.events_20260302'
        assert result['vendor_table_ref'] == 'vend.ds.events_20260302'

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.is_final_scheduled_run', return_value=False)
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.check_table_and_fetch_row_count')
    def test_skips_on_early_run(self, mock_check, mock_is_final):
        mock_check.side_effect = AirflowException("table missing")

        # Fix: Ensure conf is a dictionary and returns the correct value (as a string)
        mock_dag_run = MagicMock()
        mock_dag_run.conf = {'load_date': '2026-01-01'}  # Fixed conf

        context = {
            'dag_run': mock_dag_run,
        }

        with pytest.raises(AirflowSkipException):
            check_source_table_and_fetch_row_count_callable(
                service_account="svc",
                table_name="users",
                source_project="p",
                source_dataset="d",
                landing_project="lp",
                landing_dataset="ld",
                vendor_project="vp",
                vendor_dataset="vd",
                **context
            )

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.is_final_scheduled_run', return_value=True)
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.check_table_and_fetch_row_count')
    def test_fails_on_final_run(self, mock_check, mock_is_final):
        mock_check.side_effect = AirflowException("table missing")

        # Fix: Ensure conf is a dictionary and returns the correct value (as a string)
        mock_dag_run = MagicMock()
        mock_dag_run.conf = {'load_date': '2026-01-01'}  # Fixed conf

        context = {
            'dag_run': mock_dag_run,
        }

        with pytest.raises(AirflowException):
            check_source_table_and_fetch_row_count_callable(
                service_account="svc",
                table_name="users",
                source_project="p",
                source_dataset="d",
                landing_project="lp",
                landing_dataset="ld",
                vendor_project="vp",
                vendor_dataset="vd",
                **context
            )

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.check_table_and_fetch_row_count')
    def test_success_path(self, mock_check):
        mock_check.return_value = ("table_ref", 100)

        # Fix: Ensure conf is a dictionary and returns the correct value (as a string)
        mock_dag_run = MagicMock()
        mock_dag_run.conf = {'load_date': '2026-01-01'}  # Fixed conf

        context = {
            'dag_run': mock_dag_run,
        }

        result = check_source_table_and_fetch_row_count_callable(
            service_account="svc",
            table_name="users",
            source_project="p",
            source_dataset="d",
            landing_project="lp",
            landing_dataset="ld",
            vendor_project="vp",
            vendor_dataset="vd",
            **context
        )

        assert result["source_table_ref"] == "table_ref"
        assert result["source_row_count"] == 100


class TestReplicateAndValidateTableCallable:

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.GA4DataTransferAuditUtil')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.get_row_count')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.replicate_source_to_target')
    def test_replicate_callable_single_table_success(
        self, mock_replicate, mock_get_count, mock_audit_util_class
    ):
        """Test single-table replicate copies, validates, logs, and does not raise."""
        mock_audit_util = MagicMock()
        mock_audit_util_class.return_value = mock_audit_util
        ti = MagicMock(task_id='copy_landing')
        ti.xcom_pull.return_value = {
            'source_table_ref': 'src.p.ds.t1',
            'source_row_count': 10,
            'landing_table_ref': 'tgt.p.ds.t1',
            'vendor_table_ref': 'vendor.p.ds.t1',
        }
        ti = MagicMock(task_id='copy_landing')
        ti.xcom_pull.return_value = {
            'source_table_ref': 'src.p.ds.t1',
            'source_row_count': 10,
            'landing_table_ref': 'tgt.p.ds.t1',
            'vendor_table_ref': 'vendor.p.ds.t1',
        }
        context = {
            'ti': ti,
            'ti': ti,
            'dag_run': MagicMock(run_id='run_1'),
            'dag': MagicMock(dag_id='dag_1'),
        }
        mock_get_count.return_value = 10

        replicate_and_validate_table_callable(
            check_source_existence_task_id='tg.check_table',
            audit_table_ref='proj.ds.audit',
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            service_account='svc@proj.iam.gserviceaccount.com',
            target_table_xcom_key='landing_table_ref',
            **context,
        )

        mock_replicate.assert_called_once()
        mock_audit_util.log_changes_to_audit_table.assert_called_once()
        call_args = mock_audit_util.log_changes_to_audit_table.call_args
        assert call_args[0][0] == 'proj.ds.audit'  # audit_table_ref
        assert call_args[0][1] == 'run_1'  # dag_run_id
        assert call_args[0][2] == 'dag_1'  # dag_id
        assert call_args[0][3] == [  # copy_results
            (context['ti'].task_id, {
                'source_table': 'src.p.ds.t1',
                'target_table': 'tgt.p.ds.t1',
                'source_row_count': 10,
                'target_row_count': 10,
                'copy_status': 'SUCCESS',
                'validation_status': 'SUCCESS',
                'error_msg': None,
            }),
        ]

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.GA4DataTransferAuditUtil')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.get_row_count')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.replicate_source_to_target')
    def test_replicate_callable_single_table_pulls_target_from_xcom_by_key(
        self, mock_replicate, mock_get_count, mock_audit_util_class
    ):
        """Test single-table replicate pulls full xcom and gets target ref via target_table_xcom_key (e.g. 'landing_table_ref')."""
        mock_audit_util = MagicMock()
        mock_audit_util_class.return_value = mock_audit_util
        ti = MagicMock(task_id='replicate_and_validate_for_landing')
        ti.xcom_pull.return_value = {
            'source_table_ref': 'src.p.ds.events_20260302',
            'source_row_count': 10,
            'landing_table_ref': 'land.ds.events_20260302',
            'vendor_table_ref': 'vend.ds.events_20260302',
        }
        context = {
            'ti': ti,
            'dag_run': MagicMock(run_id='run_1'),
            'dag': MagicMock(dag_id='dag_1'),
        }
        mock_get_count.return_value = 10

        replicate_and_validate_table_callable(
            check_source_existence_task_id='events.check_table_existence',
            audit_table_ref='proj.ds.audit',
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            service_account='svc@proj.iam.gserviceaccount.com',
            target_table_xcom_key='landing_table_ref',
            **context,
        )

        mock_replicate.assert_called_once()
        call_kwargs = mock_replicate.call_args[1]
        assert call_kwargs['target_table_ref'] == 'land.ds.events_20260302'
        mock_audit_util.log_changes_to_audit_table.assert_called_once()

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.GA4DataTransferAuditUtil')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.get_row_count')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.replicate_source_to_target')
    def test_replicate_callable_single_table_missing_source_ref_raises(
        self, mock_replicate, mock_get_count, mock_audit_util_class
    ):
        """Test single-table replicate when xcom has no source_table_ref raises."""
        ti = MagicMock(task_id='copy_landing')
        ti.xcom_pull.return_value = {
            'landing_table_ref': 'tgt.p.ds.t1',
            'vendor_table_ref': 'vendor.p.ds.t1',
        }
        context = {
            'ti': ti,
            'ti': ti,
            'dag_run': MagicMock(run_id='run_1'),
            'dag': MagicMock(dag_id='dag_1'),
        }

        with pytest.raises(AirflowException, match='Missing source_table_ref from check task'):
            replicate_and_validate_table_callable(
                check_source_existence_task_id='tg.check_table',
                audit_table_ref='proj.ds.audit',
                write_disposition='WRITE_TRUNCATE',
                create_disposition='CREATE_IF_NEEDED',
                service_account='svc@proj.iam.gserviceaccount.com',
                target_table_xcom_key='landing_table_ref',
                **context,
            )

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.GA4DataTransferAuditUtil')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.get_row_count')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.replicate_source_to_target')
    def test_replicate_callable_single_table_copy_failure(
        self, mock_replicate, mock_get_count, mock_audit_util_class
    ):
        """Test single-table replicate when copy fails: copy_results include error_msg, then AirflowException."""
        mock_audit_util = MagicMock()
        mock_audit_util_class.return_value = mock_audit_util
        ti = MagicMock(task_id='copy_landing')
        ti.xcom_pull.return_value = {
            'source_table_ref': 'src.p.ds.t1',
            'source_row_count': 10,
            'landing_table_ref': 'tgt.p.ds.t1',
            'vendor_table_ref': 'vendor.p.ds.t1',
        }
        ti = MagicMock(task_id='copy_landing')
        ti.xcom_pull.return_value = {
            'source_table_ref': 'src.p.ds.t1',
            'source_row_count': 10,
            'landing_table_ref': 'tgt.p.ds.t1',
            'vendor_table_ref': 'vendor.p.ds.t1',
        }
        context = {
            'ti': ti,
            'ti': ti,
            'dag_run': MagicMock(run_id='run_1'),
            'dag': MagicMock(dag_id='dag_1'),
        }
        mock_replicate.side_effect = Exception('Permission denied')

        with pytest.raises(AirflowException, match='Copy or validation failed'):
            replicate_and_validate_table_callable(
                check_source_existence_task_id='tg.check_table',
                audit_table_ref='proj.ds.audit',
                write_disposition='WRITE_TRUNCATE',
                create_disposition='CREATE_IF_NEEDED',
                service_account='svc@proj.iam.gserviceaccount.com',
                target_table_xcom_key='landing_table_ref',
                **context,
            )

        mock_audit_util.log_changes_to_audit_table.assert_called_once()
        call_args = mock_audit_util.log_changes_to_audit_table.call_args
        copy_results = call_args[0][3]
        assert len(copy_results) == 1
        assert copy_results[0][1]['copy_status'] == 'FAILED'
        assert copy_results[0][1]['error_msg'] is not None
        assert 'Copy failed:' in copy_results[0][1]['error_msg']
        assert 'Permission denied' in copy_results[0][1]['error_msg']

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.GA4DataTransferAuditUtil')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.get_row_count')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.replicate_source_to_target')
    def test_replicate_callable_single_table_validation_failure(
        self, mock_replicate, mock_get_count, mock_audit_util_class
    ):
        """Test single-table replicate when validation fails: copy_results include error_msg."""
        mock_audit_util = MagicMock()
        mock_audit_util_class.return_value = mock_audit_util
        ti = MagicMock(task_id='copy_landing')
        ti.xcom_pull.return_value = {
            'source_table_ref': 'src.p.ds.t1',
            'source_row_count': 10,
            'landing_table_ref': 'tgt.p.ds.t1',
            'vendor_table_ref': 'vendor.p.ds.t1',
        }
        ti = MagicMock(task_id='copy_landing')
        ti.xcom_pull.return_value = {
            'source_table_ref': 'src.p.ds.t1',
            'source_row_count': 10,
            'landing_table_ref': 'tgt.p.ds.t1',
            'vendor_table_ref': 'vendor.p.ds.t1',
        }
        context = {
            'ti': ti,
            'ti': ti,
            'dag_run': MagicMock(run_id='run_1'),
            'dag': MagicMock(dag_id='dag_1'),
        }
        mock_get_count.return_value = 5

        with pytest.raises(AirflowException, match='Copy or validation failed'):
            replicate_and_validate_table_callable(
                check_source_existence_task_id='tg.check_table',
                audit_table_ref='proj.ds.audit',
                write_disposition='WRITE_TRUNCATE',
                create_disposition='CREATE_IF_NEEDED',
                service_account='svc@proj.iam.gserviceaccount.com',
                target_table_xcom_key='landing_table_ref',
                **context,
            )

        mock_audit_util.log_changes_to_audit_table.assert_called_once()
        call_args = mock_audit_util.log_changes_to_audit_table.call_args
        copy_results = call_args[0][3]
        assert len(copy_results) == 1
        assert copy_results[0][1]['validation_status'] == 'FAILED'
        assert copy_results[0][1]['error_msg'] is not None
        assert 'Validation failed' in copy_results[0][1]['error_msg']
        assert '10' in copy_results[0][1]['error_msg']
        assert '5' in copy_results[0][1]['error_msg']

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.GA4DataTransferAuditUtil')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.get_row_count')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.replicate_source_to_target')
    def test_replicate_callable_chunk_mode_success(
        self, mock_replicate, mock_get_count, mock_audit_util_class
    ):
        """Test chunk-mode replicate: copy_results include error_msg None for each chunk."""
        mock_audit_util = MagicMock()
        mock_audit_util_class.return_value = mock_audit_util
        context = {
            'ti': MagicMock(task_id='copy_chunk'),
            'dag_run': MagicMock(run_id='run_1'),
            'dag': MagicMock(dag_id='dag_1'),
        }
        context['ti'].xcom_pull.return_value = {
            '20251201': {'source_table_ref': 'proj.ds.events_20251201', 'source_row_count': 10},
            '20251202': {'source_table_ref': 'proj.ds.events_20251202', 'source_row_count': 20},
        }
        mock_get_count.side_effect = [10, 20]

        replicate_and_validate_table_callable(
            check_source_existence_task_id='tg.check_table',
            audit_table_ref='proj.ds.audit',
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            service_account='svc@proj.iam.gserviceaccount.com',
            chunk_start_date='2025-12-01',
            chunk_end_date='2025-12-02',
            table_name='events',
            source_project='proj',
            source_dataset='ds',
            target_project='tgt',
            target_dataset='ds',
            **context,
        )

        mock_audit_util.log_changes_to_audit_table.assert_called_once()
        call_args = mock_audit_util.log_changes_to_audit_table.call_args
        copy_results = call_args[0][3]
        assert len(copy_results) == 2
        for _, result in copy_results:
            assert result.get('error_msg') is None
            assert result['copy_status'] == 'SUCCESS'
            assert result['validation_status'] == 'SUCCESS'

    def test_skip_vendor_replication_true_skips_execution(self):
        ti = MagicMock()
        context = {
            "ti": ti,
            "dag_run": MagicMock(run_id="run_1", conf={}),
            "dag": MagicMock(dag_id="dag_1"),
        }

        # Should NOT raise, just return early
        replicate_and_validate_table_callable(
            check_source_existence_task_id="task1",
            audit_table_ref="proj.ds.audit",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            service_account="svc",
            target_table_xcom_key="landing_table_ref",
            skip_replication=True,
            **context,
        )

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.process_single_table_replication')
    def test_skip_vendor_replication_does_not_call_processing(self, mock_process):
        ti = MagicMock()
        context = {
            "ti": ti,
            "dag_run": MagicMock(run_id="run_1", conf={}),
            "dag": MagicMock(dag_id="dag_1"),
        }

        replicate_and_validate_table_callable(
            check_source_existence_task_id="task1",
            audit_table_ref="proj.ds.audit",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            service_account="svc",
            target_table_xcom_key="landing_table_ref",
            skip_replication=True,
            **context,
        )

        mock_process.assert_not_called()


class TestGetValidationStatus:

    def test_get_validation_status_success(self):
        """Test SUCCESS when both counts present and equal."""
        assert get_validation_status(10, 10) == 'SUCCESS'

    def test_get_validation_status_failed(self):
        """Test FAILED when counts present but not equal."""
        assert get_validation_status(10, 5) == 'FAILED'

    def test_get_validation_status_error_target_none(self):
        """Test ERROR when target_row_count is None."""
        assert get_validation_status(10, None) == 'ERROR'

    def test_get_validation_status_error_source_none_target_present(self):
        """Test FAILED when source is None but target present (counts don't match)."""
        assert get_validation_status(None, 10) == 'FAILED'

    def test_get_validation_status_both_none(self):
        """Test ERROR when both None (target_row_count is None)."""
        assert get_validation_status(None, None) == 'ERROR'


class TestHasSuccessfulTargetReplication:

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.run_bq_query')
    def test_returns_false_when_no_prior_runs(self, mock_run_bq_query):
        """When no successful replication exists for this target table in audit table, returns False."""
        mock_job = MagicMock()
        mock_job.result.return_value = []
        mock_run_bq_query.return_value = mock_job

        result = has_successful_target_replication(
            dag_id='my_dag',
            audit_table_ref='proj.ds.audit_table',
            service_account='svc@proj.iam.gserviceaccount.com',
            target_table_ref='proj.ds.events_20260216',
        )

        assert result is False
        mock_run_bq_query.assert_called_once()
        call_args = mock_run_bq_query.call_args[0][0]
        assert "target_table_ref = 'proj.ds.events_20260216'" in call_args
        assert "status = 'SUCCESS'" in call_args

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.run_bq_query')
    def test_returns_true_when_prior_run_success(self, mock_run_bq_query):
        """When a prior run successfully replicated to this target table, returns True."""
        mock_job = MagicMock()
        mock_job.result.return_value = [{'dag_run_id': 'run_0830'}]
        mock_run_bq_query.return_value = mock_job

        result = has_successful_target_replication(
            dag_id='my_dag',
            audit_table_ref='proj.ds.audit_table',
            service_account='svc@proj.iam.gserviceaccount.com',
            target_table_ref='proj.ds.events_20260216',
        )

        assert result is True
        mock_run_bq_query.assert_called_once()

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.run_bq_query')
    def test_returns_false_when_no_prior_successful_run_in_audit(self, mock_run_bq_query):
        """When audit table has no prior successful replication for this target table, returns False."""
        mock_job = MagicMock()
        mock_job.result.return_value = []
        mock_run_bq_query.return_value = mock_job

        result = has_successful_target_replication(
            dag_id='my_dag',
            audit_table_ref='proj.ds.audit_table',
            service_account='svc@proj.iam.gserviceaccount.com',
            target_table_ref='proj.ds.events_20260216',
        )

        assert result is False
        mock_run_bq_query.assert_called_once()
        call_args = mock_run_bq_query.call_args[0][0]
        assert "target_table_ref = 'proj.ds.events_20260216'" in call_args
        assert "status = 'SUCCESS'" in call_args

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.run_bq_query')
    def test_query_checks_specific_target_table(self, mock_run_bq_query):
        """Query checks for specific target_table_ref with SUCCESS status."""
        mock_job = MagicMock()
        mock_job.result.return_value = []
        mock_run_bq_query.return_value = mock_job

        has_successful_target_replication(
            dag_id='my_dag',
            audit_table_ref='proj.ds.audit_table',
            service_account='svc@proj.iam.gserviceaccount.com',
            target_table_ref='proj.ds.events_20260216',
        )

        call_args = mock_run_bq_query.call_args[0][0]
        assert "target_table_ref = 'proj.ds.events_20260216'" in call_args
        assert "status = 'SUCCESS'" in call_args
        assert "dag_id = 'my_dag'" in call_args

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.run_bq_query')
    def test_returns_false_when_audit_empty(self, mock_run_bq_query):
        """When audit table has no matching rows for this target table, returns False."""
        mock_job = MagicMock()
        mock_job.result.return_value = []
        mock_run_bq_query.return_value = mock_job

        result = has_successful_target_replication(
            dag_id='my_dag',
            audit_table_ref='proj.ds.audit_table',
            service_account='svc@proj.iam.gserviceaccount.com',
            target_table_ref='proj.ds.events_20260216',
        )

        assert result is False

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.run_bq_query')
    def test_returns_true_when_audit_has_prior_successful_run(self, mock_run_bq_query):
        """When audit table has a prior successful replication for this target table, returns True."""
        mock_job = MagicMock()
        mock_job.result.return_value = [{'dag_run_id': 'run_0830'}]
        mock_run_bq_query.return_value = mock_job

        result = has_successful_target_replication(
            dag_id='my_dag',
            audit_table_ref='proj.ds.audit_table',
            service_account='svc@proj.iam.gserviceaccount.com',
            target_table_ref='proj.ds.events_20260216',
        )

        assert result is True

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.run_bq_query')
    def test_audit_query_called_with_correct_params(self, mock_run_bq_query):
        """run_bq_query is called with query checking target_table_ref, service_account, target_project."""
        mock_job = MagicMock()
        mock_job.result.return_value = [{'dag_run_id': 'run_0830'}]
        mock_run_bq_query.return_value = mock_job

        has_successful_target_replication(
            dag_id='my_dag',
            audit_table_ref='proj.ds.audit_table',
            service_account='svc@proj.iam.gserviceaccount.com',
            target_table_ref='proj.ds.events_20260216',
        )

        mock_run_bq_query.assert_called_once()
        assert mock_run_bq_query.call_args[0][1] == 'svc@proj.iam.gserviceaccount.com'
        assert mock_run_bq_query.call_args[0][2] == 'proj'
        assert "target_table_ref = 'proj.ds.events_20260216'" in mock_run_bq_query.call_args[0][0]
        assert "status = 'SUCCESS'" in mock_run_bq_query.call_args[0][0]

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.run_bq_query')
    def test_returns_false_on_audit_query_exception(self, mock_run_bq_query):
        """When audit table query raises, returns False and proceeds (fail-open)."""
        mock_run_bq_query.side_effect = Exception("Audit table unavailable")

        result = has_successful_target_replication(
            dag_id='my_dag',
            audit_table_ref='proj.ds.audit_table',
            service_account='svc@proj.iam.gserviceaccount.com',
            target_table_ref='proj.ds.events_20260216',
        )

        assert result is False


class TestIsFinalScheduledRun:

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.pendulum')
    def test_returns_false_when_no_dag_run(self, mock_pendulum):
        context = {}
        result = is_final_scheduled_run(context)
        assert result is False

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.pendulum')
    def test_returns_false_when_not_scheduled_run(self, mock_pendulum):
        context = {
            'dag_run': MagicMock(run_type='manual')
        }
        result = is_final_scheduled_run(context)
        assert result is False

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.LAST_SCHEDULED_RUN', 9)
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.pendulum')
    def test_returns_true_when_scheduled_and_final_hour_matches(self, mock_pendulum):
        mock_now = MagicMock()
        # Simulate the current time as 9 AM in Toronto time
        mock_now.hour = 9
        mock_pendulum.now.return_value = mock_now

        context = {
            'dag_run': MagicMock(run_type='scheduled')
        }

        result = is_final_scheduled_run(context)
        assert result is True

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.LAST_SCHEDULED_RUN', 9)
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.util.pendulum')
    def test_returns_false_when_scheduled_but_hour_does_not_match(self, mock_pendulum):
        mock_now = MagicMock()
        # Simulate the current time as 8 AM in Toronto time (hour mismatch)
        mock_now.hour = 8
        mock_pendulum.now.return_value = mock_now

        context = {
            'dag_run': MagicMock(run_type='scheduled')
        }

        result = is_final_scheduled_run(context)
        assert result is False
