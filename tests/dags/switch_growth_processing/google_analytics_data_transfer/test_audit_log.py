"""
Tests for audit_log module: GA4DataTransferAuditUtil class methods.
Tests for create_audit_table, build_audit_record_values, and log_changes_to_audit_table.
"""
import pytest
from unittest.mock import patch, MagicMock

from airflow.exceptions import AirflowException

from dags.util.auditing_utils.audit_util import AuditUtil
from dags.util.auditing_utils.model.audit_record import AuditRecord
from dags.util.constants import DEPLOYMENT_ENVIRONMENT_NAME
from dags.switch_growth_processing.google_analytics_data_transfer.utils.audit_log import (
    GA4DataTransferAuditUtil,
    combine_status,
)


class TestGA4DataTransferAuditUtil:
    """Tests for GA4DataTransferAuditUtil (extends AuditUtil)."""

    def test_extends_audit_util(self):
        """Test that GA4DataTransferAuditUtil is a subclass of AuditUtil."""
        # Check that GA4DataTransferAuditUtil has AuditUtil in its base classes
        # We check by class name and module since imports might differ
        base_classes = GA4DataTransferAuditUtil.__bases__
        assert len(base_classes) > 0, "GA4DataTransferAuditUtil should have at least one base class"
        # Check that the base class is named AuditUtil (regardless of import path)
        base_class = base_classes[0]
        assert base_class.__name__ == 'AuditUtil', f"Expected base class named 'AuditUtil', got '{base_class.__name__}'"
        # Verify it's in the MRO
        assert 'AuditUtil' in [cls.__name__ for cls in GA4DataTransferAuditUtil.__mro__]


class TestCreateAuditTable:

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.audit_log.TableReference.from_string')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.audit_log.bigquery.Client')
    @patch('dags.util.auditing_utils.audit_util.read_variable_or_file')
    def test_create_audit_table_success(self, mock_read_var, mock_client_class, mock_from_string):
        """Test successful audit table creation."""
        mock_read_var.return_value = {DEPLOYMENT_ENVIRONMENT_NAME: 'dev'}
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_from_string.return_value = MagicMock()

        util = GA4DataTransferAuditUtil()
        util.create_audit_table('proj.ds.audit_table')

        mock_client_class.assert_called_once_with()
        mock_client.create_table.assert_called_once()
        # Verify exists_ok=True is passed
        call_kwargs = mock_client.create_table.call_args[1]
        assert call_kwargs.get('exists_ok') is True
        table_arg = mock_client.create_table.call_args[0][0]
        assert table_arg.description == 'Audit log for BigQuery table copy operations between projects'
        schema_names = [f.name for f in table_arg.schema]
        assert 'dag_run_id' in schema_names
        assert 'dag_id' in schema_names
        assert 'status' in schema_names  # Changed from copy_status/validation_status to single status
        assert 'created_at' in schema_names
        assert 'error_msg' in schema_names
        # Verify copy_status and validation_status are NOT in schema
        assert 'copy_status' not in schema_names
        assert 'validation_status' not in schema_names

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.audit_log.TableReference.from_string')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.audit_log.bigquery.Client')
    @patch('dags.util.auditing_utils.audit_util.read_variable_or_file')
    def test_create_audit_table_already_exists_no_raise(self, mock_read_var, mock_client_class, mock_from_string):
        """Test that create_table is called with exists_ok=True, preventing 'Already Exists' errors."""
        mock_read_var.return_value = {DEPLOYMENT_ENVIRONMENT_NAME: 'dev'}
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_from_string.return_value = MagicMock()

        util = GA4DataTransferAuditUtil()
        # Should not raise even if table exists (exists_ok=True handles this)
        util.create_audit_table('proj.ds.audit_table')

        mock_client.create_table.assert_called_once()
        # Verify exists_ok=True is passed
        call_kwargs = mock_client.create_table.call_args[1]
        assert call_kwargs.get('exists_ok') is True

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.audit_log.TableReference.from_string')
    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.audit_log.bigquery.Client')
    @patch('dags.util.auditing_utils.audit_util.read_variable_or_file')
    def test_create_audit_table_other_error_raises(self, mock_read_var, mock_client_class, mock_from_string):
        """Test that non-'Already Exists' errors are still re-raised even with exists_ok=True."""
        mock_read_var.return_value = {DEPLOYMENT_ENVIRONMENT_NAME: 'dev'}
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.create_table.side_effect = Exception('Permission denied')
        mock_from_string.return_value = MagicMock()

        util = GA4DataTransferAuditUtil()
        with pytest.raises(Exception, match='Permission denied'):
            util.create_audit_table('proj.ds.audit_table')

        # Verify exists_ok=True was still passed
        call_kwargs = mock_client.create_table.call_args[1]
        assert call_kwargs.get('exists_ok') is True


class TestCombineStatus:

    def test_combine_status_both_success(self):
        """Test combining status when both copy and validation succeed."""
        assert combine_status('SUCCESS', 'SUCCESS') == 'SUCCESS'

    def test_combine_status_copy_failed(self):
        """Test combining status when copy fails."""
        assert combine_status('FAILED', 'SUCCESS') == 'FAILED'
        assert combine_status('FAILED', 'FAILED') == 'FAILED'
        assert combine_status('FAILED', 'ERROR') == 'FAILED'

    def test_combine_status_validation_failed(self):
        """Test combining status when copy succeeds but validation fails."""
        assert combine_status('SUCCESS', 'FAILED') == 'VALIDATION_FAILED'
        assert combine_status('SUCCESS', 'ERROR') == 'VALIDATION_FAILED'


class TestBuildAuditRecordValues:

    def test_build_audit_record_values_success(self):
        """Test building audit record values string."""
        copy_result = {
            'source_table': 'src.p.ds.t',
            'target_table': 'tgt.p.ds.t',
            'source_row_count': 10,
            'target_row_count': 10,
        }
        util = GA4DataTransferAuditUtil()
        result = util.build_audit_record_values('run1', 'dag1', 'task1', copy_result)

        assert 'run1' in result
        assert 'dag1' in result
        assert 'task1' in result
        assert 'src.p.ds.t' in result
        assert 'tgt.p.ds.t' in result
        assert '10' in result
        assert "CURRENT_DATETIME('America/Toronto')" in result
        # Status should NOT be in the values (AuditUtil adds it)
        assert 'SUCCESS' not in result
        assert 'FAILED' not in result

    def test_build_audit_record_values_with_nulls(self):
        """Test building audit record values with NULL row counts."""
        copy_result = {
            'source_table': 'src.t',
            'target_table': 'tgt.t',
            'source_row_count': None,
            'target_row_count': None,
        }
        util = GA4DataTransferAuditUtil()
        result = util.build_audit_record_values('run1', 'dag1', 'task1', copy_result)

        assert 'NULL' in result
        assert 'src.t' in result
        assert 'tgt.t' in result

    def test_build_audit_record_values_with_error_msg(self):
        """Test building audit record values when error_msg is present."""
        copy_result = {
            'source_table': 'src.t',
            'target_table': 'tgt.t',
            'source_row_count': 10,
            'target_row_count': 5,
            'error_msg': 'Validation failed: source_row_count=10, target_row_count=5',
        }
        util = GA4DataTransferAuditUtil()
        result = util.build_audit_record_values('run1', 'dag1', 'task1', copy_result)

        assert 'Validation failed' in result
        assert '10' in result
        assert '5' in result

    def test_build_audit_record_values_error_msg_none(self):
        """Test building audit record values when error_msg is absent or None yields NULL."""
        copy_result = {
            'source_table': 'src.t',
            'target_table': 'tgt.t',
            'source_row_count': 10,
            'target_row_count': 10,
        }
        util = GA4DataTransferAuditUtil()
        result = util.build_audit_record_values('run1', 'dag1', 'task1', copy_result)

        # error_msg column should be NULL (no quoted error string)
        assert result.endswith('NULL') or ', NULL' in result


class TestLogChangesToAuditTable:

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.audit_log.TableReference')
    @patch('dags.util.auditing_utils.audit_util.read_variable_or_file')
    @patch('dags.util.auditing_utils.audit_util.bigquery.Client')
    def test_log_changes_to_audit_table_success(self, mock_client_class, mock_read_var, mock_table_ref_class):
        """Test successful logging using AuditUtil._insert_into_audit_table."""
        mock_read_var.return_value = {DEPLOYMENT_ENVIRONMENT_NAME: 'dev'}
        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = None
        mock_client_class.return_value = mock_client
        mock_table_ref = MagicMock()
        mock_table_ref.dataset_id = 'ds'
        mock_table_ref.table_id = 'audit'
        mock_table_ref_class.from_string.return_value = mock_table_ref

        util = GA4DataTransferAuditUtil()

        copy_results = [
            (
                'task_landing',
                {
                    'source_table': 'src.p.ds.t',
                    'target_table': 'tgt.p.ds.t',
                    'source_row_count': 10,
                    'target_row_count': 10,
                    'copy_status': 'SUCCESS',
                    'validation_status': 'SUCCESS',
                },
            ),
        ]

        util.log_changes_to_audit_table(
            audit_table_ref='proj.ds.audit',
            dag_run_id='run1',
            dag_id='dag1',
            copy_results=copy_results,
        )

        # Verify _insert_into_audit_table was called with correct status
        mock_client.query.assert_called_once()
        call_args = mock_client.query.call_args[0][0]
        assert 'SUCCESS' in call_args
        assert 'ds' in call_args
        assert 'audit' in call_args

    @patch('dags.util.auditing_utils.audit_util.read_variable_or_file')
    def test_log_changes_to_audit_table_empty_results_returns_early(self, mock_read_var):
        """Test that empty copy_results returns without calling _insert_into_audit_table."""
        mock_read_var.return_value = {DEPLOYMENT_ENVIRONMENT_NAME: 'dev'}
        util = GA4DataTransferAuditUtil()
        # Should not raise and should not call bigquery
        util.log_changes_to_audit_table(
            audit_table_ref='proj.ds.audit',
            dag_run_id='run1',
            dag_id='dag1',
            copy_results=[],
        )

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.audit_log.TableReference')
    @patch('dags.util.auditing_utils.audit_util.read_variable_or_file')
    @patch('dags.util.auditing_utils.audit_util.bigquery.Client')
    def test_log_changes_to_audit_table_skips_empty_result_items(self, mock_client_class, mock_read_var, mock_table_ref_class):
        """Test that None copy_result entries are skipped."""
        mock_read_var.return_value = {DEPLOYMENT_ENVIRONMENT_NAME: 'dev'}
        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = None
        mock_client_class.return_value = mock_client
        mock_table_ref = MagicMock()
        mock_table_ref.dataset_id = 'ds'
        mock_table_ref.table_id = 'audit'
        mock_table_ref_class.from_string.return_value = mock_table_ref

        util = GA4DataTransferAuditUtil()

        copy_results = [
            ('task_landing', None),
            (
                'task_vendor',
                {
                    'source_table': 's.t',
                    'target_table': 'v.t',
                    'source_row_count': 5,
                    'target_row_count': 5,
                    'copy_status': 'SUCCESS',
                    'validation_status': 'SUCCESS',
                },
            ),
        ]

        util.log_changes_to_audit_table(
            audit_table_ref='proj.ds.audit',
            dag_run_id='run1',
            dag_id='dag1',
            copy_results=copy_results,
        )

        # Should only call query once (for task_vendor, skipping task_landing)
        assert mock_client.query.call_count == 1
        call_args = mock_client.query.call_args[0][0]
        assert 'task_vendor' in call_args
        assert 'v.t' in call_args

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.audit_log.TableReference')
    @patch('dags.util.auditing_utils.audit_util.read_variable_or_file')
    @patch('dags.util.auditing_utils.audit_util.bigquery.Client')
    def test_log_changes_to_audit_table_insert_failure_raises(self, mock_client_class, mock_read_var, mock_table_ref_class):
        """Test that insert failure raises with clear message."""
        mock_read_var.return_value = {DEPLOYMENT_ENVIRONMENT_NAME: 'dev'}
        mock_client = MagicMock()
        mock_client.query.return_value.result.side_effect = Exception('Query failed')
        mock_client_class.return_value = mock_client
        mock_table_ref = MagicMock()
        mock_table_ref.dataset_id = 'ds'
        mock_table_ref.table_id = 'audit'
        mock_table_ref_class.from_string.return_value = mock_table_ref

        util = GA4DataTransferAuditUtil()

        copy_results = [
            (
                'task_landing',
                {
                    'source_table': 's.t',
                    'target_table': 't.t',
                    'source_row_count': 1,
                    'target_row_count': 1,
                    'copy_status': 'SUCCESS',
                    'validation_status': 'SUCCESS',
                },
            ),
        ]

        with pytest.raises(AirflowException, match="Failed to insert audit record for task_id 'task_landing'"):
            util.log_changes_to_audit_table(
                audit_table_ref='proj.ds.audit',
                dag_run_id='run1',
                dag_id='dag1',
                copy_results=copy_results,
            )

    @patch('dags.switch_growth_processing.google_analytics_data_transfer.utils.audit_log.TableReference')
    @patch('dags.util.auditing_utils.audit_util.read_variable_or_file')
    @patch('dags.util.auditing_utils.audit_util.bigquery.Client')
    def test_log_changes_to_audit_table_combines_status_correctly(self, mock_client_class, mock_read_var, mock_table_ref_class):
        """Test that status is combined correctly for different scenarios."""
        mock_read_var.return_value = {DEPLOYMENT_ENVIRONMENT_NAME: 'dev'}
        mock_client = MagicMock()
        mock_client.query.return_value.result.return_value = None
        mock_client_class.return_value = mock_client
        mock_table_ref = MagicMock()
        mock_table_ref.dataset_id = 'ds'
        mock_table_ref.table_id = 'audit'
        mock_table_ref_class.from_string.return_value = mock_table_ref

        util = GA4DataTransferAuditUtil()

        copy_results = [
            ('task1', {
                'source_table': 's1', 'target_table': 't1',
                'source_row_count': 10, 'target_row_count': 10,
                'copy_status': 'FAILED', 'validation_status': 'SUCCESS'
            }),
            ('task2', {
                'source_table': 's2', 'target_table': 't2',
                'source_row_count': 10, 'target_row_count': 5,
                'copy_status': 'SUCCESS', 'validation_status': 'FAILED'
            }),
            ('task3', {
                'source_table': 's3', 'target_table': 't3',
                'source_row_count': 10, 'target_row_count': 10,
                'copy_status': 'SUCCESS', 'validation_status': 'SUCCESS'
            }),
        ]

        util.log_changes_to_audit_table(
            audit_table_ref='proj.ds.audit',
            dag_run_id='run1',
            dag_id='dag1',
            copy_results=copy_results,
        )

        # Verify all three records were inserted with correct combined statuses
        assert mock_client.query.call_count == 3

        # Check statuses: FAILED, VALIDATION_FAILED, SUCCESS
        calls = mock_client.query.call_args_list
        assert "'FAILED'" in calls[0][0][0]  # copy failed
        assert "'VALIDATION_FAILED'" in calls[1][0][0]  # copy succeeded, validation failed
        assert "'SUCCESS'" in calls[2][0][0]  # both succeeded
