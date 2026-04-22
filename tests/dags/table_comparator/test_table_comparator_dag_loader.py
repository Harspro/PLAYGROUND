"""
Test cases for TableComparatorLoader class.
"""
import pytest
from unittest.mock import patch, MagicMock, call
import time

from table_comparator.table_comparator_dag_loader import TableComparatorLoader


@pytest.fixture
def mock_params():
    """Mock parameters for testing."""
    return {
        'source_table_fq': 'test-project.test_dataset.source_table',
        'target_table_fq': 'test-project.test_dataset.target_table',
        'pk_columns_list': ['id', 'date'],
        'filters': ['status = \'ACTIVE\'', 'amount > 1000'],
        'user_compare_cols_list': ['col1', 'col2'],
        'exclude_columns_list': ['exclude_col'],
        'mismatch_report_table_fq': 'test-project.test_dataset.mismatch_report',
        'column_expressions': {'col1': {'expressions': ['TRIM(column)']}},
        'global_expressions_by_type': {'string_types': ['COALESCE(column, \'\')']}
    }


@pytest.fixture
def mock_schemas():
    """Mock table schemas."""
    return (
        {
            'col1': {
                'type': 'STRING'}, 'col2': {
                'type': 'INTEGER'}, 'id': {
                    'type': 'STRING'}}, {
                        'col1': {
                            'type': 'STRING'}, 'col2': {
                                'type': 'INTEGER'}, 'id': {
                                    'type': 'STRING'}})


@pytest.fixture
def mock_ti():
    """Mock task instance."""
    ti = MagicMock()
    ti.xcom_push = MagicMock()
    return ti


@pytest.fixture
def mock_read_yaml():
    return {'test_dag': {}}


@pytest.fixture
def mock_read_variable():
    return {
        'deployment_environment_name': 'test',
        'deploy_env_storage_suffix': 'test'}


class TestTableComparatorLoader:
    """Test cases for TableComparatorLoader class."""

    def test_init(self, mock_read_yaml, mock_read_variable):
        """Test TableComparatorLoader initialization."""

        loader = TableComparatorLoader('test_config.yaml')
        assert isinstance(loader, TableComparatorLoader)

    # @patch('table_comparator.table_comparator_dag_loader.validate_input_parameters')
    # @patch('table_comparator.table_comparator_dag_loader.get_table_schemas')
    # @patch('table_comparator.table_comparator_dag_loader.determine_comparison_columns')
    # @patch('table_comparator.table_comparator_dag_loader.generate_sql_aliases')
    # @patch('table_comparator.table_comparator_dag_loader.generate_filter_clauses')
    # def test_generate_comparison_queries(
    #         self,
    #         mock_filter_clauses,
    #         mock_aliases,
    #         mock_columns,
    #         mock_schemas,
    #         mock_validate,
    #         mock_read_yaml,
    #         mock_read_variable,
    #         mock_params,
    #         mock_ti):
    #     """Test generate_comparison_queries method."""
    #
    #     # mock_schemas.return_value = mock_schemas_data
    #     mock_columns.return_value = ['col1', 'col2']
    #     mock_aliases.return_value = {'source': {}, 'target': {}}
    #     mock_filter_clauses.return_value = ('WHERE 1=1', 'WHERE 1=1')
    #
    #     loader = TableComparatorLoader('test_config.yaml')
    #
    #     with patch.object(loader, 'push_mismatch_report_query') as mock_push_mismatch, \
    #             patch.object(loader, 'push_count_queries') as mock_push_count, \
    #             patch.object(loader, 'push_schema_mismatch_query') as mock_push_schema:
    #
    #         kwargs = {
    #             'source_table_name': mock_params['source_table_fq'],
    #             'target_table_name': mock_params['target_table_fq'],
    #             'primary_key_columns': mock_params['pk_columns_list'],
    #             'record_load_date_column': mock_params['load_date_col'],
    #             'specific_load_dates': mock_params['specific_dates_list'],
    #             'comparison_columns': mock_params['user_compare_cols_list'],
    #             'exclude_columns': mock_params['exclude_columns_list'],
    #             'mismatch_report_table_name': mock_params['mismatch_report_table_fq'],
    #             'column_expressions': mock_params['column_expressions'],
    #             'global_expressions_by_type': mock_params['global_expressions_by_type'],
    #             'ti': mock_ti}
    #
    #         loader.generate_comparison_queries(**kwargs)
    #
    #         mock_validate.assert_called_once()
    #         mock_columns.assert_called_once()
    #         mock_aliases.assert_called_once()
    #         mock_filter_clauses.assert_called_once()
    #         mock_push_mismatch.assert_called_once()
    #         mock_push_count.assert_called_once()
    #         mock_push_schema.assert_called_once()

    def test_extract_params(self):
        """Test extract_params method."""
        loader = TableComparatorLoader('test_config.yaml')

        kwargs = {
            'source_table_name': 'source.table',
            'target_table_name': 'target.table',
            'primary_key_columns': ['id'],
            'filters': ['status = \'ACTIVE\'', 'amount > 1000'],
            'comparison_columns': ['col1'],
            'exclude_columns': ['exclude_col'],
            'mismatch_report_table_name': 'report.table',
            'column_expressions': {
                'col1': {
                    'expressions': ['TRIM(column)']}},
            'global_expressions_by_type': {
                'string_types': ['COALESCE(column, \'\')']}}

        params = loader.extract_params(kwargs)

        assert params['source_table_fq'] == 'source.table'
        assert params['target_table_fq'] == 'target.table'
        assert params['pk_columns_list'] == ['id']
        assert params['filters'] == ['status = \'ACTIVE\'', 'amount > 1000']
        assert params['user_compare_cols_list'] == ['col1']
        assert params['exclude_columns_list'] == ['exclude_col']
        assert params['mismatch_report_table_fq'] == 'report.table'
        assert params['column_expressions'] == {
            'col1': {'expressions': ['TRIM(column)']}}
        assert params['global_expressions_by_type'] == {
            'string_types': ['COALESCE(column, \'\')']}

    @patch('table_comparator.table_comparator_dag_loader.validate_input_parameters')
    def test_validate_inputs(
            self,
            mock_validate,
            mock_read_yaml,
            mock_read_variable):
        """Test validate_inputs method."""

        loader = TableComparatorLoader('test_config.yaml')
        params = {
            'source_table_fq': 'source.table',
            'target_table_fq': 'target.table',
            'pk_columns_list': ['id']
        }

        loader.validate_inputs(params)
        mock_validate.assert_called_once_with(
            source_table_fq='source.table',
            target_table_fq='target.table',
            pk_columns_list=['id']
        )

    @patch('table_comparator.table_comparator_dag_loader.determine_comparison_columns')
    def test_determine_final_compare_cols(
            self,
            mock_determine,
            mock_read_yaml,
            mock_read_variable):
        """Test determine_final_compare_cols method."""

        mock_determine.return_value = ['col1', 'col2']

        loader = TableComparatorLoader('test_config.yaml')
        params = {
            'user_compare_cols_list': ['col1'],
            'source_cols_schema': {'col1': MagicMock(field_type='STRING')},
            'target_cols_schema': {'col1': MagicMock(field_type='STRING')},
            'pk_columns_list': ['id'],
            'exclude_columns_list': ['exclude_col']
        }

        result = loader.determine_final_compare_cols(params)
        assert result == ['col1', 'col2']
        # Verify the mock_determine was called with the correct parameters
        mock_determine.assert_called_once()
        call_args = mock_determine.call_args
        assert call_args[1]['user_compare_cols_list'] == ['col1']
        assert call_args[1]['pk_columns_list'] == ['id']
        assert call_args[1]['exclude_columns_list'] == ['exclude_col']
        # Verify the schema objects have the expected structure
        source_schema = call_args[1]['source_cols_schema']
        target_schema = call_args[1]['target_cols_schema']
        assert 'col1' in source_schema and hasattr(
            source_schema['col1'], 'field_type')
        assert 'col1' in target_schema and hasattr(
            target_schema['col1'], 'field_type')

    @patch('table_comparator.table_comparator_dag_loader.generate_sql_aliases')
    def test_generate_sql_aliases(
            self,
            mock_generate,
            mock_read_yaml,
            mock_read_variable):
        """Test generate_sql_aliases method."""
        mock_generate.return_value = {'source': {}, 'target': {}}

        loader = TableComparatorLoader('test_config.yaml')
        params = {
            'pk_columns_list': ['id'],
            'final_compare_cols_list': ['col1'],
            'column_expressions': {'col1': {'expressions': ['TRIM(column)']}},
            'source_cols_schema': {'col1': MagicMock(field_type='STRING')},
            'target_cols_schema': {'col1': MagicMock(field_type='STRING')},
            'global_expressions_by_type': {'string_types': ['COALESCE(column, \'\')']}
        }

        result = loader.generate_sql_aliases(params)
        assert result == {'source': {}, 'target': {}}
        # Verify the mock_generate was called with the correct parameters
        mock_generate.assert_called_once()
        call_args = mock_generate.call_args
        assert call_args[1]['pk_columns_list'] == ['id']
        assert call_args[1]['final_compare_cols_list'] == ['col1']
        assert call_args[1]['column_expressions'] == {
            'col1': {'expressions': ['TRIM(column)']}}
        assert call_args[1]['global_expressions_by_type'] == {
            'string_types': ['COALESCE(column, \'\')']}
        # Verify the schema objects have the expected structure
        source_schema = call_args[1]['source_cols_schema']
        target_schema = call_args[1]['target_cols_schema']
        assert 'col1' in source_schema and hasattr(
            source_schema['col1'], 'field_type')
        assert 'col1' in target_schema and hasattr(
            target_schema['col1'], 'field_type')

    @patch('table_comparator.table_comparator_dag_loader.generate_filter_clauses')
    def test_generate_filter_clauses(
            self,
            mock_generate,
            mock_read_yaml,
            mock_read_variable):
        """Test generate_filter_clauses method."""
        mock_generate.return_value = (
            'WHERE status = \'ACTIVE\'',
            'WHERE status = \'ACTIVE\'')

        loader = TableComparatorLoader('test_config.yaml')
        params = {
            'filters': ['status = \'ACTIVE\''],
            'source_cols_schema': {'status': {'type': 'STRING'}},
            'target_cols_schema': {'status': {'type': 'STRING'}}
        }

        result = loader.generate_filter_clauses(params)
        assert result == (
            'WHERE status = \'ACTIVE\'',
            'WHERE status = \'ACTIVE\'')
        mock_generate.assert_called_once_with(
            filters=['status = \'ACTIVE\''],
            source_cols_schema={'status': {'type': 'STRING'}},
            target_cols_schema={'status': {'type': 'STRING'}}
        )

    def test_generate_filter_clauses_multiple_filters(
            self,
            mock_read_yaml,
            mock_read_variable):
        """Test generate_filter_clauses method with multiple filters."""
        with patch('table_comparator.table_comparator_dag_loader.generate_filter_clauses') as mock_generate:
            mock_generate.return_value = (
                'WHERE status = \'ACTIVE\' AND amount > 1000',
                'WHERE status = \'ACTIVE\' AND amount > 1000'
            )

            loader = TableComparatorLoader('test_config.yaml')
            params = {
                'filters': [
                    'status = \'ACTIVE\'', 'amount > 1000'], 'source_cols_schema': {
                    'status': {
                        'type': 'STRING'}, 'amount': {
                        'type': 'NUMERIC'}}, 'target_cols_schema': {
                        'status': {
                            'type': 'STRING'}, 'amount': {
                                'type': 'NUMERIC'}}}

            result = loader.generate_filter_clauses(params)
            assert result == (
                'WHERE status = \'ACTIVE\' AND amount > 1000',
                'WHERE status = \'ACTIVE\' AND amount > 1000'
            )
            mock_generate.assert_called_once_with(
                filters=[
                    'status = \'ACTIVE\'', 'amount > 1000'], source_cols_schema={
                    'status': {
                        'type': 'STRING'}, 'amount': {
                        'type': 'NUMERIC'}}, target_cols_schema={
                        'status': {
                            'type': 'STRING'}, 'amount': {
                                'type': 'NUMERIC'}})

    def test_generate_filter_clauses_no_filters(
            self,
            mock_read_yaml,
            mock_read_variable):
        """Test generate_filter_clauses method with no filters."""
        with patch('table_comparator.table_comparator_dag_loader.generate_filter_clauses') as mock_generate:
            mock_generate.return_value = ('', '')

            loader = TableComparatorLoader('test_config.yaml')
            params = {
                'filters': None,
                'source_cols_schema': {'status': {'type': 'STRING'}},
                'target_cols_schema': {'status': {'type': 'STRING'}}
            }

            result = loader.generate_filter_clauses(params)
            assert result == ('', '')
            mock_generate.assert_called_once_with(
                filters=None,
                source_cols_schema={'status': {'type': 'STRING'}},
                target_cols_schema={'status': {'type': 'STRING'}}
            )

    def test_generate_filter_clauses_complex_conditions(
            self,
            mock_read_yaml,
            mock_read_variable):
        """Test generate_filter_clauses method with complex filter conditions."""
        with patch('table_comparator.table_comparator_dag_loader.generate_filter_clauses') as mock_generate:
            complex_filter = 'WHERE status IN (\'ACTIVE\', \'PENDING\') AND amount BETWEEN 100 AND 10000 AND created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)'
            mock_generate.return_value = (complex_filter, complex_filter)

            loader = TableComparatorLoader('test_config.yaml')
            params = {
                'filters': [
                    'status IN (\'ACTIVE\', \'PENDING\')',
                    'amount BETWEEN 100 AND 10000',
                    'created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)'
                ],
                'source_cols_schema': {
                    'status': {'type': 'STRING'},
                    'amount': {'type': 'NUMERIC'},
                    'created_date': {'type': 'DATE'}
                },
                'target_cols_schema': {
                    'status': {'type': 'STRING'},
                    'amount': {'type': 'NUMERIC'},
                    'created_date': {'type': 'DATE'}
                }
            }

            result = loader.generate_filter_clauses(params)
            assert result == (complex_filter, complex_filter)
            mock_generate.assert_called_once_with(
                filters=[
                    'status IN (\'ACTIVE\', \'PENDING\')',
                    'amount BETWEEN 100 AND 10000',
                    'created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)'
                ],
                source_cols_schema={
                    'status': {'type': 'STRING'},
                    'amount': {'type': 'NUMERIC'},
                    'created_date': {'type': 'DATE'}
                },
                target_cols_schema={
                    'status': {'type': 'STRING'},
                    'amount': {'type': 'NUMERIC'},
                    'created_date': {'type': 'DATE'}
                }
            )

    def test_generate_filter_clauses_date_based_filter(
            self,
            mock_read_yaml,
            mock_read_variable):
        """Test generate_filter_clauses method with date-based filter (replacing old date filtering)."""
        with patch('table_comparator.table_comparator_dag_loader.generate_filter_clauses') as mock_generate:
            date_filter = 'WHERE created_date = CURRENT_DATE()'
            mock_generate.return_value = (date_filter, date_filter)

            loader = TableComparatorLoader('test_config.yaml')
            params = {
                'filters': ['created_date = CURRENT_DATE()'],
                'source_cols_schema': {'created_date': {'type': 'DATE'}},
                'target_cols_schema': {'created_date': {'type': 'DATE'}}
            }

            result = loader.generate_filter_clauses(params)
            assert result == (date_filter, date_filter)
            mock_generate.assert_called_once_with(
                filters=['created_date = CURRENT_DATE()'],
                source_cols_schema={'created_date': {'type': 'DATE'}},
                target_cols_schema={'created_date': {'type': 'DATE'}}
            )

    @patch('table_comparator.table_comparator_dag_loader.read_sql_file')
    def test_push_mismatch_report_query_with_table(
            self, mock_read_sql, mock_read_yaml, mock_read_variable, mock_ti):
        """Test push_mismatch_report_query method with mismatch report table."""
        mock_read_sql.return_value = "CREATE TABLE test.report.table AS SELECT * FROM source.table"

        loader = TableComparatorLoader('test_config.yaml')
        params = {
            'mismatch_report_table_fq': 'test.report.table',
            'sql_aliases': {
                'pk_cols_s_aliased': ['s.`id` AS s_pk_0'],
                'comp_cols_s_aliased': ['s.`col1` AS s_cmp_col1'],
                'original_cols_s_aliased': ['s.`col1` AS s_orig_col1'],
                'pk_cols_t_aliased': ['t.`id` AS t_pk_0'],
                'comp_cols_t_aliased': ['t.`col1` AS t_cmp_col1'],
                'original_cols_t_aliased': ['t.`col1` AS t_orig_col1'],
                'pk_select_coalesce': 'COALESCE(s.s_pk_0, t.t_pk_0) AS `id`',
                'first_pk_s_alias': 's_pk_0',
                'first_pk_t_alias': 't_pk_0',
                'mismatch_predicate_sql': 's.s_cmp_col1 != t.t_cmp_col1',
                'diff_details_structs_sql': 'ARRAY(...)',
                'join_on_aliased': 's.s_pk_0 = t.t_pk_0'
            },
            'pk_columns_list': ['id'],
            'source_table_fq': 'source.table',
            'target_table_fq': 'target.table',
            'source_filter_clause_str': 'WHERE 1=1',
            'target_filter_clause_str': 'WHERE 1=1'
        }

        loader.push_mismatch_report_query(mock_ti, params)

        mock_ti.xcom_push.assert_called_once_with(
            key='mismatch_query', value=mock_read_sql.return_value)

    def test_push_mismatch_report_query_without_table(
            self, mock_read_yaml, mock_read_variable, mock_ti):
        """Test push_mismatch_report_query method without mismatch report table."""

        loader = TableComparatorLoader('test_config.yaml')
        params = {
            'mismatch_report_table_fq': None,
            'sql_aliases': {}
        }

        loader.push_mismatch_report_query(mock_ti, params)

        mock_ti.xcom_push.assert_called_once_with(
            key='mismatch_query', value="")

    @patch('table_comparator.table_comparator_dag_loader.generate_count_queries')
    def test_push_count_queries(
            self,
            mock_generate_count,
            mock_read_yaml,
            mock_read_variable,
            mock_ti):
        """Test push_count_queries method."""
        mock_generate_count.return_value = {
            'count_mismatched_query': 'SELECT COUNT(*) FROM mismatched',
            'count_missing_in_source_query': 'SELECT COUNT(*) FROM missing_source',
            'count_missing_in_target_query': 'SELECT COUNT(*) FROM missing_target'}

        loader = TableComparatorLoader('test_config.yaml')
        params = {
            'mismatch_report_table_fq': 'test.report.table',
            'final_compare_cols_list': ['col1'],
            'pk_columns_list': ['id'],
            'source_table_fq': 'source.table',
            'target_table_fq': 'target.table',
            'source_filter_clause_str': 'WHERE 1=1',
            'target_filter_clause_str': 'WHERE 1=1',
            'sql_aliases': {'mismatch_predicate_sql': 's.col1 != t.col1'}
        }

        loader.push_count_queries(mock_ti, params)

        assert mock_ti.xcom_push.call_count == 3
        mock_generate_count.assert_called_once()

    @patch('table_comparator.table_comparator_dag_loader.generate_schema_mismatch_query')
    def test_push_schema_mismatch_query(
            self,
            mock_generate_schema,
            mock_read_yaml,
            mock_read_variable,
            mock_ti):
        """Test push_schema_mismatch_query method."""
        mock_generate_schema.return_value = 'SELECT * FROM schema_mismatches'

        loader = TableComparatorLoader('test_config.yaml')
        params = {
            'source_table_fq': 'source.table',
            'target_table_fq': 'target.table'
        }

        loader.push_schema_mismatch_query(mock_ti, params)

        mock_ti.xcom_push.assert_called_once_with(
            key='schema_mismatch_query',
            value='SELECT * FROM schema_mismatches')
        mock_generate_schema.assert_called_once_with(
            source_table_fq='source.table',
            target_table_fq='target.table'
        )

    def test_report_results(self, mock_read_yaml, mock_read_variable, mock_ti):
        """Test report_results method."""

        loader = TableComparatorLoader('test_config.yaml')

        with patch.object(loader, 'get_xcom_count') as mock_get_count, \
                patch.object(loader, 'get_xcom_schema_mismatches') as mock_get_schema, \
                patch.object(loader, 'log_comparison_header') as mock_log_header, \
                patch.object(loader, 'log_expressions_info') as mock_log_expressions, \
                patch.object(loader, 'log_counts') as mock_log_counts, \
                patch.object(loader, 'log_schema_mismatches') as mock_log_schema, \
                patch.object(loader, 'log_mismatch_report_table') as mock_log_report, \
                patch.object(loader, 'log_performance_summary') as mock_log_performance, \
                patch.object(loader, 'log_summary_stats') as mock_log_summary:

            mock_get_count.side_effect = [10, 5, 3]
            mock_get_schema.return_value = []

            kwargs = {
                'source_table_name': 'source.table',
                'target_table_name': 'target.table',
                'mismatch_report_table_name': 'report.table',
                'ti': mock_ti
            }

            loader.report_results(**kwargs)

            mock_log_header.assert_called_once()
            mock_log_expressions.assert_called_once()
            mock_log_counts.assert_called_once_with(10, 5, 3)
            mock_log_schema.assert_called_once_with([])
            mock_log_report.assert_called_once()
            mock_log_performance.assert_called_once()
            mock_log_summary.assert_called_once_with(10, 5, 3)

    def test_get_xcom_count_success(
            self,
            mock_read_yaml,
            mock_read_variable,
            mock_ti):
        """Test get_xcom_count method with successful retrieval."""
        mock_ti.xcom_pull.return_value = [[42]]

        loader = TableComparatorLoader('test_config.yaml')

        result = loader.get_xcom_count(
            mock_ti, 'test_task', 'count_key', 'Test Count')
        assert result == 42
        mock_ti.xcom_pull.assert_called_once_with(
            task_ids='test_task', key='count_key')

    def test_get_xcom_count_exception(
            self,
            mock_read_yaml,
            mock_read_variable,
            mock_ti):
        """Test get_xcom_count method with exception."""
        mock_ti.xcom_pull.side_effect = Exception("XCom error")

        loader = TableComparatorLoader('test_config.yaml')

        result = loader.get_xcom_count(
            mock_ti, 'test_task', 'count_key', 'Test Count')
        assert result == 0

    def test_get_xcom_schema_mismatches_success(
            self, mock_read_yaml, mock_read_variable, mock_ti):
        """Test get_xcom_schema_mismatches method with successful retrieval."""
        mock_ti.xcom_pull.return_value = [
            {'column': 'test_col', 'source_type': 'STRING', 'target_type': 'INTEGER'}]

        loader = TableComparatorLoader('test_config.yaml')

        result = loader.get_xcom_schema_mismatches(
            mock_ti, 'test_task', 'schema_key')
        assert result == [{'column': 'test_col',
                           'source_type': 'STRING', 'target_type': 'INTEGER'}]
        mock_ti.xcom_pull.assert_called_once_with(
            task_ids='test_task', key='schema_key')

    def test_get_xcom_schema_mismatches_exception(
            self, mock_read_yaml, mock_read_variable, mock_ti):
        """Test get_xcom_schema_mismatches method with exception."""
        mock_ti.xcom_pull.side_effect = Exception("XCom error")

        loader = TableComparatorLoader('test_config.yaml')

        result = loader.get_xcom_schema_mismatches(
            mock_ti, 'test_task', 'schema_key')
        assert result is None

    def test_log_comparison_header(self, mock_read_yaml, mock_read_variable):
        """Test log_comparison_header method."""

        loader = TableComparatorLoader('test_config.yaml')
        kwargs = {
            'source_table_name': 'source.table',
            'target_table_name': 'target.table',
            'primary_key_columns': ['id', 'date'],
            'exclude_columns': ['exclude_col']
        }

        # This method logs info, so we just test it doesn't raise an exception
        loader.log_comparison_header(kwargs)

    def test_log_expressions_info(self, mock_read_yaml, mock_read_variable):
        """Test log_expressions_info method."""

        loader = TableComparatorLoader('test_config.yaml')
        kwargs = {
            'global_expressions_by_type': {
                'string_types': ['COALESCE(column, \'\')']},
            'column_expressions': {
                'col1': {
                    'expressions': ['TRIM(column)'],
                    'description': 'Test'}},
            'filters': ['status = \'ACTIVE\'', 'amount > 1000']}

        # This method logs info, so we just test it doesn't raise an exception
        loader.log_expressions_info(kwargs)

    def test_log_expressions_info_no_filters(
            self, mock_read_yaml, mock_read_variable):
        """Test log_expressions_info method with no filters."""

        loader = TableComparatorLoader('test_config.yaml')
        kwargs = {
            'global_expressions_by_type': {
                'string_types': ['COALESCE(column, \'\')']},
            'column_expressions': {
                'col1': {
                    'expressions': ['TRIM(column)'],
                    'description': 'Test'}}}

        # This method logs info, so we just test it doesn't raise an exception
        loader.log_expressions_info(kwargs)

    def test_log_counts(self, mock_read_yaml, mock_read_variable):
        """Test log_counts method."""

        loader = TableComparatorLoader('test_config.yaml')

        # This method logs info, so we just test it doesn't raise an exception
        loader.log_counts(10, 5, 3)

    def test_log_schema_mismatches_with_results(
            self, mock_read_yaml, mock_read_variable):
        """Test log_schema_mismatches method with results."""

        loader = TableComparatorLoader('test_config.yaml')
        schema_mismatches = [['col1', 'STRING', 'source',
                              'INTEGER', 'target', 'type_mismatch']]

        # This method logs info, so we just test it doesn't raise an exception
        loader.log_schema_mismatches(schema_mismatches)

    def test_log_schema_mismatches_no_results(
            self, mock_read_yaml, mock_read_variable):
        """Test log_schema_mismatches method without results."""

        loader = TableComparatorLoader('test_config.yaml')

        # This method logs info, so we just test it doesn't raise an exception
        loader.log_schema_mismatches(None)

    def test_log_mismatch_report_table(
            self, mock_read_yaml, mock_read_variable):
        """Test log_mismatch_report_table method."""

        loader = TableComparatorLoader('test_config.yaml')
        kwargs = {'mismatch_report_table_name': 'test.report.table'}

        # This method logs info, so we just test it doesn't raise an exception
        loader.log_mismatch_report_table(kwargs)

    def test_log_performance_summary(self, mock_read_yaml, mock_read_variable):
        """Test log_performance_summary method."""

        loader = TableComparatorLoader('test_config.yaml')

        # This method logs info, so we just test it doesn't raise an exception
        loader.log_performance_summary(time.time())

    def test_log_summary_stats(self, mock_read_yaml, mock_read_variable):
        """Test log_summary_stats method."""

        loader = TableComparatorLoader('test_config.yaml')

        # This method logs info, so we just test it doesn't raise an exception
        loader.log_summary_stats(10, 5, 3)

    def test_complete_filtering_workflow(
            self,
            mock_read_yaml,
            mock_read_variable,
            mock_ti):
        """Test complete filtering workflow with the new flexible filtering feature."""

        with patch('table_comparator.table_comparator_dag_loader.validate_input_parameters') as mock_validate, \
                patch('table_comparator.table_comparator_dag_loader.get_table_schemas') as mock_schemas, \
                patch('table_comparator.table_comparator_dag_loader.determine_comparison_columns') as mock_columns, \
                patch('table_comparator.table_comparator_dag_loader.generate_sql_aliases') as mock_aliases, \
                patch('table_comparator.table_comparator_dag_loader.generate_filter_clauses') as mock_filter_clauses, \
                patch('table_comparator.table_comparator_dag_loader.read_sql_file') as mock_read_sql, \
                patch('table_comparator.table_comparator_dag_loader.generate_count_queries') as mock_count_queries, \
                patch('table_comparator.table_comparator_dag_loader.generate_schema_mismatch_query') as mock_schema_query:

            # Mock return values - create mock SchemaField objects with
            # field_type attribute
            mock_source_schema = {
                'id': MagicMock(field_type='STRING'),
                'status': MagicMock(field_type='STRING'),
                'amount': MagicMock(field_type='NUMERIC')
            }
            mock_target_schema = {
                'id': MagicMock(field_type='STRING'),
                'status': MagicMock(field_type='STRING'),
                'amount': MagicMock(field_type='NUMERIC')
            }
            mock_schemas.return_value = (
                mock_source_schema, mock_target_schema)
            mock_columns.return_value = ['status', 'amount']
            mock_aliases.return_value = {
                'pk_cols_s_aliased': ['s.`id` AS s_pk_0'],
                'comp_cols_s_aliased': [
                    's.`status` AS s_cmp_status',
                    's.`amount` AS s_cmp_amount'],
                'pk_cols_t_aliased': ['t.`id` AS t_pk_0'],
                'comp_cols_t_aliased': [
                    't.`status` AS t_cmp_status',
                    't.`amount` AS t_cmp_amount'],
                'pk_select_coalesce': 'COALESCE(s.s_pk_0, t.t_pk_0) AS `id`',
                'first_pk_s_alias': 's_pk_0',
                'first_pk_t_alias': 't_pk_0',
                'mismatch_predicate_sql': 's.s_cmp_status != t.t_cmp_status OR s.s_cmp_amount != t.t_cmp_amount',
                'diff_details_structs_sql': 'ARRAY(...)',
                'join_on_aliased': 's.s_pk_0 = t.t_pk_0'}
            mock_filter_clauses.return_value = (
                'WHERE status = \'ACTIVE\' AND amount > 1000',
                'WHERE status = \'ACTIVE\' AND amount > 1000'
            )
            mock_read_sql.return_value = 'CREATE TABLE test.report.table AS SELECT * FROM source.table'
            mock_count_queries.return_value = {
                'count_mismatched_query': 'SELECT COUNT(*) FROM mismatched',
                'count_missing_in_source_query': 'SELECT COUNT(*) FROM missing_source',
                'count_missing_in_target_query': 'SELECT COUNT(*) FROM missing_target'}
            mock_schema_query.return_value = 'SELECT * FROM schema_mismatches'

            loader = TableComparatorLoader('test_config.yaml')

            # Test parameters with new filtering feature
            kwargs = {
                'source_table_name': 'test-project.dataset.source_table',
                'target_table_name': 'test-project.dataset.target_table',
                'primary_key_columns': ['id'],
                'filters': [
                    'status = \'ACTIVE\'',
                    'amount > 1000'],
                'comparison_columns': [
                    'status',
                    'amount'],
                'exclude_columns': ['audit_timestamp'],
                'mismatch_report_table_name': 'test-project.dataset.mismatch_report',
                'column_expressions': {
                    'amount': {
                        'expressions': ['ROUND(column, 2)']}},
                'global_expressions_by_type': {
                    'string_types': ['COALESCE(column, \'\')']},
                'ti': mock_ti}

            # Execute the method
            loader.generate_comparison_queries(**kwargs)

            # Verify all the expected calls were made
            mock_validate.assert_called_once()
            mock_schemas.assert_called_once()
            mock_columns.assert_called_once()
            mock_aliases.assert_called_once()
            # Verify the mock_filter_clauses was called with the correct schema
            # objects
            mock_filter_clauses.assert_called_once()
            # Get the actual arguments passed to mock_filter_clauses
            call_args = mock_filter_clauses.call_args
            assert call_args[1]['filters'] == [
                'status = \'ACTIVE\'', 'amount > 1000']
            # Verify the schema objects have the expected structure
            source_schema = call_args[1]['source_cols_schema']
            target_schema = call_args[1]['target_cols_schema']
            assert 'id' in source_schema and hasattr(
                source_schema['id'], 'field_type')
            assert 'status' in source_schema and hasattr(
                source_schema['status'], 'field_type')
            assert 'amount' in source_schema and hasattr(
                source_schema['amount'], 'field_type')
            assert 'id' in target_schema and hasattr(
                target_schema['id'], 'field_type')
            assert 'status' in target_schema and hasattr(
                target_schema['status'], 'field_type')
            assert 'amount' in target_schema and hasattr(
                target_schema['amount'], 'field_type')
            # Verify read_sql_file was called 3 times with the expected SQL files
            assert mock_read_sql.call_count == 3
            expected_calls = [
                call('create_mismatch_report_table_schema.sql'),
                call('create_mismatch_report.sql'),
                call('create_mismatch_report_view.sql')
            ]
            mock_read_sql.assert_has_calls(expected_calls, any_order=True)
            mock_count_queries.assert_called_once()
            mock_schema_query.assert_called_once()

            # Verify XCom pushes
            # mismatch_query, count queries, schema query
            assert mock_ti.xcom_push.call_count >= 4
