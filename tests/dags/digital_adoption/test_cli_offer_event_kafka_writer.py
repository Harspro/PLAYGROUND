import pytest
from unittest.mock import patch, MagicMock
from airflow.models.dag import DAG
from dags.digital_adoption.cli_offer_event_kafka_writer import check_for_record_count
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from util.miscutils import read_file_env
import os


current_script_directory = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="class")
def mock_read_file_env():
    with patch('dags.digital_adoption.cli_offer_event_kafka_writer.read_file_env') as mocked:
        yield mocked


@pytest.fixture(scope="class")
def mock_run_bq_query():
    with patch('dags.digital_adoption.cli_offer_event_kafka_writer.run_bq_query') as mocked:
        yield mocked


@pytest.fixture(scope="class")
def mock_get_dagrun_last():
    with patch('dags.digital_adoption.cli_offer_event_kafka_writer.get_dagrun_last') as mocked:
        yield mocked


@pytest.fixture(scope="class")
def mock_save_job_to_control_table():
    with patch('dags.digital_adoption.cli_offer_event_kafka_writer.save_job_to_control_table') as mocked:
        yield mocked


@pytest.fixture(scope="class")
def mock_task_instance():
    """Mock task instance with xcom_push capability"""
    mock_ti = MagicMock()
    mock_ti.xcom_push = MagicMock()
    return mock_ti


@pytest.fixture(scope="class")
def mock_context(mock_task_instance):
    """Mock context dictionary with task instance"""
    return {'ti': mock_task_instance}


class TestCliOfferEventKafkaWriter:
    def test_check_for_record_count_with_records(self, mock_run_bq_query, mock_read_file_env):
        # Arrange
        mock_read_file_env.return_value = read_file_env(f"{current_script_directory}/sql/count_check_positive_scenario.sql", "test-deployment-environment")
        mock_query_result = MagicMock()
        mock_query_result.result.return_value = [{'record_count': 1}]
        mock_run_bq_query.return_value = mock_query_result

        # Act
        result = check_for_record_count(last_dag_run="2025-07-01")

        # Assert
        assert result == 'kafka_writer_task_for_cli'

    def test_check_for_record_count_no_records(self, mock_run_bq_query, mock_read_file_env):
        # Arrange
        mock_read_file_env.return_value = read_file_env(f"{current_script_directory}/sql/count_check_negative_scenario.sql", "test-deployment-environment")
        mock_query_result = MagicMock()
        mock_query_result.result.return_value = [{'record_count': 0}]
        mock_run_bq_query.return_value = mock_query_result

        # Act
        result = check_for_record_count(last_dag_run="2025-07-01")

        # Assert
        assert result == 'end'

    def test_get_last_dag_run_date_from_control_table_first_run(self, mock_get_dagrun_last, mock_context, mock_task_instance):
        from dags.digital_adoption.cli_offer_event_kafka_writer import get_last_dag_run_date_from_control_table

        mock_result = MagicMock()
        mock_result.total_rows = 0
        mock_get_dagrun_last.return_value = mock_result

        dag_id = 'cli_offer_event_kafka_writer'
        result = get_last_dag_run_date_from_control_table(dag_id, **mock_context)

        assert result == '2025-07-01'

    def test_get_last_dag_run_date_from_control_table_prior_run(self, mock_get_dagrun_last, mock_context, mock_task_instance):
        from dags.digital_adoption.cli_offer_event_kafka_writer import get_last_dag_run_date_from_control_table

        # Mock result with existing records
        mock_result = MagicMock()
        mock_result.total_rows = 1

        # Mock dataframe behavior - use real pandas DataFrame
        import pandas as pd
        job_params_json = '{"status": "SUCCESS", "dag_run_date": "2025-08-15 14:30:00 -0500"}'
        mock_df = pd.DataFrame({'job_params': [job_params_json]})
        mock_result.to_dataframe.return_value = mock_df
        mock_get_dagrun_last.return_value = mock_result

        dag_id = 'cli_offer_event_kafka_writer'
        result = get_last_dag_run_date_from_control_table(dag_id, **mock_context)

        assert result == '2025-08-15'

    def test_save_job_details_to_control_table(self, mock_save_job_to_control_table):
        from dags.digital_adoption.cli_offer_event_kafka_writer import save_job_details_to_control_table

        # Test data
        status = 'SUCCESS'
        context = {'dag_run': 'mock_dag_run', 'task_instance': 'mock_ti'}

        # Act - should not raise any exception
        save_job_details_to_control_table(status, **context)


def test_dag_structure():
    from dags.digital_adoption.cli_offer_event_kafka_writer import dag

    # Check DAG ID
    assert dag.dag_id == 'cli_offer_event_kafka_writer'

    # Expected tasks in the main path order
    expected_task_ids = [
        'start',
        'get_last_dag_run_date',
        'branch_on_record_existence',
        'kafka_writer_task_for_cli',
        'save_job_to_control_table',
        'end'
    ]

    # All expected tasks exist
    actual_task_ids = set(dag.task_dict.keys())
    assert set(expected_task_ids).issubset(actual_task_ids)

    # Verify linear dependencies along the main success path
    for current_id, next_id in zip(expected_task_ids, expected_task_ids[1:]):
        current_task = dag.get_task(current_id)
        downstream_ids = [t.task_id for t in current_task.downstream_list]
        assert next_id in downstream_ids

    # Verify branch also connects directly to control table save (no-records path)
    branch = dag.get_task('branch_on_record_existence')
    downstream_of_branch = [t.task_id for t in branch.downstream_list]
    assert 'kafka_writer_task_for_cli' in downstream_of_branch
    assert 'save_job_to_control_table' in downstream_of_branch


def test_task_types_and_configs():
    from dags.digital_adoption.cli_offer_event_kafka_writer import dag

    start = dag.get_task('start')
    end = dag.get_task('end')
    get_last = dag.get_task('get_last_dag_run_date')
    branch = dag.get_task('branch_on_record_existence')
    writer = dag.get_task('kafka_writer_task_for_cli')
    save_ctrl = dag.get_task('save_job_to_control_table')

    # Types
    assert isinstance(start, EmptyOperator)
    assert isinstance(end, EmptyOperator)
    assert isinstance(get_last, PythonOperator)
    assert isinstance(branch, BranchPythonOperator)
    assert isinstance(writer, TriggerDagRunOperator)
    assert isinstance(save_ctrl, PythonOperator)

    # Key configs
    assert end.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS

    assert get_last.python_callable.__name__ == 'get_last_dag_run_date_from_control_table'
    assert get_last.op_kwargs.get('dag_id') == dag.dag_id

    assert branch.python_callable.__name__ == 'check_for_record_count'
    assert 'last_dag_run' in branch.op_kwargs

    assert writer.trigger_dag_id == 'cli_offer_event_kafka_publish'
    assert writer.wait_for_completion is True
    assert isinstance(writer.conf, dict)
    assert 'replacements' in writer.conf
    assert '{last_dag_run_date}' in writer.conf['replacements']

    assert save_ctrl.python_callable.__name__ == 'save_job_details_to_control_table'
    assert save_ctrl.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    assert save_ctrl.op_kwargs.get('status') == 'SUCCESS'
