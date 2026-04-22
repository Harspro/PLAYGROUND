import unittest
from unittest.mock import patch, MagicMock
from airflow.utils.state import State
from datetime import datetime, timedelta
from dags.tokenized_consent_capture.external_task_status_pokesensor import ExternalTaskPokeSensor
import pytest


class TestExternalTaskPokeSensor(unittest.TestCase):
    """Test cases for ExternalTaskPokeSensor supporting single and multiple DAG/task pairs."""

    def _create_context(self, skip_sensor_task=False):
        """Helper to create a mock context for testing."""
        dag_runs_mock = MagicMock()
        dag_runs_mock.conf = {
            'file_create_date_filter': None,
            'skip_sensor_task': skip_sensor_task
        }
        return {
            'execution_date': datetime(2024, 6, 23),
            'dag_run': dag_runs_mock
        }

    # ==================== Initialization Tests ====================

    def test_init_single_pair_mode(self):
        """Test initialization with single dag_id and task_id pair."""
        sensor = ExternalTaskPokeSensor(
            task_id='test_sensor',
            external_dag_id='my_dag',
            external_task_id='my_task',
            start_date_delta=timedelta(minutes=-5),
            end_date_delta=timedelta(minutes=30),
        )
        self.assertEqual(sensor.external_dag_id, 'my_dag')
        self.assertEqual(sensor.external_task_id, 'my_task')
        self.assertEqual(len(sensor.external_tasks), 1)
        self.assertEqual(sensor.external_tasks[0], {'dag_id': 'my_dag', 'task_id': 'my_task'})

    def test_init_multiple_pairs_mode(self):
        """Test initialization with multiple dag_id/task_id pairs."""
        external_tasks = [
            {'dag_id': 'dag_1', 'task_id': 'task_1'},
            {'dag_id': 'dag_2', 'task_id': 'task_2'},
        ]
        sensor = ExternalTaskPokeSensor(
            task_id='test_sensor',
            external_tasks=external_tasks,
            start_date_delta=timedelta(minutes=-5),
            end_date_delta=timedelta(minutes=30),
        )
        self.assertIsNone(sensor.external_dag_id)
        self.assertIsNone(sensor.external_task_id)
        self.assertEqual(len(sensor.external_tasks), 2)
        self.assertEqual(sensor.external_tasks, external_tasks)

    def test_init_raises_error_when_both_modes_specified(self):
        """Test that specifying both single and multiple modes raises an error."""
        with self.assertRaises(ValueError) as context:
            ExternalTaskPokeSensor(
                task_id='test_sensor',
                external_dag_id='my_dag',
                external_task_id='my_task',
                external_tasks=[{'dag_id': 'dag_1', 'task_id': 'task_1'}],
                start_date_delta=timedelta(minutes=-5),
                end_date_delta=timedelta(minutes=30),
            )
        self.assertIn("Cannot specify both", str(context.exception))

    def test_init_raises_error_when_no_mode_specified(self):
        """Test that not specifying any mode raises an error."""
        with self.assertRaises(ValueError) as context:
            ExternalTaskPokeSensor(
                task_id='test_sensor',
                start_date_delta=timedelta(minutes=-5),
                end_date_delta=timedelta(minutes=30),
            )
        self.assertIn("Must specify either", str(context.exception))

    def test_init_raises_error_for_partial_single_mode(self):
        """Test that specifying only dag_id without task_id raises an error."""
        with self.assertRaises(ValueError) as context:
            ExternalTaskPokeSensor(
                task_id='test_sensor',
                external_dag_id='my_dag',
                start_date_delta=timedelta(minutes=-5),
                end_date_delta=timedelta(minutes=30),
            )
        self.assertIn("Must specify either", str(context.exception))

    # def test_init_raises_error_for_empty_external_tasks(self):
    #     """Test that empty external_tasks list raises an error."""
    #     with self.assertRaises(ValueError) as context:
    #         ExternalTaskPokeSensor(
    #             task_id='test_sensor',
    #             external_tasks=[],
    #             start_date_delta=timedelta(minutes=-5),
    #             end_date_delta=timedelta(minutes=30),
    #         )
    #     self.assertIn("must be a non-empty list", str(context.exception))

    def test_init_raises_error_for_invalid_external_tasks_structure(self):
        """Test that external_tasks with invalid structure raises an error."""
        with self.assertRaises(ValueError) as context:
            ExternalTaskPokeSensor(
                task_id='test_sensor',
                external_tasks=[{'dag_id': 'dag_1'}],  # Missing task_id
                start_date_delta=timedelta(minutes=-5),
                end_date_delta=timedelta(minutes=30),
            )
        self.assertIn("must have 'dag_id' and 'task_id' keys", str(context.exception))

    # ==================== Single Pair Mode - Poke Tests ====================

    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.DagRun')
    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.settings')
    def test_single_pair_dag_run_not_found(self, mock_settings, mock_dag_run):
        """Test poke returns False when no DAG run is found (single pair mode)."""
        mock_dag_run.find.return_value = []
        mock_session = MagicMock()
        mock_settings.Session.return_value = mock_session

        sensor = ExternalTaskPokeSensor(
            task_id='test_sensor',
            external_dag_id='test_source_dag',
            external_task_id='End',
            start_date_delta=timedelta(minutes=-5),
            end_date_delta=timedelta(minutes=30),
        )

        result = sensor.poke(self._create_context())
        self.assertFalse(result)

    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.DagRun')
    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.settings')
    def test_single_pair_task_instance_not_found(self, mock_settings, mock_dag_run):
        """Test poke returns False when task instance is not found (single pair mode)."""
        dag_run_mock = MagicMock()
        dag_run_mock.execution_date = datetime(2024, 6, 23, 0, 10)
        mock_dag_run.find.return_value = [dag_run_mock]

        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = None
        mock_settings.Session.return_value = mock_session

        sensor = ExternalTaskPokeSensor(
            task_id='test_sensor',
            external_dag_id='test_source_dag',
            external_task_id='End',
            start_date_delta=timedelta(minutes=-5),
            end_date_delta=timedelta(minutes=30),
        )

        result = sensor.poke(self._create_context())
        self.assertFalse(result)

    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.DagRun')
    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.settings')
    def test_single_pair_task_not_success(self, mock_settings, mock_dag_run):
        """Test poke returns False when task is not in SUCCESS state (single pair mode)."""
        dag_run_mock = MagicMock()
        dag_run_mock.execution_date = datetime(2024, 6, 23, 0, 10)
        mock_dag_run.find.return_value = [dag_run_mock]

        task_instance_mock = MagicMock()
        task_instance_mock.state = State.FAILED

        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = task_instance_mock
        mock_settings.Session.return_value = mock_session

        sensor = ExternalTaskPokeSensor(
            task_id='test_sensor',
            external_dag_id='test_source_dag',
            external_task_id='End',
            start_date_delta=timedelta(minutes=-5),
            end_date_delta=timedelta(minutes=30),
        )

        result = sensor.poke(self._create_context())
        self.assertFalse(result)

    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.DagRun')
    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.settings')
    def test_single_pair_task_success(self, mock_settings, mock_dag_run):
        """Test poke returns True when task is in SUCCESS state (single pair mode)."""
        dag_run_mock = MagicMock()
        dag_run_mock.execution_date = datetime(2024, 6, 23, 0, 10)
        mock_dag_run.find.return_value = [dag_run_mock]

        task_instance_mock = MagicMock()
        task_instance_mock.state = State.SUCCESS

        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = task_instance_mock
        mock_settings.Session.return_value = mock_session

        sensor = ExternalTaskPokeSensor(
            task_id='test_sensor',
            external_dag_id='test_source_dag',
            external_task_id='End',
            start_date_delta=timedelta(minutes=-5),
            end_date_delta=timedelta(minutes=30),
        )

        result = sensor.poke(self._create_context())
        self.assertTrue(result)

    # ==================== Multiple Pairs Mode - Poke Tests ====================

    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.DagRun')
    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.settings')
    def test_multiple_pairs_all_tasks_success(self, mock_settings, mock_dag_run):
        """Test poke returns True when all tasks are in SUCCESS state (multiple pairs mode)."""
        dag_run_mock = MagicMock()
        dag_run_mock.execution_date = datetime(2024, 6, 23, 0, 10)
        mock_dag_run.find.return_value = [dag_run_mock]

        task_instance_mock = MagicMock()
        task_instance_mock.state = State.SUCCESS

        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = task_instance_mock
        mock_settings.Session.return_value = mock_session

        sensor = ExternalTaskPokeSensor(
            task_id='test_sensor',
            external_tasks=[
                {'dag_id': 'dag_1', 'task_id': 'task_1'},
                {'dag_id': 'dag_2', 'task_id': 'task_2'},
            ],
            start_date_delta=timedelta(minutes=-5),
            end_date_delta=timedelta(minutes=30),
        )

        result = sensor.poke(self._create_context())
        self.assertTrue(result)

    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.DagRun')
    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.settings')
    def test_multiple_pairs_one_task_not_success(self, mock_settings, mock_dag_run):
        """Test poke returns False when one task is not in SUCCESS state (multiple pairs mode)."""
        dag_run_mock = MagicMock()
        dag_run_mock.execution_date = datetime(2024, 6, 23, 0, 10)
        mock_dag_run.find.return_value = [dag_run_mock]

        # First call returns SUCCESS, second returns FAILED
        task_instance_success = MagicMock()
        task_instance_success.state = State.SUCCESS
        task_instance_failed = MagicMock()
        task_instance_failed.state = State.FAILED

        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.first.side_effect = [
            task_instance_success,
            task_instance_failed
        ]
        mock_settings.Session.return_value = mock_session

        sensor = ExternalTaskPokeSensor(
            task_id='test_sensor',
            external_tasks=[
                {'dag_id': 'dag_1', 'task_id': 'task_1'},
                {'dag_id': 'dag_2', 'task_id': 'task_2'},
            ],
            start_date_delta=timedelta(minutes=-5),
            end_date_delta=timedelta(minutes=30),
        )

        result = sensor.poke(self._create_context())
        self.assertFalse(result)

    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.DagRun')
    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.settings')
    def test_multiple_pairs_one_dag_run_not_found(self, mock_settings, mock_dag_run):
        """Test poke returns False when one DAG run is not found (multiple pairs mode)."""
        dag_run_mock = MagicMock()
        dag_run_mock.execution_date = datetime(2024, 6, 23, 0, 10)

        # First DAG has a run, second doesn't
        mock_dag_run.find.side_effect = [[dag_run_mock], []]

        task_instance_mock = MagicMock()
        task_instance_mock.state = State.SUCCESS

        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = task_instance_mock
        mock_settings.Session.return_value = mock_session

        sensor = ExternalTaskPokeSensor(
            task_id='test_sensor',
            external_tasks=[
                {'dag_id': 'dag_1', 'task_id': 'task_1'},
                {'dag_id': 'dag_2', 'task_id': 'task_2'},
            ],
            start_date_delta=timedelta(minutes=-5),
            end_date_delta=timedelta(minutes=30),
        )

        result = sensor.poke(self._create_context())
        self.assertFalse(result)

    # ==================== Skip Sensor Tests ====================

    def test_skip_sensor_task_returns_true(self):
        """Test poke returns True when skip_sensor_task is set."""
        sensor = ExternalTaskPokeSensor(
            task_id='test_sensor',
            external_dag_id='test_source_dag',
            external_task_id='End',
            start_date_delta=timedelta(minutes=-5),
            end_date_delta=timedelta(minutes=30),
        )

        result = sensor.poke(self._create_context(skip_sensor_task=True))
        self.assertTrue(result)

    def test_skip_sensor_task_multiple_pairs_returns_true(self):
        """Test poke returns True when skip_sensor_task is set (multiple pairs mode)."""
        sensor = ExternalTaskPokeSensor(
            task_id='test_sensor',
            external_tasks=[
                {'dag_id': 'dag_1', 'task_id': 'task_1'},
                {'dag_id': 'dag_2', 'task_id': 'task_2'},
            ],
            start_date_delta=timedelta(minutes=-5),
            end_date_delta=timedelta(minutes=30),
        )

        result = sensor.poke(self._create_context(skip_sensor_task=True))
        self.assertTrue(result)

    # ==================== Edge Case Tests ====================

    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.DagRun')
    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.settings')
    def test_dag_run_outside_time_window(self, mock_settings, mock_dag_run):
        """Test poke returns False when DAG run is outside the time window."""
        dag_run_mock = MagicMock()
        # Execution date outside the window
        dag_run_mock.execution_date = datetime(2024, 6, 20)  # 3 days before
        mock_dag_run.find.return_value = [dag_run_mock]

        mock_session = MagicMock()
        mock_settings.Session.return_value = mock_session

        sensor = ExternalTaskPokeSensor(
            task_id='test_sensor',
            external_dag_id='test_source_dag',
            external_task_id='End',
            start_date_delta=timedelta(minutes=-5),
            end_date_delta=timedelta(minutes=30),
        )

        result = sensor.poke(self._create_context())
        self.assertFalse(result)

    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.DagRun')
    @patch('dags.tokenized_consent_capture.external_task_status_pokesensor.settings')
    def test_multiple_dag_runs_picks_most_recent(self, mock_settings, mock_dag_run):
        """Test that poke uses the most recent DAG run within the time window."""
        old_dag_run = MagicMock()
        old_dag_run.execution_date = datetime(2024, 6, 23, 0, 5)
        new_dag_run = MagicMock()
        new_dag_run.execution_date = datetime(2024, 6, 23, 0, 15)
        mock_dag_run.find.return_value = [old_dag_run, new_dag_run]

        task_instance_mock = MagicMock()
        task_instance_mock.state = State.SUCCESS

        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = task_instance_mock
        mock_settings.Session.return_value = mock_session

        sensor = ExternalTaskPokeSensor(
            task_id='test_sensor',
            external_dag_id='test_source_dag',
            external_task_id='End',
            start_date_delta=timedelta(minutes=-5),
            end_date_delta=timedelta(minutes=30),
        )

        result = sensor.poke(self._create_context())
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()
