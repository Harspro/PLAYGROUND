import pytest
from unittest.mock import Mock, MagicMock
from doc_generation_processing.custom_gke_start_pod_operator import CustomGKEStartPodOperator


@pytest.fixture
def operator_kwargs():
    return {
        'cluster_name': 'test-cluster',
        'location': 'us-central1-a',
        'project_id': 'project-id',
        'namespace': 'default',
        'name': 'test-pod'
    }


def test_custom_gke_start_pod_operator_initialization(operator_kwargs):
    env_vars = {
        'INPUT_PDFS_PATH': 'manual__2025_07_14T19_06_25_00_00/LETTER/SIMPLEX/EN',
        'OUTPUT_FILE_NAME': 'output_file',
        'OUTPUT_FILE_EXTENSION': '.pdf'
    }
    operator = CustomGKEStartPodOperator(env_vars=env_vars, task_id='test_task', **operator_kwargs)

    assert operator.env_vars == env_vars
    assert operator.task_id == 'test_task'


@pytest.mark.parametrize(
    "env_vars, expected_path, expected_filename",
    [
        (
            {
                'INPUT_PDFS_PATH': 'manual__2025_07_14T19_06_25_00_00/LETTER/SIMPLEX/EN',
                'OUTPUT_FILE_NAME': 'output_file',
                'OUTPUT_FILE_EXTENSION': '.uatv'
            },
            'LETTER/SIMPLEX/EN',
            'output_file.uatv'
        ),
        (
            {
                'INPUT_PDFS_PATH': 'manual__2025_07_14T19_06_25_00_00/LETTER/DUPLEX_2/FR',
                'OUTPUT_FILE_NAME': 'result_file',
                'OUTPUT_FILE_EXTENSION': '.prod'
            },
            'LETTER/DUPLEX_2/FR',
            'result_file.prod'
        ),
    ]
)
def test_execute_method(env_vars, expected_path, expected_filename, operator_kwargs, mocker):
    mocker.patch('airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator.execute',
                 return_value=None)
    operator = CustomGKEStartPodOperator(env_vars=env_vars, task_id='test_task', **operator_kwargs)

    mock_context = {'task_instance': MagicMock()}
    mock_push = mock_context['task_instance'].xcom_push

    operator.execute(mock_context)
    mock_push.assert_any_call(key='path', value=expected_path)
    mock_push.assert_any_call(key='outbound_filename', value=expected_filename)
