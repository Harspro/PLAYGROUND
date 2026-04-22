import unittest
from copy import deepcopy
from unittest.mock import patch
from airflow import DAG
from util.constants import DEPLOYMENT_ENVIRONMENT_NAME, DEFAULT_ARGS
from dags.data_validation.base_validator import BaseValidator, INITIAL_DEFAULT_ARGS


class TestValidator(BaseValidator):
    def validate(self, config: dict) -> None:
        pass


class TestBaseValidator(unittest.TestCase):
    @patch('data_validation.base_validator.read_variable_or_file')
    @patch('data_validation.base_validator.read_yamlfile_env')
    @patch('data_validation.base_validator.get_schedule_interval')
    def test_create_dag(self, mock_get_schedule_interval, mock_read_yamlfile_env,
                        mock_read_variable_or_file):
        mock_read_variable_or_file.return_value = {DEPLOYMENT_ENVIRONMENT_NAME: "test_env"}
        mock_read_yamlfile_env.return_value = {'job_id': {DEFAULT_ARGS: {'owner': 'test_owner'}}}
        mock_get_schedule_interval.return_value = '0 8 * * 0'

        validator = TestValidator(config_filename="mock_file.yaml", config_dir="/mock/dags/folder")
        config = mock_read_yamlfile_env.return_value['job_id']
        dag = validator.create_dag("test_dag_id", config)

        self.assertIsInstance(dag, DAG)
        self.assertEqual(dag.dag_id, "test_dag_id")

        default_args = deepcopy(INITIAL_DEFAULT_ARGS)
        default_args.update(config.get(DEFAULT_ARGS, {}))
        self.assertEqual(dag.default_args['owner'], 'test_owner')

        self.assertEqual(len(dag.task_dict), 3)
        self.assertIn('start', dag.task_dict)
        self.assertIn('validate', dag.task_dict)
        self.assertIn('end', dag.task_dict)

    @patch('data_validation.base_validator.read_variable_or_file')
    @patch('data_validation.base_validator.read_yamlfile_env')
    def test_create_dags(self, mock_read_yamlfile_env, mock_read_variable_or_file):
        mock_read_variable_or_file.return_value = {DEPLOYMENT_ENVIRONMENT_NAME: "test_env"}
        mock_read_yamlfile_env.return_value = {
            'job_id1': {
                DEFAULT_ARGS: {'owner': 'owner1'},
                'schedule_interval': {'test_env': '0 8 * * 0'}
            },
            'job_id2': {
                DEFAULT_ARGS: {'owner': 'owner2'},
                'schedule_interval': {'test_env': '0 8 * * 0'}
            }
        }

        validator = TestValidator(config_filename="mock_file.yaml", config_dir="/mock/dags/folder")
        validator.job_config = mock_read_yamlfile_env.return_value
        dags = validator.create_dags()

        self.assertEqual(len(dags), 2)
        self.assertIn('job_id1', dags)
        self.assertIn('job_id2', dags)

        for dag_id in ['job_id1', 'job_id2']:
            self.assertIsInstance(dags[dag_id], DAG)
            self.assertEqual(dags[dag_id].dag_id, dag_id)
