import unittest
import datetime
from unittest.mock import patch, MagicMock
from dags.bq_to_oracle_loader.bq_to_oracle_base import BqToOracleProcessor
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow import DAG, settings
from util.bq_utils import run_bq_query


class TestBqToOracleProcessor(unittest.TestCase):

    def setUp(self):
        self.default_args = {"owner": "test", "start_date": datetime.datetime(2023, 1, 1)}
        self.processor = BqToOracleProcessor(
            module_name="bq_to_oracle_loader",
            config_path=f"{settings.DAGS_FOLDER}/bq_to_oracle_loader/config",
            config_filename="bq_to_oracle_config.yaml",
            dag_default_args=self.default_args
        )

    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.read_file_env")
    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.bq_utils.run_bq_query")
    def test_create_temp_tables(self, mock_run_bq_query, mock_read_file_env):
        file_create_dt = "2025-07-03"
        mock_read_file_env.return_value = f"SELECT * FROM table WHERE dt = '{file_create_dt}'"
        mock_run_bq_query.return_value = "success"

        dag_config = {
            "spark": {
                "updater_job_args": {
                    "EVENTS": {
                        "oracle.schema.table.name": "SCHEMA.TABLE"
                    }
                }
            }
        }

        self.processor.create_temp_tables(dag_config, file_create_dt, segment_name="EVENTS", file_name="")

        mock_read_file_env.assert_called_once()
        mock_run_bq_query.assert_called_once()
        self.assertIn(file_create_dt, mock_run_bq_query.call_args[0][0])

    @patch.object(BqToOracleProcessor, 'create_dag')
    def test_create_dags(self, mock_create_dag):
        mock_dag = DAG(dag_id="test_dag",
                       schedule=datetime.timedelta(days=1),
                       start_date=datetime.datetime(2023, 1, 1))
        mock_create_dag.return_value = mock_dag

        self.processor.job_config = {
            "test_dag": {
                "job_size": "small",
                "cluster_name": "test-cluster",
                "tags": ["bq", "oracle"],
                "schedule": datetime.timedelta(days=1),
                "start_date": datetime.datetime(2023, 1, 1)
            }
        }

        result = self.processor.create_dags()

        self.assertIn("test_dag", result)
        self.assertIs(result["test_dag"], mock_dag)
        self.assertEqual(result["test_dag"].tags, ["bq", "oracle"])

    def test_skip_oracle_load_configuration(self):
        """Test that skip_oracle_segments config is properly read"""
        dag_config = {
            'enable_gcs_export': True,
            'gcs_export': {
                'skip_oracle_segments': ['SEGMENT_A', 'SEGMENT_B'],
                'export_segments': ['SEGMENT_A', 'SEGMENT_B'],
                'move_to_landing': {
                    'enabled': True,
                    'destination_bucket': 'landing-bucket',
                    'destination_folder': 'landing_folder'
                }
            }
        }

        export_config = dag_config.get('gcs_export', {})
        skip_segments = export_config.get('skip_oracle_segments', None)

        self.assertIsNotNone(skip_segments)
        self.assertIn('SEGMENT_A', skip_segments)
        self.assertIn('SEGMENT_B', skip_segments)

        landing_config = export_config.get('move_to_landing', {})
        self.assertTrue(landing_config.get('enabled', False))
        self.assertEqual(landing_config.get('destination_bucket'), 'landing-bucket')

    def test_destination_bucket_environment_substitution(self):
        """Test that environment variables in destination bucket are replaced correctly"""
        self.processor.deploy_env = 'dev'
        destination_bucket = 'mft-pcb-outbound{env_suffix}'

        env_suffix = '' if self.processor.deploy_env == 'prod' else f'-{self.processor.deploy_env}'
        result_bucket = destination_bucket.replace('{env}', self.processor.deploy_env).replace('{env_suffix}',
                                                                                               env_suffix)

        self.assertEqual(result_bucket, 'mft-pcb-outbound-dev')

        self.processor.deploy_env = 'prod'
        env_suffix = '' if self.processor.deploy_env == 'prod' else f'-{self.processor.deploy_env}'
        result_bucket = destination_bucket.replace('{env}', self.processor.deploy_env).replace('{env_suffix}',
                                                                                               env_suffix)

        self.assertEqual(result_bucket, 'mft-pcb-outbound')

    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.delete_folder")
    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.bigquery.Client")
    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.read_env_filepattern")
    def test_export_bq_table_pushes_xcom(self, mock_read_env, mock_bq_client, mock_delete_folder):
        """Test that export_bq_table_to_gcs actually pushes XCom values"""

        mock_client = MagicMock()
        mock_bq_client.return_value = mock_client
        mock_extract_job = MagicMock()
        mock_client.extract_table.return_value = mock_extract_job

        mock_read_env.return_value = 'dat'

        mock_ti = MagicMock()
        context = {'ti': mock_ti}

        dag_config = {
            'bigquery': {
                'processing_project_id': 'test-project',
                'dataset_id': 'test_dataset'
            },
            'gcs_export': {
                'bucket_name': 'test-bucket',
                'file_format': 'CSV',
                'compression': 'GZIP',
                'field_delimiter': '|',
                'segment_file_names': {
                    'TEST_SEGMENT': {
                        'output_filename': 'test_file',
                        'file_extension': '{file_env}',
                        'gcs_path': 'test_exports'
                    }
                }
            }
        }

        result = self.processor.export_bq_table_to_gcs(dag_config, 'TEST_SEGMENT', **context)

        self.assertIsNotNone(result)
        self.assertTrue(result.startswith('gs://'))
        self.assertIn('test-bucket', result)
        self.assertIn('test_exports', result)

        self.assertEqual(mock_ti.xcom_push.call_count, 3)

        call_args_list = mock_ti.xcom_push.call_args_list
        pushed_keys = [call[1]['key'] for call in call_args_list]

        self.assertIn('TEST_SEGMENT_gcs_bucket', pushed_keys)
        self.assertIn('TEST_SEGMENT_gcs_path', pushed_keys)
        self.assertIn('TEST_SEGMENT_output_filename', pushed_keys)

        bucket_call = [call for call in call_args_list if call[1]['key'] == 'TEST_SEGMENT_gcs_bucket'][0]
        self.assertEqual(bucket_call[1]['value'], 'test-bucket')

    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.delete_folder")
    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.bigquery.Client")
    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.read_env_filepattern")
    def test_export_cleanup_before_start(self, mock_read_env, mock_bq_client, mock_delete_folder):
        """Test that export cleans up the folder before starting"""
        mock_client = MagicMock()
        mock_bq_client.return_value = mock_client
        mock_extract_job = MagicMock()
        mock_client.extract_table.return_value = mock_extract_job
        mock_read_env.return_value = 'csv'

        mock_ti = MagicMock()
        context = {'ti': mock_ti}

        dag_config = {
            'bigquery': {'processing_project_id': 'test-project', 'dataset_id': 'test_dataset'},
            'gcs_export': {
                'bucket_name': 'test-bucket',
                'file_format': 'CSV',
                'compression': 'NONE',
                'field_delimiter': '|',
                'segment_file_names': {
                    'TEST_SEGMENT': {
                        'output_filename': 'test_file',
                        'file_extension': 'csv',
                        'gcs_path': 'test_exports'
                    }
                }
            }
        }

        self.processor.export_bq_table_to_gcs(dag_config, 'TEST_SEGMENT', **context)

        mock_delete_folder.assert_called_once_with('test-bucket', 'test_exports')

    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.compose_file")
    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.delete_blobs")
    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.list_blobs_with_prefix")
    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.upload_blob")
    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.read_env_filepattern")
    @patch("os.path.exists")
    def test_compose_without_header(self, mock_exists, mock_read_env, mock_upload,
                                    mock_list_blobs, mock_delete, mock_compose):
        """Test compose works without header when add_header is False"""
        mock_exists.return_value = True
        mock_read_env.return_value = 'csv'

        mock_list_blobs.return_value = [f'file-{i:012d}.csv' for i in range(10)]

        mock_ti = MagicMock()
        mock_ti.xcom_pull.side_effect = [
            'test-bucket',
            'test_exports/test_file_20251102-*'
        ]

        context = {'ti': mock_ti}

        dag_config = {
            'gcs_export': {
                'file_format': 'CSV',
                'merge_sharded_files': {
                    'enabled': True,
                    'add_header': False
                },
                'segment_file_names': {
                    'TEST_SEGMENT': {
                        'output_filename': 'test_file',
                        'file_extension': 'csv',
                        'gcs_path': 'test_exports'
                    }
                }
            }
        }

        self.processor.compose_sharded_files_into_one(dag_config, 'TEST_SEGMENT', **context)

        mock_upload.assert_not_called()
        mock_compose.assert_called_once()

    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.compose_file")
    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.list_blobs_with_prefix")
    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.storage.Client")
    @patch("os.path.exists")
    def test_compose_raises_error_when_no_files_found(self, mock_exists, mock_storage_client, mock_list_blobs, mock_compose):
        """Test compose raises AirflowFailException when no files found"""
        from airflow.exceptions import AirflowFailException

        mock_exists.return_value = True
        mock_list_blobs.return_value = []

        mock_ti = MagicMock()
        mock_ti.xcom_pull.side_effect = [
            'test-bucket',
            'test_exports/test_file_20251102-*'
        ]

        context = {'ti': mock_ti}

        dag_config = {
            'gcs_export': {
                'file_format': 'CSV',
                'merge_sharded_files': {
                    'enabled': True,
                    'add_header': False
                },
                'segment_file_names': {
                    'TEST_SEGMENT': {
                        'output_filename': 'test_file',
                        'file_extension': 'csv',
                        'gcs_path': 'test_exports'
                    }
                }
            }
        }

        with self.assertRaises(AirflowFailException):
            self.processor.compose_sharded_files_into_one(dag_config, 'TEST_SEGMENT', **context)

    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.list_blobs_with_prefix")
    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.storage.Client")
    @patch("os.path.exists")
    def test_compose_raises_error_when_header_file_missing(self, mock_exists, mock_storage_client, mock_list_blobs):
        """Test compose raises AirflowFailException when header file not found"""
        from airflow.exceptions import AirflowFailException

        mock_exists.return_value = False
        mock_list_blobs.return_value = [f'file-{i:012d}.csv' for i in range(10)]

        mock_ti = MagicMock()
        mock_ti.xcom_pull.side_effect = [
            'test-bucket',
            'test_exports/test_file_20251102-*'
        ]

        context = {'ti': mock_ti}

        dag_config = {
            'gcs_export': {
                'file_format': 'CSV',
                'merge_sharded_files': {
                    'enabled': True,
                    'add_header': True
                },
                'segment_file_names': {
                    'TEST_SEGMENT': {
                        'output_filename': 'test_file',
                        'file_extension': 'csv',
                        'gcs_path': 'test_exports',
                        'header_file': 'non_existent_header.csv'
                    }
                }
            }
        }

        with self.assertRaises(AirflowFailException):
            self.processor.compose_sharded_files_into_one(dag_config, 'TEST_SEGMENT', **context)

    @patch("dags.bq_to_oracle_loader.bq_to_oracle_base.storage.Client")
    def test_compose_raises_error_when_header_path_not_configured(self, mock_storage_client):
        """Test compose raises AirflowFailException when header_file not configured"""
        from airflow.exceptions import AirflowFailException

        mock_ti = MagicMock()
        mock_ti.xcom_pull.side_effect = [
            'test-bucket',
            'test_exports/test_file_20251102-*'
        ]

        context = {'ti': mock_ti}

        dag_config = {
            'gcs_export': {
                'file_format': 'CSV',
                'merge_sharded_files': {
                    'enabled': True,
                    'add_header': True
                },
                'segment_file_names': {
                    'TEST_SEGMENT': {
                        'output_filename': 'test_file',
                        'file_extension': 'csv',
                        'gcs_path': 'test_exports'
                    }
                }
            }
        }

        with self.assertRaises(AirflowFailException):
            self.processor.compose_sharded_files_into_one(dag_config, 'TEST_SEGMENT', **context)


if __name__ == "__main__":
    unittest.main()
