import unittest
from unittest.mock import patch, Mock

from airflow.exceptions import AirflowFailException

from dags.application_data_purging.purging_job_launcher import PurgingJobLauncher


class TestPurgingJobLauncher(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create patches
        self.mock_read_var_patcher = patch('dags.application_data_purging.purging_job_launcher.read_variable_or_file')
        self.mock_read_yaml_patcher = patch('dags.application_data_purging.purging_job_launcher.read_yamlfile_env')
        self.mock_settings_patcher = patch('dags.application_data_purging.purging_job_launcher.settings')

        # Start patches
        self.mock_read_var = self.mock_read_var_patcher.start()
        self.mock_read_yaml = self.mock_read_yaml_patcher.start()
        self.mock_settings = self.mock_settings_patcher.start()

        # Mock GCP config
        self.mock_read_var.side_effect = lambda x: {
            'gcp_config': {
                'deployment_environment_name': 'dev',
                'processing_zone_connection_id': 'google_cloud_default'
            },
            'dataproc_config': {
                'project_id': 'test-project',
                'location': 'us-central1'
            }
        }.get(x, {})

        # Mock job config
        self.mock_read_yaml.return_value = {
            'file_transfer': {
                'gcs_source_bucket': 'pcb-{env}-staging-extract',
                'gcs_source_path': 'purging/',
                'files_to_transfer': ['adm_purge_list.csv', 'experian_purge_list.csv'],
                'folders_to_transfer': ['delete_procedure']
            },
            'args': {
                'cutoff_date': '20190930',
                'adm_gcp_source': 'N',
                'repartition_count': 100
            },
            'spark_job': {
                'jar_file_uris': ['gs://test-bucket/jar.jar'],
                'main_class': 'com.test.Main',
                'file_uris': ['gs://test-bucket/file.yaml'],
                'properties': {}
            }
        }

        # Mock settings
        self.mock_settings.DAGS_FOLDER = '/tmp/dags'

        self.launcher = PurgingJobLauncher()

    def tearDown(self):
        """Clean up after each test method."""
        # Stop all patches
        self.mock_read_var_patcher.stop()
        self.mock_read_yaml_patcher.stop()
        self.mock_settings_patcher.stop()

    def test_generate_pause_file_names(self):
        """Test generate_pause_file_names method."""
        # Mock report config for this specific test
        self.mock_read_yaml.return_value = {
            'sql.script.generator': {
                'tables': {
                    'adm': [
                        {
                            'name': 'ADM_STD_DISP_ADDR_INFO',
                            'schemas': 'PR053.ADM, PR053.ADM_P053, PR303.ADM'
                        },
                        {
                            'name': 'ADM_STD_DISP_CUST_INFO',
                            'schemas': 'PR053.ADM'
                        }
                    ]
                }
            }
        }

        pause_files = self.launcher.generate_pause_file_names()

        # For dev environment, should use IT prefix
        expected_files = [
            'IT053_ADM_ADM_STD_DISP_ADDR_INFO_pause.txt',
            'IT053_ADM_P053_ADM_STD_DISP_ADDR_INFO_pause.txt',
            'IT303_ADM_ADM_STD_DISP_ADDR_INFO_pause.txt',
            'IT053_ADM_ADM_STD_DISP_CUST_INFO_pause.txt'
        ]

        self.assertEqual(sorted(pause_files), sorted(expected_files))

    @patch('dags.application_data_purging.purging_job_launcher.GCSHook')
    def test_create_pause_files_in_gcs_success(self, mock_gcs_hook_class):
        """Test create_pause_files_in_gcs method with successful file creation."""
        # Mock GCS Hook
        mock_gcs_hook = Mock()
        mock_gcs_hook_class.return_value = mock_gcs_hook

        # Mock report config for pause file generation
        self.mock_read_yaml.return_value = {
            'sql.script.generator': {
                'tables': {
                    'adm': [
                        {
                            'name': 'ADM_STD_DISP_ADDR_INFO',
                            'schemas': 'PR053.ADM'
                        }
                    ]
                }
            }
        }

        # Call the method
        self.launcher.create_pause_files_in_gcs()

        # Verify GCS Hook was called correctly
        mock_gcs_hook_class.assert_called_once_with(
            gcp_conn_id='google_cloud_default'
        )

        # Verify upload was called for the pause file
        mock_gcs_hook.upload.assert_called_once_with(
            bucket_name='pcb-dev-staging-extract',
            object_name='purging/IT053_ADM_ADM_STD_DISP_ADDR_INFO_pause.txt',
            data=b"empty file"
        )

    @patch('dags.application_data_purging.purging_job_launcher.GCSHook')
    def test_create_pause_files_in_gcs_upload_error(self, mock_gcs_hook_class):
        """Test create_pause_files_in_gcs method with upload error."""
        # Mock GCS Hook
        mock_gcs_hook = Mock()
        mock_gcs_hook_class.return_value = mock_gcs_hook

        # Mock upload to raise exception
        mock_gcs_hook.upload.side_effect = Exception("Upload failed")

        # Mock report config
        self.mock_read_yaml.return_value = {
            'sql.script.generator': {
                'tables': {
                    'adm': [
                        {
                            'name': 'ADM_STD_DISP_ADDR_INFO',
                            'schemas': 'PR053.ADM'
                        }
                    ]
                }
            }
        }

        # Should raise exception
        with self.assertRaises(Exception):
            self.launcher.create_pause_files_in_gcs()

    def test_prepare_transfer_data_success(self):
        """Test prepare_transfer_data method with successful data preparation."""
        # Mock pause file generation
        self.launcher.generate_pause_file_names = Mock(return_value=['IT053_ADM_STD_DISP_ADDR_INFO_pause.txt'])

        # Mock delete procedures files
        self.launcher.get_delete_procedures_files = Mock(return_value=[
            {'file': 'test_file.sql', 'gcs_path': 'purging/delete_procedure/test_file.sql',
             'oci_path': 'delete_procedure/test_file.sql'}
        ])

        context = {'dag_run': Mock(conf={'oci_destination_url': 'https://test-oci-url.com/upload'})}

        # --- Act ---
        result = self.launcher.prepare_transfer_data(**context)

        # --- Assert ---
        expected_result = [
            {
                'file': 'test_file.sql',
                'gcs_path': 'purging/delete_procedure/test_file.sql',
                'oci_path': 'delete_procedure/test_file.sql',
                'source': 'delete_procedure folder'
            },
            {
                'file': 'adm_purge_list.csv',
                'gcs_path': 'purging/adm_purge_list.csv',
                'oci_path': 'adm_purge_list.csv',
                'source': 'configured file'
            },
            {
                'file': 'experian_purge_list.csv',
                'gcs_path': 'purging/experian_purge_list.csv',
                'oci_path': 'experian_purge_list.csv',
                'source': 'configured file'
            },
            {
                'file': 'IT053_ADM_STD_DISP_ADDR_INFO_pause.txt',
                'gcs_path': 'purging/IT053_ADM_STD_DISP_ADDR_INFO_pause.txt',
                'oci_path': 'IT053_ADM_STD_DISP_ADDR_INFO_pause.txt',
                'source': 'pause file'
            }
        ]

        self.assertEqual(result, expected_result)

    def test_prepare_transfer_data_missing_oci_url(self):
        """Test prepare_transfer_data method with missing OCI URL."""
        # Call the method without OCI URL
        context = {'dag_run': Mock(conf={})}

        with self.assertRaises(AirflowFailException) as cm:
            self.launcher.prepare_transfer_data(**context)

        self.assertIn("OCI destination URL not provided", str(cm.exception))

    def test_build_transfer_items(self):
        """Test build_transfer_items method."""
        file_transfer_config = {
            'folders_to_transfer': ['delete_procedure'],
            'files_to_transfer': ['test.csv']
        }
        gcs_path = 'purging/'
        pause_files = ['pause1.txt', 'pause2.txt']

        # Mock get_delete_procedures_files
        self.launcher.get_delete_procedures_files = Mock(return_value=[
            {'file': 'proc1.sql', 'gcs_path': 'purging/delete_procedure/proc1.sql', 'oci_path': 'delete_procedure/proc1.sql'}
        ])

        # --- Act ---
        result = self.launcher.build_transfer_items(file_transfer_config, gcs_path, pause_files)

        # --- Assert ---
        expected_items = [
            {
                'file': 'proc1.sql',
                'gcs_path': 'purging/delete_procedure/proc1.sql',
                'oci_path': 'delete_procedure/proc1.sql',
                'source': 'delete_procedure folder'
            },
            {
                'file': 'test.csv',
                'gcs_path': 'purging/test.csv',
                'oci_path': 'test.csv',
                'source': 'configured file'
            },
            {
                'file': 'pause1.txt',
                'gcs_path': 'purging/pause1.txt',
                'oci_path': 'pause1.txt',
                'source': 'pause file'
            },
            {
                'file': 'pause2.txt',
                'gcs_path': 'purging/pause2.txt',
                'oci_path': 'pause2.txt',
                'source': 'pause file'
            }
        ]

        self.assertEqual(result, expected_items)

    def test_create_gcs_to_oci_transfer_data_task(self):
        """Test create_gcs_to_oci_transfer_data_task method."""
        # --- Act ---
        task = self.launcher.create_gcs_to_oci_transfer_data_task()

        # --- Assert ---
        self.assertEqual(task.task_id, 'prepare_gcs_to_oci_transfer_data')
        self.assertEqual(task.python_callable, self.launcher.prepare_transfer_data)

    def test_create_pause_files_task(self):
        """Test create_pause_files_task method."""
        # --- Act ---
        task = self.launcher.create_pause_files_task()

        # --- Assert ---
        self.assertEqual(task.task_id, 'create_pause_files')
        self.assertEqual(task.python_callable, self.launcher.create_pause_files_in_gcs)

    def test_get_delete_procedures_files_success(self):
        """Test get_delete_procedures_files method with successful file listing."""
        # Mock GCS Hook
        with patch('dags.application_data_purging.purging_job_launcher.GCSHook') as mock_gcs_hook_class:
            mock_gcs_hook = Mock()
            mock_gcs_hook_class.return_value = mock_gcs_hook
            mock_gcs_hook.list.return_value = [
                'purging/delete_procedure/file1.sql',
                'purging/delete_procedure/file2.sql',
                'purging/delete_procedure/subfolder/',  # Should be skipped
                'purging/delete_procedure/file3.sql'
            ]

            # --- Act ---
            result = self.launcher.get_delete_procedures_files('delete_procedure')

            # --- Assert ---
            expected_files = [
                {
                    'file': 'delete_procedure/file1.sql',
                    'gcs_path': 'purging/delete_procedure/file1.sql',
                    'oci_path': 'delete_procedure/file1.sql'
                },
                {
                    'file': 'delete_procedure/file2.sql',
                    'gcs_path': 'purging/delete_procedure/file2.sql',
                    'oci_path': 'delete_procedure/file2.sql'
                },
                {
                    'file': 'delete_procedure/file3.sql',
                    'gcs_path': 'purging/delete_procedure/file3.sql',
                    'oci_path': 'delete_procedure/file3.sql'
                }
            ]

            self.assertEqual(result, expected_files)
            mock_gcs_hook.list.assert_called_once_with(
                bucket_name='pcb-dev-staging-extract',
                prefix='purging/delete_procedure/'
            )

    def test_get_delete_procedures_files_error(self):
        """Test get_delete_procedures_files method with error."""
        # Mock GCS Hook to raise exception
        with patch('dags.application_data_purging.purging_job_launcher.GCSHook') as mock_gcs_hook_class:
            mock_gcs_hook = Mock()
            mock_gcs_hook_class.return_value = mock_gcs_hook
            mock_gcs_hook.list.side_effect = Exception("GCS error")

            # --- Act & Assert ---
            with self.assertRaises(AirflowFailException):
                self.launcher.get_delete_procedures_files('delete_procedure')


if __name__ == '__main__':
    unittest.main()
