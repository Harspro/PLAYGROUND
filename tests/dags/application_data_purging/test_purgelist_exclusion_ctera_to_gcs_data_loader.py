import pytest
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowFailException

from dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader import (
    PurgelistExclusionDagBuilder
)
from dags.dag_factory.environment_config import EnvironmentConfig
import dags.util.constants as consts


@pytest.fixture
def mock_environment_config():
    """Fixture to provide mock EnvironmentConfig."""
    config = MagicMock(spec=EnvironmentConfig)
    config.local_tz = MagicMock()
    config.gcp_config = {}
    config.deploy_env = 'nonprod'
    return config


@pytest.fixture
def mock_gcp_config():
    """Fixture to provide mock GCP config."""
    return {
        consts.DEPLOYMENT_ENVIRONMENT_NAME: 'nonprod'
    }


@pytest.fixture
def sample_config():
    """Fixture to provide sample configuration."""
    return {
        'nonprod': {
            'file_name': 'purgelist_exclusion.csv',
            'ctera_shared_folder': 'DFSPCBALMUAT/UAT/poc_data_movement/GS',
            'hostname': 'vllscetpr02.ngco.com',
            'username': 'LCE\\SVCPCBALMSHAREAPIUAT',
            'secret_path': '/uat-secret/data/alm',
            'secret_key': 'SVCPCBALMSHAREAPIUAT'
        },
        'prod': {
            'file_name': 'purgelist_exclusion.csv',
            'ctera_shared_folder': 'DFSPCBALMPROD/PROD/common/GS',
            'hostname': 'vllscetpr02.ngco.com',
            'username': 'LCE\\SVCPCBLEGALSHARE',
            'secret_path': '/secret/data/terminus/ctera',
            'secret_key': 'lgl.svc.password'
        },
        'gcs_bucket': 'pcb-{env}-staging-extract',
        'gcs_folder': 'purging',
        'dag': {
            'task_id': {
                'purgelist_load_task_id': 'purgelist_exclusion_process_dag'
            },
            'dag_id': {
                'purgelist_exclusion_dag_id': 'purgelist_exclusion'
            },
            'wait_for_completion': True,
            'poke_interval': 30
        },
        'tags': ['team-growth-and-sales']
    }


@pytest.fixture
def mock_context():
    """Fixture to provide mock Airflow context."""
    return {
        'config': {
            'nonprod': {
                'file_name': 'purgelist_exclusion.csv',
                'ctera_shared_folder': 'DFSPCBALMUAT/UAT/poc_data_movement/GS',
                'hostname': 'vllscetpr02.ngco.com',
                'username': 'LCE\\SVCPCBALMSHAREAPIUAT',
                'secret_path': '/uat-secret/data/alm',
                'secret_key': 'SVCPCBALMSHAREAPIUAT'
            },
            'prod': {
                'file_name': 'purgelist_exclusion.csv',
                'ctera_shared_folder': 'DFSPCBALMPROD/PROD/common/GS',
                'hostname': 'vllscetpr02.ngco.com',
                'username': 'LCE\\SVCPCBPDSSHARE',
                'secret_path': '/secret/data/terminus/ctera',
                'secret_key': 'pds.svc.password'
            },
            'gcs_bucket': 'pcb-{env}-staging-extract',
            'gcs_folder': 'purging',
            'dag': {
                'task_id': {
                    'purgelist_load_task_id': 'purgelist_exclusion_process_dag'
                },
                'dag_id': {
                    'purgelist_exclusion_dag_id': 'purgelist_exclusion'
                },
                'wait_for_completion': True,
                'poke_interval': 30
            },
            'tags': ['team-growth-and-sales']
        }
    }


class TestPurgelistExclusionDagBuilder:
    """Test class for PurgelistExclusionDagBuilder."""

    @patch('dags.util.miscutils.get_pnp_env')
    @patch('dags.util.miscutils.read_variable_or_file')
    def test_ctera_to_gcs_file_copy_success(self, mock_read_variable, mock_get_pnp_env, mock_environment_config, mock_gcp_config):
        """Test _ctera_to_gcs_file_copy with successful copy."""
        mock_read_variable.return_value = mock_gcp_config
        mock_get_pnp_env.return_value = 'nonprod'
        mock_ctera_util = MagicMock()
        mock_ctera_util.copy_ctera_to_gcs.return_value = consts.CTERA_SUCCESS
        builder = PurgelistExclusionDagBuilder(mock_environment_config)
        # Create a test context
        test_context = {
            'config': {
                'nonprod': {
                    'ctera_shared_folder': 'test_folder',
                    'file_name': 'test_file.csv'
                },
                'gcs_bucket': 'test_bucket',
                'gcs_folder': 'test_folder'
            }
        }
        with patch.object(builder, '_delete_file_from_ctera', return_value='SUCCESS: File deleted') as mock_delete:
            with patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.resolve_smb_server_config', return_value=('server_ip', 'username', 'password')):
                with patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.SMBUtil', return_value=mock_ctera_util):
                    result = builder._ctera_to_gcs_file_copy_task(**test_context)
                    assert result is None  # The method doesn't return anything on success
                    mock_ctera_util.copy_ctera_to_gcs.assert_called_once_with(
                        'test_folder', 'test_file.csv', 'test_bucket', 'test_folder', 'test_file.csv'
                    )
                    mock_delete.assert_called_once()

    @patch('dags.util.miscutils.get_pnp_env')
    @patch('dags.util.miscutils.read_variable_or_file')
    def test_ctera_to_gcs_file_copy_failure(self, mock_read_variable, mock_get_pnp_env, mock_environment_config, mock_gcp_config):
        """Test _ctera_to_gcs_file_copy with failed copy."""
        mock_read_variable.return_value = mock_gcp_config
        mock_get_pnp_env.return_value = 'nonprod'
        mock_ctera_util = MagicMock()
        mock_ctera_util.copy_ctera_to_gcs.return_value = 'FAILURE'
        builder = PurgelistExclusionDagBuilder(mock_environment_config)
        # Create a test context
        test_context = {
            'config': {
                'nonprod': {
                    'ctera_shared_folder': 'test_folder',
                    'file_name': 'test_file.csv'
                },
                'gcs_bucket': 'test_bucket',
                'gcs_folder': 'test_folder'
            }
        }
        with patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.resolve_smb_server_config', return_value=('server_ip', 'username', 'password')):
            with patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.SMBUtil', return_value=mock_ctera_util):
                with pytest.raises(AirflowFailException, match="FAILURE:test_file.csv is not copied in gcs folder test_folder"):
                    builder._ctera_to_gcs_file_copy_task(**test_context)

    @patch('dags.util.miscutils.get_pnp_env')
    @patch('dags.util.miscutils.read_variable_or_file')
    def test_ctera_to_gcs_file_copy_no_connection(self, mock_read_variable, mock_get_pnp_env, mock_environment_config, mock_gcp_config):
        """Test _ctera_to_gcs_file_copy with no CTERA connection."""
        mock_read_variable.return_value = mock_gcp_config
        mock_get_pnp_env.return_value = 'nonprod'
        builder = PurgelistExclusionDagBuilder(mock_environment_config)
        # Create a test context
        test_context = {
            'config': {
                'nonprod': {
                    'ctera_shared_folder': 'test_folder',
                    'file_name': 'test_file.csv'
                },
                'gcs_bucket': 'test_bucket',
                'gcs_folder': 'test_folder'
            }
        }
        with patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.resolve_smb_server_config', return_value=('server_ip', 'username', 'password')):
            with patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.SMBUtil', return_value=None):
                with pytest.raises(AirflowFailException, match="CTERA connection not available"):
                    builder._ctera_to_gcs_file_copy_task(**test_context)

    @patch('dags.util.miscutils.get_pnp_env')
    @patch('dags.util.miscutils.read_variable_or_file')
    def test_delete_file_from_ctera(self, mock_read_variable, mock_get_pnp_env, mock_environment_config, mock_gcp_config):
        """Test _delete_file_from_ctera method."""
        mock_read_variable.return_value = mock_gcp_config
        mock_get_pnp_env.return_value = 'nonprod'
        mock_ctera_util = MagicMock()
        builder = PurgelistExclusionDagBuilder(mock_environment_config)
        result = builder._delete_file_from_ctera(
            'test_file.csv', 'test_folder', 'gcs_folder', mock_ctera_util, 'server_ip'
        )
        expected_status = "SUCCESS:test_file.csv archived in gcs_folder:gcs_folder and removed from //server_ip/test_folder/test_file.csv path"
        assert result == expected_status
        mock_ctera_util.delete_file.assert_called_once_with('test_folder/test_file.csv')

    @patch('dags.util.miscutils.get_pnp_env')
    @patch('dags.util.miscutils.read_variable_or_file')
    @patch('dags.util.bq_utils.submit_transformation')
    @patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.bigquery.Client')
    def test_file_data_validation_task_with_external_table_creation(self, mock_bq_client, mock_submit_transformation, mock_read_variable, mock_get_pnp_env, mock_environment_config, mock_gcp_config):
        """Test _file_data_validation_task with successful table creation and validation."""
        mock_read_variable.return_value = mock_gcp_config
        mock_get_pnp_env.return_value = 'nonprod'
        mock_environment_config.gcp_config = {'processing_zone_project_id': 'test-project'}

        # Mock BigQuery client
        mock_bq_client_instance = MagicMock()
        mock_bq_client.return_value = mock_bq_client_instance

        # Mock submit_transformation
        mock_submit_transformation.return_value = {'id': 'test-project.domain_customer_acquisition.purgelist_exclusion_external_20250101_120000'}

        builder = PurgelistExclusionDagBuilder(mock_environment_config)
        # Mock the PurgeListExclusionFileValidation class
        with patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.PurgeListExclusionFileValidation') as mock_validation_class:
            mock_validation_instance = MagicMock()
            mock_validation_class.return_value = mock_validation_instance
            # Mock successful validation result
            mock_validation_result = {
                'overall_valid': True,
                'column_validation': {'is_valid': True},
                'field_validation': {'is_valid': True},
                'dob_validation': {'is_valid': True}
            }
            mock_validation_instance.perform_file_validation.return_value = mock_validation_result
            # Create a test context
            test_context = {
                'config': {
                    'nonprod': {
                        'file_name': 'purgelist_exclusion.csv'
                    },
                    'gcs_bucket': 'pcb-{env}-staging-extract',
                    'gcs_folder': 'purging',
                    'staging_config': {
                        'dataset_id': 'domain_customer_acquisition',
                        'table_name': 'purgelist_exclusion_external',
                        'schema_external': [
                            {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name': 'dob', 'type': 'STRING', 'mode': 'NULLABLE'}
                        ],
                        'skip_leading_rows': 1,
                        'field_delimiter': ',',
                        'file_format': 'CSV'
                    },
                    'validation_config': {
                        'required_columns': ['customer_id', 'dob'],
                        'mandatory_fields': ['customer_id'],
                        'dob_column': 'dob',
                        'dob_expected_format': 'YYYY-MM-DD'
                    }
                }
            }
            result = builder._file_data_validation_task(**test_context)
            assert result == "SUCCESS: File data validation completed successfully"
            mock_validation_instance.perform_file_validation.assert_called_once()

    @patch('dags.util.miscutils.get_pnp_env')
    @patch('dags.util.miscutils.read_variable_or_file')
    @patch('dags.util.bq_utils.submit_transformation')
    @patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.bigquery.Client')
    def test_file_data_validation_task_success(self, mock_bq_client, mock_submit_transformation, mock_read_variable, mock_get_pnp_env, mock_environment_config, mock_gcp_config):
        """Test _file_data_validation_task with successful validation."""
        mock_read_variable.return_value = mock_gcp_config
        mock_get_pnp_env.return_value = 'nonprod'
        mock_environment_config.gcp_config = {'processing_zone_project_id': 'test-project'}

        # Mock BigQuery client
        mock_bq_client_instance = MagicMock()
        mock_bq_client.return_value = mock_bq_client_instance

        # Mock submit_transformation
        mock_submit_transformation.return_value = {'id': 'test-project.domain_customer_acquisition.purgelist_exclusion_external_20250101_120000'}

        builder = PurgelistExclusionDagBuilder(mock_environment_config)
        # Mock the PurgeListExclusionFileValidation class
        with patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.PurgeListExclusionFileValidation') as mock_validation_class:
            mock_validation_instance = MagicMock()
            mock_validation_class.return_value = mock_validation_instance
            # Mock successful validation result
            mock_validation_result = {
                'overall_valid': True,
                'column_validation': {'is_valid': True},
                'field_validation': {'is_valid': True},
                'dob_validation': {'is_valid': True}
            }
            mock_validation_instance.perform_file_validation.return_value = mock_validation_result
            # Create a test context
            test_context = {
                'config': {
                    'nonprod': {
                        'file_name': 'purgelist_exclusion.csv'
                    },
                    'gcs_bucket': 'pcb-{env}-staging-extract',
                    'gcs_folder': 'purging',
                    'staging_config': {
                        'dataset_id': 'domain_customer_acquisition',
                        'table_name': 'purgelist_exclusion_external',
                        'schema_external': [
                            {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name': 'dob', 'type': 'STRING', 'mode': 'NULLABLE'}
                        ],
                        'skip_leading_rows': 1,
                        'field_delimiter': ',',
                        'file_format': 'CSV'
                    },
                    'validation_config': {
                        'required_columns': ['customer_id', 'dob'],
                        'mandatory_fields': ['customer_id'],
                        'dob_column': 'dob',
                        'dob_expected_format': 'YYYY-MM-DD'
                    }
                }
            }
            result = builder._file_data_validation_task(**test_context)
            assert result == "SUCCESS: File data validation completed successfully"
            mock_validation_instance.perform_file_validation.assert_called_once()

    @patch('dags.util.miscutils.get_pnp_env')
    @patch('dags.util.miscutils.read_variable_or_file')
    @patch('dags.util.bq_utils.submit_transformation')
    @patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.bigquery.Client')
    def test_file_data_validation_task_failure(self, mock_bq_client, mock_submit_transformation, mock_read_variable, mock_get_pnp_env, mock_environment_config, mock_gcp_config):
        """Test _file_data_validation_task with validation failure."""
        mock_read_variable.return_value = mock_gcp_config
        mock_get_pnp_env.return_value = 'nonprod'
        mock_environment_config.gcp_config = {'processing_zone_project_id': 'test-project'}

        # Mock BigQuery client
        mock_bq_client_instance = MagicMock()
        mock_bq_client.return_value = mock_bq_client_instance

        # Mock submit_transformation
        mock_submit_transformation.return_value = {'id': 'test-project.domain_customer_acquisition.purgelist_exclusion_external_20250101_120000'}

        builder = PurgelistExclusionDagBuilder(mock_environment_config)
        # Mock the PurgeListExclusionFileValidation class
        with patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.PurgeListExclusionFileValidation') as mock_validation_class:
            mock_validation_instance = MagicMock()
            mock_validation_class.return_value = mock_validation_instance
            # Mock failed validation result
            mock_validation_result = {
                'overall_valid': False,
                'column_validation': {'is_valid': False, 'error_message': 'Missing required columns: [dob]', 'missing_columns': ['dob']},
                'field_validation': {'is_valid': True, 'total_violations': 0, 'null_violations': 0, 'empty_violations': 0},
                'dob_validation': {'is_valid': True, 'invalid_format_count': 0}
            }
            mock_validation_instance.perform_file_validation.return_value = mock_validation_result
            # Create a test context
            test_context = {
                'config': {
                    'nonprod': {
                        'file_name': 'purgelist_exclusion.csv'
                    },
                    'gcs_bucket': 'pcb-{env}-staging-extract',
                    'gcs_folder': 'purging',
                    'staging_config': {
                        'dataset_id': 'domain_customer_acquisition',
                        'table_name': 'purgelist_exclusion_external',
                        'schema_external': [
                            {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name': 'dob', 'type': 'STRING', 'mode': 'NULLABLE'}
                        ],
                        'skip_leading_rows': 1,
                        'field_delimiter': ',',
                        'file_format': 'CSV'
                    },
                    'validation_config': {
                        'required_columns': ['customer_id', 'dob'],
                        'mandatory_fields': ['customer_id'],
                        'dob_column': 'dob',
                        'dob_expected_format': 'YYYY-MM-DD'
                    }
                }
            }
            with pytest.raises(AirflowFailException, match="BigQuery data validation failed - Missing required columns: \\[dob\\]"):
                builder._file_data_validation_task(**test_context)

    @patch('dags.util.miscutils.get_pnp_env')
    @patch('dags.util.miscutils.read_variable_or_file')
    @patch('dags.util.bq_utils.submit_transformation')
    @patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.bigquery.Client')
    def test_file_data_validation_task_missing_external_table(self, mock_bq_client, mock_submit_transformation, mock_read_variable, mock_get_pnp_env, mock_environment_config, mock_gcp_config):
        """Test _file_data_validation_task with missing external table ID."""
        mock_read_variable.return_value = mock_gcp_config
        mock_get_pnp_env.return_value = 'nonprod'
        mock_environment_config.gcp_config = {'processing_zone_project_id': 'test-project'}

        # Mock BigQuery client
        mock_bq_client_instance = MagicMock()
        mock_bq_client.return_value = mock_bq_client_instance

        # Mock submit_transformation
        mock_submit_transformation.return_value = {'id': 'test-project.domain_customer_acquisition.purgelist_exclusion_external_20250101_120000'}

        builder = PurgelistExclusionDagBuilder(mock_environment_config)
        # Mock the PurgeListExclusionFileValidation class
        with patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.PurgeListExclusionFileValidation') as mock_validation_class:
            mock_validation_instance = MagicMock()
            mock_validation_class.return_value = mock_validation_instance
            # Mock validation result that includes all required keys
            mock_validation_result = {
                'overall_valid': False,
                'column_validation': {'is_valid': False, 'error_message': 'Missing required columns: [customer_id, dob]', 'missing_columns': ['customer_id', 'dob']},
                'field_validation': {'is_valid': True, 'total_violations': 0, 'null_violations': 0, 'empty_violations': 0},
                'dob_validation': {'is_valid': True, 'invalid_format_count': 0}
            }
            mock_validation_instance.perform_file_validation.return_value = mock_validation_result
            # Create a test context
            test_context = {
                'config': {
                    'nonprod': {
                        'file_name': 'purgelist_exclusion.csv'
                    },
                    'gcs_bucket': 'pcb-{env}-staging-extract',
                    'gcs_folder': 'purging',
                    'staging_config': {
                        'dataset_id': 'domain_customer_acquisition',
                        'table_name': 'purgelist_exclusion_external',
                        'schema_external': [
                            {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name': 'dob', 'type': 'STRING', 'mode': 'NULLABLE'}
                        ],
                        'skip_leading_rows': 1,
                        'field_delimiter': ',',
                        'file_format': 'CSV'
                    },
                    'validation_config': {
                        'required_columns': ['customer_id', 'dob'],
                        'mandatory_fields': ['customer_id'],
                        'dob_column': 'dob',
                        'dob_expected_format': 'YYYY-MM-DD'
                    }
                }
            }
            with pytest.raises(AirflowFailException, match="BigQuery data validation failed - Missing required columns: \\[customer_id, dob\\]"):
                builder._file_data_validation_task(**test_context)

    @patch('dags.util.miscutils.get_pnp_env')
    @patch('dags.util.miscutils.read_variable_or_file')
    @patch('dags.util.bq_utils.submit_transformation')
    @patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.bigquery.Client')
    def test_file_data_validation_task_dob_validation_failure(self, mock_bq_client, mock_submit_transformation, mock_read_variable, mock_get_pnp_env, mock_environment_config, mock_gcp_config):
        """Test _file_data_validation_task with DOB validation failure."""
        mock_read_variable.return_value = mock_gcp_config
        mock_get_pnp_env.return_value = 'nonprod'
        mock_environment_config.gcp_config = {'processing_zone_project_id': 'test-project'}

        # Mock BigQuery client
        mock_bq_client_instance = MagicMock()
        mock_bq_client.return_value = mock_bq_client_instance

        # Mock submit_transformation
        mock_submit_transformation.return_value = {'id': 'test-project.domain_customer_acquisition.purgelist_exclusion_external_20250101_120000'}

        builder = PurgelistExclusionDagBuilder(mock_environment_config)
        # Mock the PurgeListExclusionFileValidation class
        with patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.PurgeListExclusionFileValidation') as mock_validation_class:
            mock_validation_instance = MagicMock()
            mock_validation_class.return_value = mock_validation_instance
            # Mock failed DOB validation result
            mock_validation_result = {
                'overall_valid': False,
                'column_validation': {'is_valid': True, 'missing_columns': []},
                'field_validation': {'is_valid': True, 'total_violations': 0, 'null_violations': 0, 'empty_violations': 0},
                'dob_validation': {'is_valid': False, 'error_message': 'Found 5 DOB validation violations', 'invalid_format_count': 5}
            }
            mock_validation_instance.perform_file_validation.return_value = mock_validation_result
            # Create a test context
            test_context = {
                'config': {
                    'nonprod': {
                        'file_name': 'purgelist_exclusion.csv'
                    },
                    'gcs_bucket': 'pcb-{env}-staging-extract',
                    'gcs_folder': 'purging',
                    'staging_config': {
                        'dataset_id': 'domain_customer_acquisition',
                        'table_name': 'purgelist_exclusion_external',
                        'schema_external': [
                            {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name': 'dob', 'type': 'STRING', 'mode': 'NULLABLE'}
                        ],
                        'skip_leading_rows': 1,
                        'field_delimiter': ',',
                        'file_format': 'CSV'
                    },
                    'validation_config': {
                        'required_columns': ['customer_id', 'dob'],
                        'mandatory_fields': ['customer_id'],
                        'dob_column': 'dob',
                        'dob_expected_format': 'YYYY-MM-DD'
                    }
                }
            }
            with pytest.raises(AirflowFailException, match="BigQuery data validation failed - Found 5 DOB validation violations"):
                builder._file_data_validation_task(**test_context)

    @patch('dags.util.miscutils.get_pnp_env')
    @patch('dags.util.miscutils.read_variable_or_file')
    @patch('dags.util.bq_utils.submit_transformation')
    @patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.bigquery.Client')
    def test_file_data_validation_task_field_validation_failure(self, mock_bq_client, mock_submit_transformation, mock_read_variable, mock_get_pnp_env, mock_environment_config, mock_gcp_config):
        """Test _file_data_validation_task with field validation failure."""
        mock_read_variable.return_value = mock_gcp_config
        mock_get_pnp_env.return_value = 'nonprod'
        mock_environment_config.gcp_config = {'processing_zone_project_id': 'test-project'}

        # Mock BigQuery client
        mock_bq_client_instance = MagicMock()
        mock_bq_client.return_value = mock_bq_client_instance

        # Mock submit_transformation
        mock_submit_transformation.return_value = {'id': 'test-project.domain_customer_acquisition.purgelist_exclusion_external_20250101_120000'}

        builder = PurgelistExclusionDagBuilder(mock_environment_config)
        # Mock the PurgeListExclusionFileValidation class
        with patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.PurgeListExclusionFileValidation') as mock_validation_class:
            mock_validation_instance = MagicMock()
            mock_validation_class.return_value = mock_validation_instance
            # Mock failed field validation result
            mock_validation_result = {
                'overall_valid': False,
                'column_validation': {'is_valid': True, 'missing_columns': []},
                'field_validation': {'is_valid': False, 'error_message': 'Found 8 violations in mandatory fields', 'total_violations': 8, 'null_violations': 5, 'empty_violations': 3},
                'dob_validation': {'is_valid': True, 'invalid_format_count': 0}
            }
            mock_validation_instance.perform_file_validation.return_value = mock_validation_result
            # Create a test context
            test_context = {
                'config': {
                    'nonprod': {
                        'file_name': 'purgelist_exclusion.csv'
                    },
                    'gcs_bucket': 'pcb-{env}-staging-extract',
                    'gcs_folder': 'purging',
                    'staging_config': {
                        'dataset_id': 'domain_customer_acquisition',
                        'table_name': 'purgelist_exclusion_external',
                        'schema_external': [
                            {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name': 'dob', 'type': 'STRING', 'mode': 'NULLABLE'}
                        ],
                        'skip_leading_rows': 1,
                        'field_delimiter': ',',
                        'file_format': 'CSV'
                    },
                    'validation_config': {
                        'required_columns': ['customer_id', 'dob'],
                        'mandatory_fields': ['customer_id'],
                        'dob_column': 'dob',
                        'dob_expected_format': 'YYYY-MM-DD'
                    }
                }
            }
            with pytest.raises(AirflowFailException, match="BigQuery data validation failed - Found 8 violations in mandatory fields"):
                builder._file_data_validation_task(**test_context)

    @patch('dags.util.miscutils.get_pnp_env')
    @patch('dags.util.miscutils.read_variable_or_file')
    @patch('dags.util.bq_utils.submit_transformation')
    @patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.bigquery.Client')
    def test_file_data_validation_task_multiple_validation_failures(self, mock_bq_client, mock_submit_transformation, mock_read_variable, mock_get_pnp_env, mock_environment_config, mock_gcp_config):
        """Test _file_data_validation_task with multiple validation failures."""
        mock_read_variable.return_value = mock_gcp_config
        mock_get_pnp_env.return_value = 'nonprod'
        mock_environment_config.gcp_config = {'processing_zone_project_id': 'test-project'}

        # Mock BigQuery client
        mock_bq_client_instance = MagicMock()
        mock_bq_client.return_value = mock_bq_client_instance

        # Mock submit_transformation
        mock_submit_transformation.return_value = {'id': 'test-project.domain_customer_acquisition.purgelist_exclusion_external_20250101_120000'}

        builder = PurgelistExclusionDagBuilder(mock_environment_config)
        # Mock the PurgeListExclusionFileValidation class
        with patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.PurgeListExclusionFileValidation') as mock_validation_class:
            mock_validation_instance = MagicMock()
            mock_validation_class.return_value = mock_validation_instance
            # Mock multiple validation failures
            mock_validation_result = {
                'overall_valid': False,
                'column_validation': {'is_valid': False, 'error_message': 'Missing required columns: [dob]', 'missing_columns': ['dob']},
                'field_validation': {'is_valid': False, 'error_message': 'Found 5 violations in mandatory fields', 'total_violations': 5, 'null_violations': 3, 'empty_violations': 2},
                'dob_validation': {'is_valid': False, 'error_message': 'Found 3 DOB validation violations', 'invalid_format_count': 3}
            }
            mock_validation_instance.perform_file_validation.return_value = mock_validation_result
            # Create a test context
            test_context = {
                'config': {
                    'nonprod': {
                        'file_name': 'purgelist_exclusion.csv'
                    },
                    'gcs_bucket': 'pcb-{env}-staging-extract',
                    'gcs_folder': 'purging',
                    'staging_config': {
                        'dataset_id': 'domain_customer_acquisition',
                        'table_name': 'purgelist_exclusion_external',
                        'schema_external': [
                            {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name': 'dob', 'type': 'STRING', 'mode': 'NULLABLE'}
                        ],
                        'skip_leading_rows': 1,
                        'field_delimiter': ',',
                        'file_format': 'CSV'
                    },
                    'validation_config': {
                        'required_columns': ['customer_id', 'dob'],
                        'mandatory_fields': ['customer_id'],
                        'dob_column': 'dob',
                        'dob_expected_format': 'YYYY-MM-DD'
                    }
                }
            }
            with pytest.raises(AirflowFailException, match="BigQuery data validation failed - Missing required columns: \\[dob\\] - Found 5 violations in mandatory fields - Found 3 DOB validation violations"):
                builder._file_data_validation_task(**test_context)

    @patch('dags.util.miscutils.get_pnp_env')
    @patch('dags.util.miscutils.read_variable_or_file')
    @patch('dags.util.bq_utils.submit_transformation')
    @patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.bigquery.Client')
    def test_file_data_validation_task_prod_env_success(self, mock_bq_client, mock_submit_transformation, mock_read_variable, mock_get_pnp_env, mock_environment_config, mock_gcp_config):
        """Test _file_data_validation_task with prod environment."""
        prod_gcp_config = {consts.DEPLOYMENT_ENVIRONMENT_NAME: 'prod'}
        mock_read_variable.return_value = prod_gcp_config
        mock_get_pnp_env.return_value = 'prod'
        mock_environment_config.deploy_env = 'prod'
        mock_environment_config.gcp_config = {'processing_zone_project_id': 'test-project'}
        builder = PurgelistExclusionDagBuilder(mock_environment_config)
        # Mock the PurgeListExclusionFileValidation class
        with patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.PurgeListExclusionFileValidation') as mock_validation_class:
            mock_validation_instance = MagicMock()
            mock_validation_class.return_value = mock_validation_instance
            # Mock successful validation result
            mock_validation_result = {
                'overall_valid': True,
                'column_validation': {'is_valid': True},
                'field_validation': {'is_valid': True},
                'dob_validation': {'is_valid': True}
            }
            mock_validation_instance.perform_file_validation.return_value = mock_validation_result
            # Create a test context
            test_context = {
                'config': {
                    'prod': {
                        'file_name': 'purgelist_exclusion.csv'
                    },
                    'gcs_bucket': 'pcb-{env}-staging-extract',
                    'gcs_folder': 'purging',
                    'staging_config': {
                        'dataset_id': 'domain_customer_acquisition',
                        'table_name': 'purgelist_exclusion_external',
                        'schema_external': [
                            {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name': 'dob', 'type': 'STRING', 'mode': 'NULLABLE'}
                        ],
                        'skip_leading_rows': 1,
                        'field_delimiter': ',',
                        'file_format': 'CSV'
                    },
                    'validation_config': {
                        'required_columns': ['customer_id', 'dob'],
                        'mandatory_fields': ['customer_id'],
                        'dob_column': 'dob',
                        'dob_expected_format': 'YYYY-MM-DD'
                    }
                }
            }
            result = builder._file_data_validation_task(**test_context)
            assert result == "SUCCESS: File data validation completed successfully"
            mock_validation_instance.perform_file_validation.assert_called_once()

    @patch('dags.util.miscutils.get_pnp_env')
    @patch('dags.util.miscutils.read_variable_or_file')
    @patch('dags.util.bq_utils.submit_transformation')
    @patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.bigquery.Client')
    def test_file_data_validation_task_prod_env(self, mock_bq_client, mock_submit_transformation, mock_read_variable, mock_get_pnp_env, mock_environment_config, mock_gcp_config):
        """Test _file_data_validation_task method with prod environment."""
        prod_gcp_config = {consts.DEPLOYMENT_ENVIRONMENT_NAME: 'prod'}
        mock_read_variable.return_value = prod_gcp_config
        mock_get_pnp_env.return_value = 'prod'
        mock_environment_config.deploy_env = 'prod'
        mock_environment_config.gcp_config = {'processing_zone_project_id': 'test-project'}
        builder = PurgelistExclusionDagBuilder(mock_environment_config)
        # Mock the PurgeListExclusionFileValidation class
        with patch('dags.application_data_purging.purgelist_exclusion_ctera_to_gcs_data_loader.PurgeListExclusionFileValidation') as mock_validation_class:
            mock_validation_instance = MagicMock()
            mock_validation_class.return_value = mock_validation_instance
            # Mock successful validation result
            mock_validation_result = {
                'overall_valid': True,
                'column_validation': {'is_valid': True},
                'field_validation': {'is_valid': True},
                'dob_validation': {'is_valid': True}
            }
            mock_validation_instance.perform_file_validation.return_value = mock_validation_result
            # Create a test context
            test_context = {
                'config': {
                    'prod': {
                        'file_name': 'purgelist_exclusion.csv'
                    },
                    'gcs_bucket': 'pcb-{env}-staging-extract',
                    'gcs_folder': 'purging',
                    'staging_config': {
                        'dataset_id': 'domain_customer_acquisition',
                        'table_name': 'purgelist_exclusion_external',
                        'schema_external': [
                            {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                            {'name': 'dob', 'type': 'STRING', 'mode': 'NULLABLE'}
                        ],
                        'skip_leading_rows': 1,
                        'field_delimiter': ',',
                        'file_format': 'CSV'
                    },
                    'validation_config': {
                        'required_columns': ['customer_id', 'dob'],
                        'mandatory_fields': ['customer_id'],
                        'dob_column': 'dob',
                        'dob_expected_format': 'YYYY-MM-DD'
                    }
                }
            }
            result = builder._file_data_validation_task(**test_context)
            assert result == "SUCCESS: File data validation completed successfully"
            mock_validation_instance.perform_file_validation.assert_called_once()
