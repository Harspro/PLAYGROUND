from __future__ import annotations
import os
from unittest.mock import patch, MagicMock, call

import pytest

from airflow import settings
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule

from bigquery_db_connector_loader.bigquery_db_connector_base import BigQueryDbConnector
from file_kafka_connector_loader.file_kafka_connector_writer_base import FileKafkaConnectorWriter
from util.miscutils import read_yamlfile_env_suffix, read_yamlfile_env, get_cluster_config_by_job_size

current_script_directory = os.path.dirname(os.path.abspath(__file__))
kafka_writer_config_directory = os.path.join(
    current_script_directory,
    "..",
    "config",
    "file_kafka_connector_writer_configs"
)


@pytest.fixture
def test_gcp_dataproc_data() -> dict:
    """
    Pytest fixture providing test data for the creation of Kafka Writer DAGs.

    It returns a dictionary that includes sample values for deployment environment name,
    network tag, processing zone connection id, location, and project id.

    :returns: A dictionary containing test data for when creating Kafka Writer DAGs.
    :rtype: dict
    """
    return {
        'deployment_environment_name': 'test-deployment-environment',
        'network_tag': 'test-network-tag',
        'processing_zone_connection_id': 'google_cloud_default',
        'location': 'northamerica-northeast1',
        'project_id': 'test-project',
        'deploy_env_storage_suffix': 'test-suffix'
    }


class TestFileKafkaConnectorWriter:
    """
    This test suite covers the functionality of the `FileKafkaConnectorWriter` class.

    The class is responsible for using config files to create DAGs that publish events
    to Kafka.

    The tests validate various aspects of the classes functionality, including:
    - Ensuring DAGs are created using the correct config parameters provided.
    - Ensuring Operators are called correctly.
    - Error handling when using a BigQuery source but not providing a query.

    Usage:
    - Run the tests using the pytest command.
    - Utilize coverage package to determine test coverage on operator.
    """

    @pytest.mark.parametrize("config_file, query_file, bigquery_source",
                             [
                                 ("test_bq_to_kafka_case_1.yaml", False, True),
                                 ("test_bq_to_kafka_case_2.yaml", True, True),
                                 ("test_bq_to_kafka_case_3.yaml", False, True),
                                 ("test_file_to_kafka_case_1.yaml", False, False),
                                 ("test_file_to_kafka_case_2.yaml", False, False)
                             ])
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.PythonOperator")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.DataprocSubmitJobOperator")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.DataprocCreateClusterOperator")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.pause_unpause_dag")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.read_pause_unpause_setting")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.read_yamlfile_env_suffix")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.read_variable_or_file")
    def test_kafka_writer_dag_creation(self, mock_read_variable_or_file, mock_read_yamlfile_env_suffix,
                                       mock_read_pause_unpause_setting, mock_pause_unpause_dag,
                                       mock_dataproc_create_cluster_operator,
                                       mock_dataproc_submit_job_operator, mock_python_operator,
                                       config_file: str, query_file: bool,
                                       bigquery_source: bool, test_gcp_dataproc_data: dict):
        """
        Test Kafka writer DAG creation for the `FileKafkaConnectorWriter` class. This is used to
        simulate creating DAGs with various different configuration.

        This test function is parameterized to run with different combinations of
        config files (`config_file`), whether to use a query file (`query_file`),
         and whether the source of the data is from bigquery (`bigquery_source`).

        :param mock_read_variable_or_file: Fixture providing a mock object for when
            reading variables.
        :type mock_read_variable_or_file: :class:`unittest.mock.MagicMock`

        :param mock_read_yamlfile_env_suffix: Fixture providing a mock object for when
            reading a file
        :type mock_read_yamlfile_env_suffix: :class:`unittest.mock.MagicMock`

        :param mock_read_pause_unpause_setting: Fixture providing a mock object
            for determining if a DAG is paused.
        :type mock_read_pause_unpause_setting: :class:`unittest.mock.MagicMock`

        :param mock_pause_unpause_dag: Fixture providing a mock object to
            unpause/pause DAG.
        :type mock_pause_unpause_dag: :class:`unittest.mock.MagicMock`

        :param mock_dataproc_create_cluster_operator: Fixture providing a mock object
            for the dataproc create cluster operator.
        :type mock_dataproc_create_cluster_operator: :class:`unittest.mock.MagicMock`

        :param mock_dataproc_submit_job_operator: Fixture providing a mock object
            for the dataproc submit job operator.
        :type mock_dataproc_submit_job_operator: :class:`unittest.mock.MagicMock`

        :param mock_python_operator: Fixture providing a mock object
            for the python operator.
        :type mock_python_operator: :class:`unittest.mock.MagicMock`

        :param config_file: Config file to use.
        :type config_file: str

        :param query_file: Whether a query file will be used.
        :type query_file: bool

        :param bigquery_source: Whether the source of the data is BigQuery.
        :type bigquery_source: bool

        :param test_gcp_dataproc_data: Dictionary of fixture test data for
            Kafka Writer DAG creation test cases.
        :type test_gcp_dataproc_data: dict
        """

        # Setting the return value for the mocked objects.
        mock_read_variable_or_file.return_value = test_gcp_dataproc_data
        mock_read_yamlfile_env_suffix.return_value = read_yamlfile_env_suffix(
            f'{current_script_directory}/sql/test_query_file.sql',
            test_gcp_dataproc_data['deployment_environment_name']
        )
        mock_read_pause_unpause_setting.return_value = True
        mock_pause_unpause_dag.return_value = None

        if config_file == "test_bq_to_kafka_case_3.yaml":
            with pytest.raises(AirflowFailException) as e:
                # Initiate DAG creation based on config file.
                globals().update(
                    FileKafkaConnectorWriter(
                        config_file,
                        kafka_writer_config_directory
                    ).create_dags()
                )

            # Check if the correct error message is thrown when not providing a query if
            # BigQuery is the source.
            assert str(e.value) == "Please provide query, none found."
        else:
            # Initiate DAG creation based on config file.
            globals().update(
                FileKafkaConnectorWriter(
                    config_file,
                    kafka_writer_config_directory
                ).create_dags()
            )

            # Verify reading environment variables is done twice.
            # Once for gcp config and another for dataproc config.
            assert mock_read_variable_or_file.call_count == 2

            # Verifying the Dataproc Create Cluster Operator is called once
            # with the correct arguments.
            mock_dataproc_create_cluster_operator.assert_called_once_with(
                task_id="create_cluster",
                project_id=test_gcp_dataproc_data['project_id'],
                cluster_config=get_cluster_config_by_job_size(
                    test_gcp_dataproc_data['deployment_environment_name'],
                    test_gcp_dataproc_data['network_tag'],
                    "large"
                ),
                region=test_gcp_dataproc_data['location'],
                cluster_name="{{ dag_run.conf.get('cluster_name')  or 'test-cluster-name' }}"
            )

            # Verifying the Dataproc Submit Job Operator is called once with the correct arguments.
            mock_dataproc_submit_job_operator.assert_called_once_with(
                task_id="kafka-writer-test",
                job=FileKafkaConnectorWriter.build_fetching_job(
                    FileKafkaConnectorWriter(config_file, kafka_writer_config_directory),
                    cluster_name="{{ dag_run.conf.get('cluster_name')  or 'test-cluster-name' }}",
                    config=(
                        next(
                            iter(
                                read_yamlfile_env(
                                    f'{kafka_writer_config_directory}/{config_file}',
                                    test_gcp_dataproc_data['deployment_environment_name']
                                ).values()
                            )
                        )
                    )
                ),
                region=test_gcp_dataproc_data['location'],
                project_id=test_gcp_dataproc_data['project_id'],
                gcp_conn_id=test_gcp_dataproc_data['processing_zone_connection_id'],
                trigger_rule=TriggerRule.ALL_SUCCESS
            )

            if not bigquery_source:
                # Verify the read pause/unpause setting is called once with the correct arguments.
                mock_read_pause_unpause_setting.assert_called_once_with(
                    "test_file_to_kafka_dag",
                    "test-deployment-environment"
                )
            else:
                if query_file:
                    # Verify reading the sql file is called once with the correct arguments.
                    mock_read_yamlfile_env_suffix.assert_called_once_with(
                        f"{settings.DAGS_FOLDER}/file_kafka_connector_loader/sql/test_query_file.sql",
                        test_gcp_dataproc_data['deployment_environment_name'],
                        test_gcp_dataproc_data['deploy_env_storage_suffix']
                    )

                # Verifying the Python Operator is called once with the correct arguments.
                # Different SQL format based on query_file parameter
                if query_file:
                    expected_query = (
                        "EXPORT DATA OPTIONS ( "
                        "uri='gs://pcb-test-deployment-environment-staging-extract"
                        "/test-folder-name/test-file-name-*.parquet', "
                        "format = 'PARQUET', "
                        "overwrite = true ) "
                        "AS (SELECT * FROM "
                        "`pcb-test-deployment-environment-landing.domain_test.test_table`);"
                    )
                else:
                    expected_query = (
                        "EXPORT DATA OPTIONS\n(\n"
                        "uri='gs://pcb-test-deployment-environment-staging-extract"
                        "/test-folder-name/test-file-name-*.parquet',\n"
                        "format = 'PARQUET',\n"
                        "overwrite = true\n) "
                        "AS (SELECT * FROM "
                        "`pcb-test-deployment-environment-landing.domain_test.test_table`);"
                    )

                mock_python_operator.assert_called_with(
                    task_id="query_bigquery_table",
                    python_callable=BigQueryDbConnector.run_bigquery_staging_query,
                    op_kwargs={"query": expected_query,
                               "project_id": "pcb-test-deployment-environment-processing",
                               "dataset_id": "domain_test",
                               "table_id": "test",
                               "replacements": "{{ dag_run.conf.get('replacements')  or 'None' }}",
                               "env_specific_replacements": None,
                               "deploy_env": "test-deployment-environment"
                               }
                )

    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.storage.Client")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.get_matching_files")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.count_rows_in_file")
    def test_count_file_rows_with_parquet_files(self, mock_count_rows, mock_get_matching_files, mock_storage_client):
        """
        Test the count_file_rows static method with Parquet files.

        This test verifies that the method correctly identifies matching files and counts
        total rows across multiple files.

        :param mock_count_rows: Mock for the count_rows_in_file function.
        :type mock_count_rows: :class:`unittest.mock.MagicMock`

        :param mock_get_matching_files: Mock for the get_matching_files function.
        :type mock_get_matching_files: :class:`unittest.mock.MagicMock`

        :param mock_storage_client: Mock for the GCS storage client.
        :type mock_storage_client: :class:`unittest.mock.MagicMock`
        """
        # Setup mocks
        mock_bucket = MagicMock()
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_storage_client.return_value = mock_client

        mock_get_matching_files.return_value = [
            'folder/file-part1.parquet',
            'folder/file-part2.parquet'
        ]
        mock_count_rows.side_effect = [100, 150]  # Two files with different row counts

        mock_ti = MagicMock()

        # Execute
        result = FileKafkaConnectorWriter.count_file_rows(
            bucket='test-bucket',
            file_path_pattern='folder/file-*.parquet',
            has_header=False,
            ti=mock_ti
        )

        # Assert
        assert result == 250  # 100 + 150
        mock_storage_client.assert_called_once()
        mock_client.bucket.assert_called_once_with('test-bucket')
        mock_get_matching_files.assert_called_once_with(
            mock_bucket,
            'folder/file-*.parquet',
            file_extensions=None
        )
        assert mock_count_rows.call_count == 2
        mock_ti.xcom_push.assert_called_once_with(key='file_row_count', value=250)

    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.storage.Client")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.get_matching_files")
    def test_count_file_rows_no_files_found(self, mock_get_matching_files, mock_storage_client):
        """
        Test the count_file_rows static method when no files match the pattern.

        This test verifies that the method correctly handles the case when no files
        are found matching the pattern, returning 0.

        :param mock_get_matching_files: Mock for the get_matching_files function.
        :type mock_get_matching_files: :class:`unittest.mock.MagicMock`

        :param mock_storage_client: Mock for the GCS storage client.
        :type mock_storage_client: :class:`unittest.mock.MagicMock`
        """
        # Setup mocks
        mock_bucket = MagicMock()
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_storage_client.return_value = mock_client

        mock_get_matching_files.return_value = []

        mock_ti = MagicMock()

        # Execute
        result = FileKafkaConnectorWriter.count_file_rows(
            bucket='test-bucket',
            file_path_pattern='folder/nonexistent-*.parquet',
            ti=mock_ti
        )

        # Assert
        assert result == 0
        mock_ti.xcom_push.assert_called_once_with(key='file_row_count', value=0)

    def test_branch_skip_if_empty_with_zero_rows(self):
        """
        Test the branch_skip_if_empty static method when file has zero rows.

        This test verifies that the method correctly returns the skip_task_id
        when the row count is 0.
        """
        # Setup mock TaskInstance
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = 0

        # Execute
        result = FileKafkaConnectorWriter.branch_skip_if_empty(
            skip_task_id='skip_kafka_writer_empty_file',
            continue_task_id='kafka-writer-test',
            ti=mock_ti
        )

        # Assert
        assert result == 'skip_kafka_writer_empty_file'
        mock_ti.xcom_pull.assert_called_once_with(
            task_ids='count_file_rows',
            key='file_row_count'
        )

    def test_branch_skip_if_empty_with_data_rows(self):
        """
        Test the branch_skip_if_empty static method when file has data rows.

        This test verifies that the method correctly returns the continue_task_id
        when the row count is greater than 0.
        """
        # Setup mock TaskInstance
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = 100

        # Execute
        result = FileKafkaConnectorWriter.branch_skip_if_empty(
            skip_task_id='skip_kafka_writer_empty_file',
            continue_task_id='kafka-writer-test',
            ti=mock_ti
        )

        # Assert
        assert result == 'kafka-writer-test'
        mock_ti.xcom_pull.assert_called_once_with(
            task_ids='count_file_rows',
            key='file_row_count'
        )

    def test_branch_skip_if_empty_xcom_not_found(self):
        """
        Test the branch_skip_if_empty static method when XCom value is not found.

        This test verifies that the method defaults to continue_task_id when
        the row count is not found in XCom.
        """
        # Setup mock TaskInstance
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = None

        # Execute
        result = FileKafkaConnectorWriter.branch_skip_if_empty(
            skip_task_id='skip_kafka_writer_empty_file',
            continue_task_id='kafka-writer-test',
            ti=mock_ti
        )

        # Assert - defaults to continue when XCom not found
        assert result == 'kafka-writer-test'

    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.EmptyOperator")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.BranchPythonOperator")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.PythonOperator")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.read_variable_or_file")
    def test_create_branch_operator_tasks(self, mock_read_variable_or_file, mock_python_operator,
                                          mock_branch_operator, mock_empty_operator,
                                          test_gcp_dataproc_data: dict):
        """
        Test the _create_branch_operator_tasks method.

        This test verifies that the method correctly creates the three tasks needed
        for branch operator functionality: count task, branch task, and skip task.

        :param mock_read_variable_or_file: Mock for reading configuration variables.
        :type mock_read_variable_or_file: :class:`unittest.mock.MagicMock`

        :param mock_python_operator: Mock for PythonOperator.
        :type mock_python_operator: :class:`unittest.mock.MagicMock`

        :param mock_branch_operator: Mock for BranchPythonOperator.
        :type mock_branch_operator: :class:`unittest.mock.MagicMock`

        :param mock_empty_operator: Mock for EmptyOperator.
        :type mock_empty_operator: :class:`unittest.mock.MagicMock`

        :param test_gcp_dataproc_data: Dictionary of fixture test data.
        :type test_gcp_dataproc_data: dict
        """
        # Setup
        mock_read_variable_or_file.return_value = test_gcp_dataproc_data

        # Create instance
        writer = FileKafkaConnectorWriter(
            'test_bq_to_kafka_case_1.yaml',
            kafka_writer_config_directory
        )

        # Execute
        count_task, branch_task, skip_task = writer._create_branch_operator_tasks(
            bucket='test-bucket',
            file_path_pattern='folder/file-*.parquet',
            kafka_writer_task_id='kafka-writer-test',
            has_header=False
        )

        # Assert
        mock_python_operator.assert_called_once_with(
            task_id='count_file_rows',
            python_callable=FileKafkaConnectorWriter.count_file_rows,
            op_kwargs={
                'bucket': 'test-bucket',
                'file_path_pattern': 'folder/file-*.parquet',
                'has_header': False,
                'file_type': None
            }
        )

        mock_branch_operator.assert_called_once_with(
            task_id='branch_check_empty_file',
            python_callable=FileKafkaConnectorWriter.branch_skip_if_empty,
            op_kwargs={
                'skip_task_id': 'skip_kafka_writer_empty_file',
                'continue_task_id': 'kafka-writer-test'
            }
        )

        mock_empty_operator.assert_called_once_with(
            task_id='skip_kafka_writer_empty_file',
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )

    @pytest.mark.parametrize("config_file, bigquery_source",
                             [
                                 ("test_bq_to_kafka_with_branch.yaml", True),
                                 ("test_file_to_kafka_with_branch.yaml", False)
                             ])
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.EmptyOperator")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.BranchPythonOperator")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.PythonOperator")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.DataprocSubmitJobOperator")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.DataprocCreateClusterOperator")
    @patch("file_kafka_connector_loader.file_kafka_connector_writer_base.read_variable_or_file")
    def test_kafka_writer_dag_with_branch_operator(self,
                                                   mock_read_variable_or_file,
                                                   mock_dataproc_create_cluster_operator,
                                                   mock_dataproc_submit_job_operator,
                                                   mock_python_operator,
                                                   mock_branch_operator,
                                                   mock_empty_operator,
                                                   config_file: str,
                                                   bigquery_source: bool,
                                                   test_gcp_dataproc_data: dict):
        """
        Test Kafka writer DAG creation with branch operator enabled.

        This test verifies that when skip_kafka_writer_if_empty is enabled in the config,
        the DAG is created with the proper branch operator tasks and dependencies.

        :param mock_read_variable_or_file: Mock for reading configuration variables.
        :type mock_read_variable_or_file: :class:`unittest.mock.MagicMock`

        :param mock_dataproc_create_cluster_operator: Mock for DataprocCreateClusterOperator.
        :type mock_dataproc_create_cluster_operator: :class:`unittest.mock.MagicMock`

        :param mock_dataproc_submit_job_operator: Mock for DataprocSubmitJobOperator.
        :type mock_dataproc_submit_job_operator: :class:`unittest.mock.MagicMock`

        :param mock_python_operator: Mock for PythonOperator.
        :type mock_python_operator: :class:`unittest.mock.MagicMock`

        :param mock_branch_operator: Mock for BranchPythonOperator.
        :type mock_branch_operator: :class:`unittest.mock.MagicMock`

        :param mock_empty_operator: Mock for EmptyOperator.
        :type mock_empty_operator: :class:`unittest.mock.MagicMock`

        :param config_file: Config file to use for testing.
        :type config_file: str

        :param bigquery_source: Whether the source is BigQuery.
        :type bigquery_source: bool

        :param test_gcp_dataproc_data: Dictionary of fixture test data.
        :type test_gcp_dataproc_data: dict
        """
        # Setup mocks
        mock_read_variable_or_file.return_value = test_gcp_dataproc_data

        # Initiate DAG creation based on config file
        globals().update(
            FileKafkaConnectorWriter(
                config_file,
                kafka_writer_config_directory
            ).create_dags()
        )

        # Verify reading environment variables is done twice
        assert mock_read_variable_or_file.call_count == 2

        # Verify the Dataproc Create Cluster Operator is called once
        mock_dataproc_create_cluster_operator.assert_called_once_with(
            task_id="create_cluster",
            project_id=test_gcp_dataproc_data['project_id'],
            cluster_config=get_cluster_config_by_job_size(
                test_gcp_dataproc_data['deployment_environment_name'],
                test_gcp_dataproc_data['network_tag'],
                "large"
            ),
            region=test_gcp_dataproc_data['location'],
            cluster_name="{{ dag_run.conf.get('cluster_name')  or 'test-cluster-name' }}"
        )

        # Verify the Dataproc Submit Job Operator is called with trigger_rule for branch
        mock_dataproc_submit_job_operator.assert_called_once()
        call_kwargs = mock_dataproc_submit_job_operator.call_args[1]
        assert call_kwargs['task_id'] == 'kafka-writer-test'
        assert call_kwargs['trigger_rule'] == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        assert call_kwargs['region'] == test_gcp_dataproc_data['location']
        assert call_kwargs['project_id'] == test_gcp_dataproc_data['project_id']
        assert call_kwargs['gcp_conn_id'] == test_gcp_dataproc_data['processing_zone_connection_id']

        # Verify branch operator tasks are created
        if bigquery_source:
            # For BigQuery source, expect: delete_gcs, query_bigquery, count_file_rows
            # PythonOperator calls: delete_gcs, query_bigquery, count_file_rows = 3 calls
            assert mock_python_operator.call_count >= 3

            # Check that count_file_rows task was created
            count_task_calls = [
                c for c in mock_python_operator.call_args_list
                if c[1].get('task_id') == 'count_file_rows'
            ]
            assert len(count_task_calls) == 1
            assert count_task_calls[0][1]['python_callable'] == FileKafkaConnectorWriter.count_file_rows
        else:
            # For file source, expect: count_file_rows = 1 call
            assert mock_python_operator.call_count >= 1

            # Check that count_file_rows task was created
            count_task_calls = [
                c for c in mock_python_operator.call_args_list
                if c[1].get('task_id') == 'count_file_rows'
            ]
            assert len(count_task_calls) == 1

        # Verify branch operator is created
        mock_branch_operator.assert_called_once_with(
            task_id='branch_check_empty_file',
            python_callable=FileKafkaConnectorWriter.branch_skip_if_empty,
            op_kwargs={
                'skip_task_id': 'skip_kafka_writer_empty_file',
                'continue_task_id': 'kafka-writer-test'
            }
        )

        # Verify EmptyOperator is called for start, end, and skip tasks
        # Expected calls: start, end, skip_kafka_writer_empty_file
        assert mock_empty_operator.call_count >= 3

        # Check that skip task was created with correct trigger rule
        skip_task_calls = [
            c for c in mock_empty_operator.call_args_list
            if c[1].get('task_id') == 'skip_kafka_writer_empty_file'
        ]
        assert len(skip_task_calls) == 1
        assert skip_task_calls[0][1]['trigger_rule'] == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
