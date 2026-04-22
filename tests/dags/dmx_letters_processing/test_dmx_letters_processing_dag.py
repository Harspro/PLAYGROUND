import os
import yaml
from unittest.mock import patch, call, MagicMock
import pytest
from airflow.exceptions import AirflowFailException

from dmx_letters_processing.dmx_letters_processing_dag import DmxLettersDagBuilder
from dag_factory.environment_config import EnvironmentConfig

import util.constants as consts
from util.miscutils import read_yamlfile_env

current_script_directory = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = f"{current_script_directory}/config/test_config.yaml"


def dag_builder_fixture():
    """Fixture providing configured DAG builder instance."""
    # Create a mock or minimal EnvironmentConfig
    environment_config = MagicMock()
    environment_config.gcp_config = {
        'processing_zone_project_id': 'pcb-dev-processing',
        'landing_zone_project_id': 'pcb-dev-landing',
        'deploy_env_storage_suffix': '-dev'
    }
    # Use a proper timezone object instead of MagicMock
    from pendulum import timezone
    environment_config.local_tz = timezone('America/Toronto')
    environment_config.storage_suffix = '-dev'
    environment_config.deploy_env = 'test'

    builder = DmxLettersDagBuilder(environment_config)

    builder.dag_config = read_yamlfile_env(f"{current_script_directory}/config/test_config.yaml")

    return builder


def test_create_unique_folder_path():
    """Test the create_unique_folder_path method"""
    instance = dag_builder_fixture()

    mock_dag_run = MagicMock()
    mock_dag_run.run_id = "scheduled__2024-01-15T08:00:00+00:00"

    kwargs = {
        'dag_run': mock_dag_run
    }

    result = instance.create_unique_folder_path(**kwargs)

    expected = "scheduled__2024_01_15T08_00_00_00_00"
    assert result == expected


def test_create_unique_folder_path_with_special_characters():
    """Test the create_unique_folder_path method with various special characters"""
    instance = dag_builder_fixture()

    mock_dag_run = MagicMock()
    mock_dag_run.run_id = "manual__2024-01-15T08:00:00+00:00-test@dag#run$"

    kwargs = {
        'dag_run': mock_dag_run
    }

    result = instance.create_unique_folder_path(**kwargs)

    # All special characters should be replaced with underscores
    expected = "manual__2024_01_15T08_00_00_00_00_test_dag_run_"
    assert result == expected


@patch("dmx_letters_processing.dmx_letters_processing_dag.run_bq_query")
@patch("dmx_letters_processing.dmx_letters_processing_dag.bigquery.Client")
@patch("dmx_letters_processing.dmx_letters_processing_dag.create_external_table")
@patch("dmx_letters_processing.dmx_letters_processing_dag.apply_column_transformation")
def test_load_parquet_to_bq(mock_apply_column_transformation,
                            mock_create_external_table,
                            mock_bigquery_client,
                            mock_run_bq_query):
    """Test the load_parquet_to_bq method with column transformations"""
    instance = dag_builder_fixture()

    bq_config = instance.dag_config['dag']['bigquery']

    mock_client = MagicMock()
    mock_bigquery_client.return_value = mock_client
    mock_target_table = MagicMock()
    mock_target_table.num_rows = 100
    mock_client.get_table.return_value = mock_target_table

    mock_transformed_data = {
        consts.ID: 'pcb-test-processing.domain_communication.MAIL_PRINT_EXT',
        consts.COLUMNS: 'source, mailType, printingMode, parameters, timestamp'
    }
    mock_create_external_table.return_value = mock_transformed_data
    mock_apply_column_transformation.return_value = mock_transformed_data

    unique_folder_path = "test_unique_folder_123"
    instance.load_parquet_to_bq(bq_config, unique_folder_path)

    mock_bigquery_client.assert_called_once()

    mock_create_external_table.assert_called_once_with(
        mock_client,
        f"{instance.environment_config.gcp_config.get(consts.PROCESSING_ZONE_PROJECT_ID)}.domain_communication.MAIL_PRINT_EXT",
        "gs://pcb-test-staging-extract/dmx_letters/test_unique_folder_123/*.parquet"
    )

    expected_add_columns = [
        "'DMX RRDF' AS source",
        "'LETTER' AS mailType",
        "'SIMPLEX' AS printingMode",
        "SAFE.PARSE_JSON(NULLIF(TRIM(parametersString), '')) AS parameters",
        "TIMESTAMP(CURRENT_DATETIME('America/Toronto')) AS timestamp"
    ]
    expected_drop_columns = ['parametersString']

    mock_apply_column_transformation.assert_called_once_with(
        mock_client,
        mock_transformed_data,
        expected_add_columns,
        expected_drop_columns
    )

    expected_ddl = """
                INSERT INTO `pcb-test-landing.domain_communication.MAIL_PRINT` (source, mailType, printingMode, parameters, timestamp)
                SELECT source, mailType, printingMode, parameters, timestamp
                FROM `pcb-test-processing.domain_communication.MAIL_PRINT_EXT`;
            """
    mock_run_bq_query.assert_called_once_with(expected_ddl)

    mock_client.get_table.assert_called_once_with("pcb-test-landing.domain_communication.MAIL_PRINT")


@patch("dmx_letters_processing.dmx_letters_processing_dag.bigquery.Client")
@patch("dmx_letters_processing.dmx_letters_processing_dag.create_external_table")
def test_load_parquet_to_bq_error_handling(mock_create_external_table,
                                           mock_bigquery_client):
    """Test the load_parquet_to_bq method error handling"""
    instance = dag_builder_fixture()

    bq_config = instance.dag_config['dag']['bigquery']

    # Mock external table creation to raise an exception
    mock_create_external_table.side_effect = Exception("External table creation failed")

    unique_folder_path = "test_unique_folder_123"

    with pytest.raises(AirflowFailException) as exc_info:
        instance.load_parquet_to_bq(bq_config, unique_folder_path)

    assert "Failed to load parquet data to BigQuery" in str(exc_info.value)
    mock_bigquery_client.assert_called_once()
    mock_create_external_table.assert_called_once()


def test_build_spark_job():
    """Test the build_spark_job method"""
    instance = dag_builder_fixture()
    instance.default_args = instance.dag_config['default_args']

    spark_config = instance.dag_config['dag']['spark']

    cluster_name = "test-cluster"
    input_bucket_name = "test-input-bucket"
    input_folder_name = "test-folder"
    input_file_name = "test-file.json"
    unique_folder_path = "test_unique_folder_123"

    with patch("dmx_letters_processing.dmx_letters_processing_dag.build_spark_logging_info") as mock_build_spark_logging:
        mock_build_spark_logging.return_value = [
            "input.file.path=gs://test-input-bucket/test-folder/test-file.json",
            "output.file.path=gs://pcb-test-staging-extract/dmx_letters/test_unique_folder_123",
            "output.file.format=parquet",
            "logging_key=logging_value"
        ]

        result = instance.build_spark_job(
            cluster_name,
            input_bucket_name,
            input_folder_name,
            input_file_name,
            unique_folder_path,
            spark_config
        )

    expected_spark_job = {
        consts.REFERENCE: {consts.PROJECT_ID: instance.environment_config.dataproc_config.get(consts.PROJECT_ID)},
        consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
        consts.SPARK_JOB: {
            consts.JAR_FILE_URIS: spark_config[consts.JAR_FILE_URIS],
            consts.MAIN_CLASS: spark_config[consts.MAIN_CLASS],
            consts.ARGS: [
                "input.file.path=gs://test-input-bucket/test-folder/test-file.json",
                "output.file.path=gs://pcb-test-staging-extract/dmx_letters/test_unique_folder_123",
                "output.file.format=parquet",
                "logging_key=logging_value"
            ]
        }
    }

    assert result == expected_spark_job
    mock_build_spark_logging.assert_called_once_with(
        dag_id='{{dag.dag_id}}',
        default_args=instance.default_args,
        arg_list=[
            'input.file.path=gs://test-input-bucket/test-folder/test-file.json',
            'output.file.path=gs://pcb-test-staging-extract/dmx_letters/test_unique_folder_123',
            'output.file.format=parquet'
        ]
    )


@patch("dmx_letters_processing.dmx_letters_processing_dag.EmptyOperator")
@patch("dmx_letters_processing.dmx_letters_processing_dag.PythonOperator")
@patch("dmx_letters_processing.dmx_letters_processing_dag.DataprocSubmitJobOperator")
@patch("dmx_letters_processing.dmx_letters_processing_dag.DataprocCreateClusterOperator")
@patch("dmx_letters_processing.dmx_letters_processing_dag.get_cluster_config_by_job_size")
@patch("dmx_letters_processing.dmx_letters_processing_dag.get_cluster_name_for_dag")
def test_build_dag(mock_get_cluster_name,
                   mock_get_cluster_config,
                   mock_dataproc_create_cluster,
                   mock_dataproc_submit_job,
                   mock_python,
                   mock_empty):
    """Test the build method creates DAG with correct structure and tasks"""
    instance = dag_builder_fixture()

    config = instance.dag_config

    mock_get_cluster_name.return_value = "test-cluster"
    mock_get_cluster_config.return_value = {"test": "config"}

    result_dag = instance.build("test_dag_id", config)

    # Verify DAG properties
    assert result_dag.dag_id == "test_dag_id"
    assert result_dag.description == (
        'This DAG processes DMX RRDF files by loading JSON data, mapping it, '
        'and writing the results to the MAIL_PRINT table in BigQuery using a Dataproc cluster.'
    )
    assert result_dag.catchup is False
    assert result_dag.max_active_tasks == 5
    assert result_dag.max_active_runs == 1
    assert result_dag.is_paused_upon_creation is True
    assert config[consts.TAGS] == result_dag.tags

    mock_empty.assert_has_calls([
        call(task_id=consts.START_TASK_ID),
        call(task_id=consts.END_TASK_ID)
    ])

    mock_dataproc_create_cluster.assert_called_once_with(
        task_id=consts.CLUSTER_CREATING_TASK_ID,
        project_id=instance.environment_config.dataproc_config.get(consts.PROJECT_ID),
        cluster_config={"test": "config"},
        region=instance.environment_config.dataproc_config.get(consts.LOCATION),
        cluster_name="test-cluster"
    )

    mock_dataproc_submit_job.assert_called_once_with(
        task_id="parse_file_and_generate_parquet",
        job={
            consts.REFERENCE: {consts.PROJECT_ID: instance.environment_config.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: "test-cluster"},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: config['dag']['spark']['jar_file_uris'],
                consts.MAIN_CLASS: config['dag']['spark']['main_class'],
                consts.ARGS: [
                    "input.file.path=gs://{{ dag_run.conf['bucket'] }}/{{ dag_run.conf['folder_name'] }}/{{ dag_run.conf['file_name'] }}",
                    "output.file.path=gs://pcb-test-staging-extract/dmx_letters/{{ task_instance.xcom_pull(task_ids='create_unique_folder_path', key='return_value') }}",
                    "output.file.format=parquet",
                    'pcb.data.traceability.dag.info=[dag_name: {{dag.dag_id}}, dag_run_id: {{run_id}}, dag_owner: team-telegraphers-alerts]'
                ]
            }
        },
        region=instance.environment_config.dataproc_config.get(consts.LOCATION),
        project_id=instance.environment_config.dataproc_config.get(consts.PROJECT_ID),
        gcp_conn_id=instance.environment_config.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID)
    )

    mock_python.assert_has_calls([
        call(
            task_id="create_unique_folder_path",
            python_callable=instance.create_unique_folder_path
        ),
        call(
            task_id="load_parquet_to_bq",
            python_callable=instance.load_parquet_to_bq,
            op_kwargs={
                'bq_config': config['dag']['bigquery'],
                'unique_folder_path': "{{ task_instance.xcom_pull(task_ids='create_unique_folder_path', key='return_value') }}"
            }
        )
    ])
