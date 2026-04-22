from unittest.mock import patch
import pytest
import util.constants as consts
from datetime import datetime
from tsys_processing.generic_file_loader import GenericFileLoader
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowFailException
from unittest.mock import MagicMock
import pandas as pd
from util.miscutils import get_file_date_parameter


@pytest.fixture
def test_gcp_dataproc_data() -> dict:
    """
    Pytest fixture providing test data for Airflow environment variables
    :returns: A dictionary containing test data for Airflow environment variables
    :rtype: dict
    """
    return {
        'deployment_environment_name': 'test-deployment-environment',
        'network_tag': 'test-network-tag',
        'processing_zone_connection_id': 'google_cloud_default',
        'location': 'northamerica-northeast1',
        'project_id': 'test-project'
    }


@pytest.mark.parametrize("join_type",
                         [
                             None,
                             "RIGHT JOIN"
                         ])
@patch("tsys_processing.tsys_file_loader_base.read_variable_or_file")
@patch("tsys_processing.tsys_file_loader_base.read_yamlfile_env")
@patch("tsys_processing.generic_file_loader.create_external_table")
@patch("tsys_processing.generic_file_loader.apply_timestamp_transformation")
@patch("tsys_processing.generic_file_loader.table_exists")
@patch("util.bq_utils.table_exists")
@patch("tsys_processing.generic_file_loader.apply_schema_sync_transformation")
@patch("tsys_processing.generic_file_loader.apply_join_transformation")
def test_apply_transformations_join_types(mock_apply_join_transformation,
                                          mock_apply_schema_sync_transformation,
                                          mock_table_exists,
                                          mock_util_table_exists,
                                          mock_apply_timestamp_transformation,
                                          mock_create_external_table,
                                          mock_read_yamlfile_env,
                                          mock_read_variable_or_file,
                                          join_type: str,
                                          test_gcp_dataproc_data: dict):

    mock_read_variable_or_file.return_value = test_gcp_dataproc_data
    mock_table_exists.return_value = False
    mock_util_table_exists.return_value = False
    mock_create_external_table.return_value = {'id': 'test_project.test_dataset.test_table_id'}

    generic_file_loader = GenericFileLoader('dummy.yaml')

    join_config = {'table_name': 'test_join_table',
                   'join_type': join_type,
                   'join_columns': [{'from_col': 'test_from_columns',
                                     'to_col': 'test_to_col'}],
                   'output_columns': ['test_output_columns']}

    if not join_type:
        del join_config['join_type']

    transformation_config = {'join_spec': [join_config],
                             'destination_table': {'id': 'test_table'}}

    generic_file_loader.apply_transformations(bigquery_client=None,
                                              transformation_config=transformation_config)

    args, kwargs = mock_apply_join_transformation.call_args

    join_clause = args[2].get(consts.JOIN_CLAUSE)

    if join_type:
        assert join_clause.startswith(join_type)
    else:
        assert join_clause.startswith(consts.LEFT_JOIN)


def test_trigger_dag_operator_created():
    dag_id = "test_dag"
    dag_config = {'trigger_flag': True}
    generic_loader = GenericFileLoader("dummy.yaml")
    generic_loader.job_config = {dag_id: {'trigger_oracle_dag_id': 'target_dag'}}

    operator = generic_loader.trigger_bq_to_oracle_dag(dag_id, dag_config)

    assert isinstance(operator, TriggerDagRunOperator)
    assert operator.task_id.startswith("trigger_")


def test_duplicate_trailer_check_raises_for_duplicates():
    loader = GenericFileLoader("dummy.yaml")

    mock_client = MagicMock()
    mock_query = mock_client.query.return_value
    mock_query.result.return_value.to_dataframe.return_value = pd.DataFrame({'col1': ['val']})

    with patch("google.cloud.bigquery.Client", return_value=mock_client):
        with pytest.raises(AirflowFailException, match="Duplicate entry with these details"):
            loader.duplicate_trailer_check(
                bigquery_config={
                    consts.TABLES: {
                        "TRLR": {consts.DUPLICATE_CHECK_COLS: ["col1"]}
                    }
                },
                transformation_config={
                    consts.DESTINATION_TABLE: {consts.ID: "target_table"}
                },
                transformed_view={consts.ID: "source_table"},
                segment_name="TRLR"
            )


@patch('tsys_processing.generic_file_loader.apply_deduplication_transformation')
@patch('tsys_processing.generic_file_loader.apply_schema_sync_transformation')
@patch('tsys_processing.generic_file_loader.apply_timestamp_transformation')
@patch('tsys_processing.generic_file_loader.apply_column_transformation')
@patch('tsys_processing.generic_file_loader.table_exists')
@patch('tsys_processing.generic_file_loader.create_external_table')
@patch('tsys_processing.tsys_file_loader_base.read_variable_or_file')
@patch('tsys_processing.tsys_file_loader_base.read_yamlfile_env')
def test_apply_transformations_with_deduplication(mock_read_yamlfile_env,
                                                  mock_read_variable_or_file,
                                                  mock_create_external_table,
                                                  mock_table_exists,
                                                  mock_apply_column_transformation,
                                                  mock_apply_timestamp_transformation,
                                                  mock_apply_schema_sync_transformation,
                                                  mock_apply_deduplication_transformation,
                                                  test_gcp_dataproc_data: dict):
    """Test that deduplication transformation is applied when deduplication columns are provided."""

    mock_read_variable_or_file.return_value = test_gcp_dataproc_data
    mock_create_external_table.return_value = {consts.ID: 'test_project.test_dataset.test_table_id'}
    mock_apply_column_transformation.return_value = {consts.ID: 'test_project.test_dataset.test_table_column'}
    mock_apply_timestamp_transformation.return_value = {consts.ID: 'test_project.test_dataset.test_table_timestamp'}
    mock_table_exists.return_value = True
    mock_apply_schema_sync_transformation.return_value = {consts.ID: 'test_project.test_dataset.test_table_schema'}
    mock_apply_deduplication_transformation.return_value = {consts.ID: 'test_project.test_dataset.test_table_dedup'}

    generic_file_loader = GenericFileLoader('dummy.yaml')

    transformation_config = {
        consts.EXTERNAL_TABLE_ID: 'test_project.test_dataset.external_table',
        consts.DATA_FILE_LOCATION: 'gs://test-bucket/test-file.csv',
        consts.DESTINATION_TABLE: {consts.ID: 'test_project.test_dataset.target_table'},
        consts.DEDUPLICATION_COLUMNS: ['col1', 'col2', 'col3'],
        consts.JOIN_SPECIFICATION: []
    }

    mock_bigquery_client = MagicMock()

    generic_file_loader.apply_transformations(
        bigquery_client=mock_bigquery_client,
        transformation_config=transformation_config
    )

    mock_apply_deduplication_transformation.assert_called_once()
    call_args = mock_apply_deduplication_transformation.call_args

    assert call_args[0][0] == mock_bigquery_client
    assert call_args[0][1] == {consts.ID: 'test_project.test_dataset.test_table_timestamp'}
    assert call_args[0][2] == ['col1', 'col2', 'col3']

    # Verify apply_schema_sync_transformation is called
    mock_apply_schema_sync_transformation.assert_called_once()
    schema_call_args = mock_apply_schema_sync_transformation.call_args
    assert schema_call_args[0][0] == mock_bigquery_client
    assert schema_call_args[0][1] == {consts.ID: 'test_project.test_dataset.test_table_dedup'}
    assert schema_call_args[0][2] == 'test_project.test_dataset.target_table'


@patch('tsys_processing.generic_file_loader.apply_deduplication_transformation')
@patch('tsys_processing.generic_file_loader.apply_schema_sync_transformation')
@patch('tsys_processing.generic_file_loader.apply_timestamp_transformation')
@patch('tsys_processing.generic_file_loader.apply_column_transformation')
@patch('tsys_processing.generic_file_loader.table_exists')
@patch('tsys_processing.generic_file_loader.create_external_table')
@patch('tsys_processing.tsys_file_loader_base.read_variable_or_file')
@patch('tsys_processing.tsys_file_loader_base.read_yamlfile_env')
def test_apply_transformations_without_deduplication(mock_read_yamlfile_env,
                                                     mock_read_variable_or_file,
                                                     mock_create_external_table,
                                                     mock_table_exists,
                                                     mock_apply_column_transformation,
                                                     mock_apply_timestamp_transformation,
                                                     mock_apply_schema_sync_transformation,
                                                     mock_apply_deduplication_transformation,
                                                     test_gcp_dataproc_data: dict):
    """Test that deduplication transformation is NOT applied when no deduplication columns are provided."""

    mock_read_variable_or_file.return_value = test_gcp_dataproc_data
    mock_create_external_table.return_value = {consts.ID: 'test_project.test_dataset.test_table_id'}
    mock_apply_column_transformation.return_value = {consts.ID: 'test_project.test_dataset.test_table_column'}
    mock_apply_timestamp_transformation.return_value = {consts.ID: 'test_project.test_dataset.test_table_timestamp'}
    mock_table_exists.return_value = True
    mock_apply_schema_sync_transformation.return_value = {consts.ID: 'test_project.test_dataset.test_table_schema'}

    generic_file_loader = GenericFileLoader('dummy.yaml')

    transformation_config = {
        consts.EXTERNAL_TABLE_ID: 'test_project.test_dataset.external_table',
        consts.DATA_FILE_LOCATION: 'gs://test-bucket/test-file.csv',
        consts.DESTINATION_TABLE: {consts.ID: 'test_project.test_dataset.target_table'},
        consts.JOIN_SPECIFICATION: []
    }

    mock_bigquery_client = MagicMock()

    generic_file_loader.apply_transformations(
        bigquery_client=mock_bigquery_client,
        transformation_config=transformation_config
    )

    mock_apply_deduplication_transformation.assert_not_called()

    # Verify apply_schema_sync_transformation is called
    mock_apply_schema_sync_transformation.assert_called_once()
    schema_call_args = mock_apply_schema_sync_transformation.call_args
    assert schema_call_args[0][0] == mock_bigquery_client
    assert schema_call_args[0][1] == {consts.ID: 'test_project.test_dataset.test_table_timestamp'}
    assert schema_call_args[0][2] == 'test_project.test_dataset.target_table'
