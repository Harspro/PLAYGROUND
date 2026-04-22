from dataclasses import dataclass
from datetime import date, datetime as datetime_time
from typing import Final, List, Tuple
from unittest.mock import patch, MagicMock

import pytest
import pytz
from google.cloud.bigquery import SqlTypeNames
from google.cloud.exceptions import NotFound

import util.constants as consts
from util.bq_utils import build_back_up_job, get_table_columns, copy_table, load_records_to_bq, get_bq_schema, \
    create_bq_table_if_not_exists, apply_deduplication_transformation, schema_preserving_load, \
    parse_table_fqdn, get_table_via_rest, patch_table_via_rest, BigQueryRestApiError


@pytest.fixture
def mock_bq_table() -> MagicMock:
    mock_table = MagicMock()
    field1 = MagicMock()
    field1.name = 'col1'
    field2 = MagicMock()
    field2.name = 'col2'
    field3 = MagicMock()
    field3.name = 'col3'
    mock_table.schema = [field1, field2, field3]

    return mock_table


@patch("google.cloud.bigquery.Client")
def test_build_back_up_job(mock_bigquery_client, mock_bq_table):
    bigquery_client = MagicMock()
    mock_bigquery_client.return_value = bigquery_client

    bigquery_client.get_table.return_value = mock_bq_table
    build_back_up_job('test_proj.test_dataset.test_table', True)

    bigquery_client.get_table.assert_called_once_with('test_proj.test_dataset.test_table_BACK_UP')


def test_get_table_columns(mock_bq_table):
    bigquery_client = MagicMock()

    bigquery_client.get_table.return_value = mock_bq_table

    result = get_table_columns(bigquery_client, 'test_proj.test_dataset.test_table')

    assert result.get(consts.ID) == 'test_proj.test_dataset.test_table'
    assert result.get(consts.COLUMNS) == 'col1, col2, col3'


@patch("google.cloud.bigquery.Client")
def test_copy_table(mock_bigquery_client):
    bigquery_client = MagicMock()
    mock_bigquery_client.return_value = bigquery_client

    bq_job = MagicMock()
    bigquery_client.query.return_value = bq_job
    bq_job.result.return_value = None

    copy_table('test_proj.test_dataset.source_table', 'test_proj.test_dataset.target_table')

    bigquery_client.query.assert_called_once()


@patch('util.bq_utils.is_bq_query_done')
@patch('google.cloud.bigquery.Client')
def test_create_bq_table_if_not_exists(
        mock_bq_client,
        mock_is_bq_query_done
):
    mock_is_bq_query_done.return_value = True
    schema = {'name': 'STRING', 'value': 'INT64'}

    mock_bq_client_instance = mock_bq_client.return_value
    mock_query_job = MagicMock()
    mock_bq_client_instance.query.return_value = mock_query_job

    create_bq_table_if_not_exists('project.dataset.table', schema)

    expected_query = (
        "CREATE TABLE IF NOT EXISTS `project.dataset.table` "
        "( name STRING , value INT64 ) ;"
    )

    mock_bq_client_instance.query.assert_called_once_with(expected_query)


@patch('util.bq_utils.submit_transformation')
def test_apply_deduplication_transformation(mock_submit_transformation):
    bigquery_client = MagicMock()
    source_table = {
        consts.ID: 'test_proj.test_dataset.test_table',
        consts.COLUMNS: 'col1, col2, col3'
    }
    dedup_columns = ['col1', 'col2']
    expiration_hours = 8

    expected_result = {
        consts.ID: 'test_proj.test_dataset.test_table_DEDUP_TF',
        consts.COLUMNS: 'col1, col2, col3'
    }
    mock_submit_transformation.return_value = expected_result

    result = apply_deduplication_transformation(
        bigquery_client,
        source_table,
        dedup_columns,
        expiration_hours
    )

    mock_submit_transformation.assert_called_once()
    call_args = mock_submit_transformation.call_args

    assert call_args[0][0] == bigquery_client  # bigquery_client
    assert call_args[0][1] == 'test_proj.test_dataset.test_table_DEDUP_TF'  # view_id

    ddl = call_args[0][2]  # view_ddl
    assert 'CREATE OR REPLACE VIEW' in ddl
    assert 'test_proj.test_dataset.test_table_DEDUP_TF' in ddl
    assert 'ROW_NUMBER() OVER (PARTITION BY col1, col2)' in ddl
    assert 'col1, col2, col3' in ddl
    assert 'test_proj.test_dataset.test_table' in ddl
    assert 'INTERVAL 8 HOUR' in ddl
    assert 'WHERE rn = 1' in ddl

    assert result == expected_result


def test_apply_deduplication_transformation_empty_columns():
    """Test that apply_deduplication_transformation returns source_table when dedup_columns is empty"""
    bigquery_client = MagicMock()
    source_table = {
        consts.ID: 'test_proj.test_dataset.test_table',
        consts.COLUMNS: 'col1, col2, col3'
    }
    dedup_columns = []
    expiration_hours = 8

    result = apply_deduplication_transformation(
        bigquery_client,
        source_table,
        dedup_columns,
        expiration_hours
    )

    # Should return the original source_table unchanged
    assert result == source_table


@patch('util.bq_utils.table_exists')
@patch('google.cloud.bigquery.Client')
def test_schema_preserving_load_table_exists(mock_bigquery_client, mock_table_exists):
    """Test that schema_preserving_load executes TRUNCATE and INSERT when table exists"""
    bigquery_client = MagicMock()
    mock_bigquery_client.return_value = bigquery_client
    mock_table_exists.return_value = True

    create_ddl = "CREATE OR REPLACE TABLE `test_proj.test_dataset.test_table` AS SELECT * FROM source"
    insert_stmt = "INSERT INTO `test_proj.test_dataset.test_table` SELECT * FROM source"
    target_table_id = "test_proj.test_dataset.test_table"

    schema_preserving_load(create_ddl, insert_stmt, target_table_id, bq_client=bigquery_client)

    # Verify table_exists was called
    mock_table_exists.assert_called_once_with(bigquery_client, target_table_id)

    # Verify that query_and_wait was called twice (once for TRUNCATE, once for INSERT)
    assert bigquery_client.query_and_wait.call_count == 2

    # Verify TRUNCATE query
    truncate_call = bigquery_client.query_and_wait.call_args_list[0]
    assert truncate_call[0][0] == f"TRUNCATE TABLE `{target_table_id}`"

    # Verify INSERT query
    insert_call = bigquery_client.query_and_wait.call_args_list[1]
    assert insert_call[0][0] == insert_stmt


@patch('util.bq_utils.table_exists')
@patch('google.cloud.bigquery.Client')
def test_schema_preserving_load_table_not_exists(mock_bigquery_client, mock_table_exists):
    """Test that schema_preserving_load executes CREATE OR REPLACE when table doesn't exist"""
    bigquery_client = MagicMock()
    mock_bigquery_client.return_value = bigquery_client
    mock_table_exists.return_value = False

    create_ddl = "CREATE OR REPLACE TABLE `test_proj.test_dataset.test_table` AS SELECT * FROM source"
    insert_stmt = "INSERT INTO `test_proj.test_dataset.test_table` SELECT * FROM source"
    target_table_id = "test_proj.test_dataset.test_table"

    schema_preserving_load(create_ddl, insert_stmt, target_table_id, bq_client=bigquery_client)

    # Verify table_exists was called
    mock_table_exists.assert_called_once_with(bigquery_client, target_table_id)

    # Verify that query_and_wait was called once (only for CREATE OR REPLACE)
    assert bigquery_client.query_and_wait.call_count == 1

    # Verify CREATE OR REPLACE query
    create_call = bigquery_client.query_and_wait.call_args_list[0]
    assert create_call[0][0] == create_ddl


@patch('util.bq_utils.table_exists')
@patch('google.cloud.bigquery.Client')
def test_schema_preserving_load_creates_client_when_not_provided(mock_bigquery_client, mock_table_exists):
    """Test that schema_preserving_load creates a new client when bq_client is not provided"""
    bigquery_client = MagicMock()
    mock_bigquery_client.return_value = bigquery_client
    mock_table_exists.return_value = False

    create_ddl = "CREATE OR REPLACE TABLE `test_proj.test_dataset.test_table` AS SELECT * FROM source"
    insert_stmt = "INSERT INTO `test_proj.test_dataset.test_table` SELECT * FROM source"
    target_table_id = "test_proj.test_dataset.test_table"

    schema_preserving_load(create_ddl, insert_stmt, target_table_id)

    # Verify that a new client was created
    mock_bigquery_client.assert_called_once()

    # Verify table_exists was called with the created client
    mock_table_exists.assert_called_once_with(bigquery_client, target_table_id)


class TestLoadRecordsToBQ:
    @dataclass
    class SampleDataClass:
        name: str
        value: int
        date_field: date
        datetime_field: datetime_time

    @dataclass
    class SampleDataClassTwo:
        height: float
        is_ambidextrous: bool
        current_time: datetime_time

    LOAD_RECORDS_TO_BQ_TEST_CASES: Final[List[Tuple[dataclass, dict, bool]]] = [
        [
            [
                SampleDataClass(name="test1", value=100, date_field=date(2023, 1, 1),
                                datetime_field=datetime_time.now()),
                SampleDataClass(name="test2", value=200, date_field=date(2023, 1, 2),
                                datetime_field=datetime_time.now())
            ],
            {
                'name': SqlTypeNames.STRING,
                'value': SqlTypeNames.INT64,
                'date_field': SqlTypeNames.DATE,
                'datetime_field': SqlTypeNames.DATETIME
            },
            False
        ],
        [
            [
                SampleDataClassTwo(height=173.94, is_ambidextrous=True,
                                   current_time=datetime_time.now(tz=pytz.timezone('America/Toronto')))
            ],
            {
                'height': SqlTypeNames.FLOAT64,
                'is_ambidextrous': SqlTypeNames.BOOL,
                'current_time': SqlTypeNames.TIMESTAMP
            },
            True
        ]
    ]

    @pytest.mark.parametrize("sample_list_of_dataclass, rows_schema, is_timestamp", LOAD_RECORDS_TO_BQ_TEST_CASES)
    @patch('dataclasses.is_dataclass')
    def test_get_bq_schema(
            self,
            mock_is_dataclass,
            rows_schema,
            sample_list_of_dataclass,
            is_timestamp
    ):
        mock_is_dataclass.return_value = True
        schema = get_bq_schema(sample_list_of_dataclass[0], is_timestamp)
        assert schema == rows_schema

    @pytest.mark.parametrize("sample_list_of_dataclass, rows_schema, is_timestamp", LOAD_RECORDS_TO_BQ_TEST_CASES)
    @patch('util.bq_utils.get_bq_schema')
    @patch('util.bq_utils.create_bq_table_if_not_exists')
    @patch('google.cloud.bigquery.Client')
    @patch('util.bq_utils.is_bq_query_done')
    def test_load_records_to_bq(
            self,
            mock_is_bq_query_done,
            mock_bq_client,
            mock_create_bq_table_if_not_exists,
            mock_get_bq_schema,
            sample_list_of_dataclass,
            rows_schema,
            is_timestamp
    ):
        mock_bq_client_instance = mock_bq_client.return_value
        mock_bq_client_instance.load_table_from_dataframe.return_value = MagicMock()

        mock_get_bq_schema.return_value = rows_schema
        mock_is_bq_query_done.return_value = True

        load_records_to_bq(table_id='project.dataset.table', records=sample_list_of_dataclass, timestamp=is_timestamp)

        mock_get_bq_schema.assert_called_once_with(sample_list_of_dataclass[0], timestamp=is_timestamp)
        mock_create_bq_table_if_not_exists.assert_called_once_with('project.dataset.table', schema=rows_schema)
        mock_bq_client_instance.load_table_from_dataframe.assert_called_once()


# ==========================================================
# REST API Helpers (get_table_via_rest, patch_table_via_rest)
# ==========================================================

def test_parse_table_fqdn_valid():
    project, dataset, table = parse_table_fqdn("proj.ds.tbl")
    assert project == "proj"
    assert dataset == "ds"
    assert table == "tbl"


def test_parse_table_fqdn_invalid():
    with pytest.raises(ValueError, match="table_fqdn must be 'project.dataset.table'"):
        parse_table_fqdn("invalid_format")


@patch("util.bq_utils.make_bq_api_request")
def test_get_table_via_rest_success(mock_request):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.ok = True
    mock_response.json.return_value = {"id": "test-table"}
    mock_request.return_value = mock_response

    result = get_table_via_rest("proj.ds.tbl")
    assert result == {"id": "test-table"}
    mock_request.assert_called_once()
    assert mock_request.call_args[0][0] == "GET"


@patch("util.bq_utils.make_bq_api_request")
def test_get_table_via_rest_not_found(mock_request):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.ok = False
    mock_request.return_value = mock_response

    with pytest.raises(NotFound):
        get_table_via_rest("proj.ds.tbl")


@patch("util.bq_utils.make_bq_api_request")
def test_get_table_via_rest_error(mock_request):
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.ok = False
    mock_request.return_value = mock_response

    with pytest.raises(BigQueryRestApiError, match="BigQuery API request failed with status 500"):
        get_table_via_rest("proj.ds.tbl")


@patch("util.bq_utils.make_bq_api_request")
def test_patch_table_via_rest_success(mock_request):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.ok = True
    mock_response.json.return_value = {"id": "test-table", "description": "updated"}
    mock_request.return_value = mock_response

    body = {"description": "updated"}
    result = patch_table_via_rest("proj.ds.tbl", body)
    assert result["description"] == "updated"
    mock_request.assert_called_once()
    assert mock_request.call_args[0][0] == "PATCH"
    assert mock_request.call_args[1]["json"] == body


@patch("util.bq_utils.make_bq_api_request")
def test_patch_table_via_rest_with_etag(mock_request):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.ok = True
    mock_request.return_value = mock_response

    patch_table_via_rest("proj.ds.tbl", {"desc": "x"}, etag="etag123")
    headers = mock_request.call_args[1]["headers"]
    assert headers["If-Match"] == "etag123"
