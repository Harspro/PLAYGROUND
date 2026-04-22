import pytest
import os
import tempfile
from unittest.mock import patch, MagicMock
from datetime import datetime

from bigquery_table_management.bq_terraform_table_audit_management_dag import (
    BigQueryTableConfig,
)


@pytest.fixture
def bq_table_config():
    project_id = "pcb-test-landing"
    with patch(
        "bigquery_table_management.bq_terraform_table_audit_management_dag.bigquery.Client",
        autospec=True,
    ) as mock_client_class:
        mock_client = mock_client_class.return_value
        yield BigQueryTableConfig(project_id), mock_client


@pytest.fixture
def bq_table_config_obj():
    # For tests of protected methods (no client access)
    project_id = "pcb-test-landing"
    return BigQueryTableConfig(project_id)


def test_init_fixture(bq_table_config):
    obj, mock_client = bq_table_config
    assert obj.project_id == "pcb-test-landing"


def test_get_table_config_positive(bq_table_config):
    obj, mock_client = bq_table_config
    table_fqn = "pcb-test-landing.test_dataset.test_table"
    mock_schema = "mock_schema"
    mock_partitioning = "mock_partitioning"
    mock_table = MagicMock()
    mock_table.schema = "schema_fields"
    mock_table.clustering_fields = ["col1", "col2"]
    mock_table.modified = datetime(2024, 6, 22, 0, 0)
    mock_client.get_table.return_value = mock_table

    with patch.object(obj, "_extract_fields", return_value=mock_schema), patch.object(
        obj, "_extract_partitioning", return_value=mock_partitioning
    ):
        result = obj.get_table_config(table_fqn)

        assert result[0] == {"columns": mock_schema}
        assert result[1] == {
            "clustering": ["col1", "col2"],
            "partitioning": mock_partitioning,
        }
        assert isinstance(result[2], datetime)
        assert result[2] == datetime(2024, 6, 22, 0, 0)


def test_get_table_config_negative(bq_table_config):
    obj, mock_client = bq_table_config
    bad_fqn = "pcb-test-landing.test_table"  # Only two parts

    with pytest.raises(ValueError) as excinfo:
        obj.get_table_config(bad_fqn)
    assert "Expected format: project.dataset.table" in str(excinfo.value)


def test_extract_fields_positive_simple(bq_table_config_obj):
    # Simple flat schema
    field1 = MagicMock()
    field1.name = "col1"
    field1.field_type = "STRING"
    field1.mode = "NULLABLE"
    field1.default_value_expression = None
    field1.fields = []

    field2 = MagicMock()
    field2.name = "col2"
    field2.field_type = "INTEGER"
    field2.mode = "REQUIRED"
    field2.default_value_expression = "GENERATED ALWAYS"
    field2.fields = []

    fields = [field1, field2]
    result = bq_table_config_obj._extract_fields(fields)

    assert result == [
        {"name": "col1", "type": "STRING", "mode": "NULLABLE"},
        {
            "name": "col2",
            "type": "INTEGER",
            "mode": "REQUIRED",
            "defaultValueExpression": "GENERATED ALWAYS",
        },
    ]


def test_extract_fields_positive_nested_record(bq_table_config_obj):
    # Nested RECORD type
    inner_field = MagicMock()
    inner_field.name = "nested_col"
    inner_field.field_type = "FLOAT"
    inner_field.mode = "NULLABLE"
    inner_field.default_value_expression = None
    inner_field.fields = []

    outer_field = MagicMock()
    outer_field.name = "struct_col"
    outer_field.field_type = "RECORD"
    outer_field.mode = "REPEATED"
    outer_field.default_value_expression = None
    outer_field.fields = [inner_field]

    fields = [outer_field]
    result = bq_table_config_obj._extract_fields(fields)

    assert result == [
        {
            "name": "struct_col",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [{"name": "nested_col", "type": "FLOAT", "mode": "NULLABLE"}],
        }
    ]


def test_extract_fields_negative_empty_list(bq_table_config_obj):
    # Empty input should return empty list
    result = bq_table_config_obj._extract_fields([])
    assert result == []


def test_extract_fields_negative_missing_attrs(bq_table_config_obj):
    # Field missing attributes (simulate as dict or incomplete MagicMock)
    incomplete_field = MagicMock()
    incomplete_field.name = "unknown"
    incomplete_field.field_type = None
    incomplete_field.mode = None
    incomplete_field.default_value_expression = None
    incomplete_field.fields = []

    fields = [incomplete_field]
    result = bq_table_config_obj._extract_fields(fields)
    # Should handle the missing type and mode gracefully
    assert result == [{"name": "unknown", "type": None, "mode": None}]


def test_extract_partitioning_positive(bq_table_config_obj):
    # Simulate a table with time partitioning
    mock_table = MagicMock()
    mock_partitioning = MagicMock()
    mock_partitioning.type_ = "DAY"
    mock_partitioning.field = "created_at"
    mock_table.time_partitioning = mock_partitioning

    result = bq_table_config_obj._extract_partitioning(mock_table)
    assert result == {"type": "DAY", "field": "created_at"}


def test_extract_partitioning_positive_default_field(bq_table_config_obj):
    # Partitioning exists, but field is None (should default to _PARTITIONTIME)
    mock_table = MagicMock()
    mock_partitioning = MagicMock()
    mock_partitioning.type_ = "DAY"
    mock_partitioning.field = None
    mock_table.time_partitioning = mock_partitioning

    result = bq_table_config_obj._extract_partitioning(mock_table)
    assert result == {"type": "DAY", "field": "_PARTITIONTIME"}


def test_extract_partitioning_negative_no_partitioning(bq_table_config_obj):
    # No partitioning should return empty dict
    mock_table = MagicMock()
    mock_table.time_partitioning = None

    result = bq_table_config_obj._extract_partitioning(mock_table)
    assert result == {}


def test_build_table_config_rows_positive(bq_table_config_obj):
    tables = ["pcb-test-landing.ds.table1", "pcb-test-landing.ds.table2"]
    mock_schema = {"columns": "mock_cols"}
    mock_cluster = {"clustering": [], "partitioning": {}}
    mock_last_modified = datetime(2024, 6, 22, 15, 0)

    # Patch get_table_config method to return expected values for both tables
    with patch.object(
        bq_table_config_obj,
        "get_table_config",
        side_effect=[
            (mock_schema, mock_cluster, mock_last_modified),
            (mock_schema, mock_cluster, mock_last_modified),
        ],
    ):
        result = bq_table_config_obj.build_table_config_rows(tables)
        assert len(result) == 2
        for table_id in tables:
            assert result[table_id]["table_id"] == table_id
            assert result[table_id]["version"] == 0
            assert result[table_id]["applied_script"] == "terraform"
            assert result[table_id]["insert_timestamp"] == mock_last_modified
            assert result[table_id]["json_table_schema"] == mock_schema
            assert result[table_id]["metadata"] == mock_cluster


def test_build_table_config_rows_negative(bq_table_config_obj, caplog):
    tables = ["pcb-test-landing.ds.table1", "pcb-test-landing.ds.table2"]
    mock_schema = {"columns": "mock_cols"}
    mock_cluster = {"clustering": [], "partitioning": {}}
    mock_last_modified = datetime(2024, 6, 22, 15, 0)

    # First call succeeds, second raises Exception
    with patch.object(
        bq_table_config_obj,
        "get_table_config",
        side_effect=[
            (mock_schema, mock_cluster, mock_last_modified),
            Exception("Test error"),
        ],
    ):
        with caplog.at_level("ERROR"):
            result = bq_table_config_obj.build_table_config_rows(tables)
        assert len(result) == 1
        assert tables[0] in result
        assert tables[1] not in result
        assert "Error processing pcb-test-landing.ds.table2" in caplog.text


# -- Tests for read_tables_from_file --


def test_read_tables_from_file_positive():
    lines = [
        "# This is a comment line\n",
        "pcb-test-landing.ds.table1\n",
        "pcb-test-landing.ds.table2\n",
        "   pcb-test-landing.ds.table3  \n",
        "\n",
        "# Another comment\n",
    ]
    with tempfile.NamedTemporaryFile("w+", delete=False) as tf:
        tf.writelines(lines)
        filename = tf.name
    try:
        tables = BigQueryTableConfig.read_tables_from_file(filename)
        assert tables == [
            "pcb-test-landing.ds.table1",
            "pcb-test-landing.ds.table2",
            "pcb-test-landing.ds.table3",
        ]
    finally:
        os.remove(filename)


def test_read_tables_from_file_negative():
    with tempfile.NamedTemporaryFile("w+", delete=False) as tf:
        filename = tf.name
    try:
        tables = BigQueryTableConfig.read_tables_from_file(filename)
        assert tables == []
    finally:
        os.remove(filename)


def test_get_latest_version_positive(bq_table_config):
    obj, mock_client = bq_table_config

    mock_query_job = MagicMock()
    mock_row = MagicMock()
    mock_row.version = 42
    mock_query_job.result.return_value = [mock_row]
    mock_client.query.return_value = mock_query_job

    with patch(
        "bigquery_table_management.bq_terraform_table_audit_management_dag.bigquery.ScalarQueryParameter"
    ), patch(
        "bigquery_table_management.bq_terraform_table_audit_management_dag.bigquery.QueryJobConfig"
    ):
        result = obj.get_latest_version(
            "pcb-test-landing.ds.audit_table", "my_table_ref"
        )

    assert result == 42


def test_get_latest_version_negative_exception(bq_table_config, caplog):
    obj, mock_client = bq_table_config

    target_table = "pcb-test-landing.ds.audit_table"
    table_ref = "my_table_ref"

    # Simulate an exception when client.query is called
    mock_client.query.side_effect = Exception("query failed")

    # Patch ScalarQueryParameter and QueryJobConfig if used in the method
    with patch(
        "bigquery_table_management.bq_terraform_table_audit_management_dag.bigquery.ScalarQueryParameter"
    ), patch(
        "bigquery_table_management.bq_terraform_table_audit_management_dag.bigquery.QueryJobConfig"
    ):
        with caplog.at_level("ERROR"):
            result = obj.get_latest_version(target_table, table_ref)

    assert result is None
    assert "Failed to get latest version for `my_table_ref`" in caplog.text


def test_update_table_schema_insert(bq_table_config_obj):
    rows = {
        "table1": {
            "table_id": "table1",
            "version": 0,
            "applied_script": "terraform",
            "insert_timestamp": "now",
            "json_table_schema": {},
            "metadata": {},
        }
    }
    with patch.object(
        bq_table_config_obj, "get_latest_version", return_value=None
    ), patch.object(bq_table_config_obj, "insert_row_bigquery") as mock_insert:
        bq_table_config_obj.update_table_schema(rows, "table1", "target_table")
        mock_insert.assert_called_once_with("target_table", rows["table1"])


def test_update_table_schema_version_zero(bq_table_config_obj):
    rows = {
        "table1": {
            "table_id": "table1",
            "version": 0,
            "applied_script": "terraform",
            "insert_timestamp": "now",
            "json_table_schema": {},
            "metadata": {},
        }
    }
    with patch.object(
        bq_table_config_obj, "get_latest_version", return_value=0
    ), patch.object(bq_table_config_obj, "insert_row_bigquery"):
        with pytest.raises(ValueError) as excinfo:
            bq_table_config_obj.update_table_schema(rows, "table1", "target_table")
        assert "Version is 0 for table ID 'table1'" in str(excinfo.value)


def test_update_table_schema_missing_table_id(bq_table_config_obj):
    rows = {}  # table_id not in rows
    with patch.object(bq_table_config_obj, "get_latest_version"), patch.object(
        bq_table_config_obj, "insert_row_bigquery"
    ):
        with pytest.raises(ValueError) as excinfo:
            bq_table_config_obj.update_table_schema(rows, "table1", "target_table")
        assert "Table ID 'table1' does not exist in the rows." in str(excinfo.value)


def test_update_table_schema_version_nonzero_logs_skip(bq_table_config_obj, caplog):
    rows = {
        "table1": {
            "table_id": "table1",
            "version": 1,
            "applied_script": "terraform",
            "insert_timestamp": "now",
            "json_table_schema": {},
            "metadata": {},
        }
    }
    with patch.object(
        bq_table_config_obj, "get_latest_version", return_value=1
    ), patch.object(bq_table_config_obj, "insert_row_bigquery"):
        with caplog.at_level("INFO"):
            bq_table_config_obj.update_table_schema(rows, "table1", "target_table")
        # Should log info and not insert
        assert (
            "Row for table ID 'table1' is not created via terraform. Skipping..."
            in caplog.text
        )


def test_insert_row_bigquery_positive(bq_table_config, caplog):
    obj, mock_client = bq_table_config

    row = {
        "table_id": "table123",
        "version": 1,
        "applied_script": "terraform",
        "insert_timestamp": "2024-06-22 18:00:00",
        "json_table_schema": {"columns": []},
        "metadata": {"clustering": [], "partitioning": {}},
    }
    target_table = "pcb-test-landing.ds.target_table"

    mock_job = MagicMock()
    mock_job.result.return_value = None
    mock_client.query.return_value = mock_job

    with patch(
        "bigquery_table_management.bq_terraform_table_audit_management_dag.bigquery.ScalarQueryParameter"
    ), patch(
        "bigquery_table_management.bq_terraform_table_audit_management_dag.bigquery.QueryJobConfig"
    ):
        with caplog.at_level("INFO"):
            obj.insert_row_bigquery(target_table, row)
    mock_client.query.assert_called_once()
    mock_job.result.assert_called_once()
    assert f"Insert successful for table {row['table_id']}" in caplog.text


def test_insert_row_bigquery_negative_exception(bq_table_config, caplog):
    obj, mock_client = bq_table_config

    row = {
        "table_id": "table123",
        "version": 1,
        "applied_script": "terraform",
        "insert_timestamp": "2024-06-22 18:00:00",
        "json_table_schema": {"columns": []},
        "metadata": {"clustering": [], "partitioning": {}},
    }
    target_table = "pcb-test-landing.ds.target_table"

    mock_client.query.side_effect = Exception("query failed")

    with patch(
        "bigquery_table_management.bq_terraform_table_audit_management_dag.bigquery.ScalarQueryParameter"
    ), patch(
        "bigquery_table_management.bq_terraform_table_audit_management_dag.bigquery.QueryJobConfig"
    ):
        with caplog.at_level("ERROR"):
            try:
                obj.insert_row_bigquery(target_table, row)
            except Exception:  # since your method doesn't catch exceptions
                pass
    # Test should raise, so no INFO log
    assert f"Insert successful for table {row['table_id']}" not in caplog.text
