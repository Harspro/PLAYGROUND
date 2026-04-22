from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace
from typing import Optional
from unittest.mock import patch

import pytest

from prmi_jira_tickets.etl.load import (
    _get_datetime_columns,
    ensure_columns_exist,
    validate_schema_compatibility,
)


@dataclass
class SampleRecord:
    created_date: datetime
    updated_date: Optional[datetime]
    name: str


def test_get_datetime_columns_from_model():
    cols = _get_datetime_columns(SampleRecord(datetime.now(), None, "x"))
    assert cols == {"created_date", "updated_date"}


def test_validate_schema_compatibility_mismatch_raises():
    source_schema = {"name": "STRING"}
    target_schema = {"name": "INT64"}

    with pytest.raises(ValueError) as exc:
        validate_schema_compatibility("proj.ds.tbl", source_schema, target_schema, set())

    assert "Schema mismatch detected" in str(exc.value)


@patch("prmi_jira_tickets.etl.load.bigquery.Client")
def test_ensure_columns_exist_blocks_terraform_tables(mock_client):
    table = SimpleNamespace(labels={"managed_by": "terraform"}, schema=[])
    mock_client.return_value.get_table.return_value = table

    with pytest.raises(RuntimeError):
        ensure_columns_exist("proj.ds.tbl", SampleRecord(datetime.now(), None, "x"))
