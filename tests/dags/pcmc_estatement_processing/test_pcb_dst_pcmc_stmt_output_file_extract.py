import pytest

import util.constants as consts  # Assuming consts.PROCESSING_BUCKET is defined somewhere
from pcmc_estatement_processing.pcb_dst_pcmc_stmt_output_file_extract import get_output_folder_path, get_interim_folder_path


@pytest.fixture
def dag_config():
    return {
        consts.PROCESSING_BUCKET: "my-test-bucket",
        "PROCESSING_OUTPUT_DIR": "output_data",
        "PROCESSING_INTERIM_DIR": "interim_data"
    }


def test_get_output_folder_path(dag_config):
    """Test that output folder path is generated correctly."""
    result = get_output_folder_path(dag_config)
    assert result == "gs://my-test-bucket/output_data"


def test_get_interim_folder_path(dag_config):
    """Test that interim folder path is generated correctly."""
    result = get_interim_folder_path(dag_config)
    assert result == "gs://my-test-bucket/interim_data"


def test_missing_keys_in_dag_config(monkeypatch):
    """Test missing keys handling in dag_config."""
    # Patch consts to have a known key name
    monkeypatch.setattr(consts, "PROCESSING_BUCKET", "PROCESSING_BUCKET")
    dag_config = {"PROCESSING_BUCKET": "bucket-only"}

    # PROCESSING_OUTPUT_DIR or PROCESSING_INTERIM_DIR missing → should append 'None'
    output_path = get_output_folder_path(dag_config)
    interim_path = get_interim_folder_path(dag_config)

    assert output_path == "gs://bucket-only/None"
    assert interim_path == "gs://bucket-only/None"
