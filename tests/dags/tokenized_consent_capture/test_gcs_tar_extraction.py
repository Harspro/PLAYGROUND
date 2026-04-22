from __future__ import annotations
import os
import tarfile
import tempfile
import io
from unittest.mock import patch, MagicMock

import pytest
from airflow import settings
from airflow.exceptions import AirflowFailException
from idl_consent_signature.consent_signature_gcs_tar_extraction import GCSTarExtraction
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from util.miscutils import read_yamlfile_env

current_script_directory = os.path.dirname(os.path.abspath(__file__))
gcs_tar_extraction_config_directory = os.path.join(
    current_script_directory,
    "..",
    "config",
    "gcs_tar_extraction_configs")


@pytest.fixture
def test_gcs_tar_extraction_data() -> dict:
    return {
        'task_id': 'test-task-id',
        'source_bucket': 'test-source-bucket',
        'destination_bucket': 'test-destination-bucket',
        'tar_file': 'test-archive.tar',
        'destination_folder': 'test-folder'
    }


@pytest.fixture
def test_gcs_tar_extraction_config() -> dict:
    return read_yamlfile_env(os.path.join(gcs_tar_extraction_config_directory, "test_gcs_tar_extraction_case_1.yaml"),
                             "test")


class TestGCSTarExtraction:
    @pytest.mark.parametrize("bucket_name, source_blob, destination_bucket, destination_folder",
                             [
                                 ("test-source-bucket", "test-archive.tar", "test-destination-bucket", "test-folder"),
                                 ("sample-bucket", "sample.tar", "destination-bucket", "backup-folder")
                             ])
    @patch("idl_consent_signature.consent_signature_gcs_tar_extraction.GCSHook")
    def test_execute_gcs_extraction(self, mock_gcs_hook, bucket_name: str, source_blob: str,
                                    destination_bucket: str, destination_folder: str,
                                    test_gcs_tar_extraction_data: dict, test_gcs_tar_extraction_config: dict):

        temp_dir = tempfile.mkdtemp()
        tar_path = os.path.join(temp_dir, source_blob)

        with tarfile.open(tar_path, "w") as tar:
            for file_name in ["folder1/file1.txt", "folder2/file2.txt"]:
                data = b"dummy data"
                tar_info = tarfile.TarInfo(file_name)
                tar_info.size = len(data)
                tar.addfile(tar_info, io.BytesIO(data))

        mock_gcs_instance = mock_gcs_hook.return_value
        mock_gcs_instance.download.side_effect = lambda bucket, blob, path: os.rename(tar_path, path)
        mock_gcs_instance.upload.return_value = None

        operator = GCSTarExtraction.extract_and_upload(
            bucket_name=bucket_name,
            source_blob=source_blob,
            destination_bucket=destination_bucket,
            destination_folder=destination_folder
        )

        operator
        mock_gcs_instance.upload.assert_called()

    def test_safe_extract(self):
        temp_dir = tempfile.mkdtemp()
        tar_path = os.path.join(temp_dir, "test_archive.tar")

        with tarfile.open(tar_path, "w") as tar:
            for file_name in ["folder1/file1.txt", "folder2/file2.txt"]:
                data = b"dummy data"
                tar_info = tarfile.TarInfo(file_name)
                tar_info.size = len(data)
                tar.addfile(tar_info, io.BytesIO(data))

        extract_path = os.path.join(temp_dir, "extracted")
        os.makedirs(extract_path, exist_ok=True)

        with tarfile.open(tar_path, 'r') as tar:
            GCSTarExtraction.safe_extract(tar, extract_path)

        for file_name in ["folder1/file1.txt", "folder2/file2.txt"]:
            assert os.path.exists(os.path.join(extract_path, file_name))
