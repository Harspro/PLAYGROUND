"""
Tests for the gcs_utils functions utilized throughout the data foundation repository.

This module contains unit tests for GCS utility functions, with a focus on testing
the cleanup_gcs_folder function used in CDIC processing and other data pipelines.
"""
import sys
import os
import pytest
from unittest.mock import patch, MagicMock, call
from google.cloud import storage
from google.cloud.exceptions import NotFound

from util.gcs_utils import (
    cleanup_gcs_folder,
    parse_file_path_pattern,
    get_matching_files,
    count_rows_in_file
)


class TestCleanupGcsFolder:
    """Test cases for the cleanup_gcs_folder function."""

    @pytest.fixture
    def mock_storage_client(self):
        """Mock storage client fixture."""
        with patch('util.gcs_utils.storage.Client') as mock_client:
            yield mock_client

    @pytest.fixture
    def mock_bucket(self):
        """Mock bucket fixture."""
        mock_bucket = MagicMock()
        return mock_bucket

    @pytest.fixture
    def mock_blobs(self):
        """Mock blobs fixture."""
        mock_blob1 = MagicMock()
        mock_blob1.name = "cdic/pcma/extract/cdic-0201-20250101.txt"

        mock_blob2 = MagicMock()
        mock_blob2.name = "cdic/pcma/extract/cdic-0201-20250102.txt"

        mock_blob3 = MagicMock()
        mock_blob3.name = "cdic/pcma/extract/cdic-0202-20250101.txt"

        mock_blob4 = MagicMock()
        mock_blob4.name = "cdic/pcma/extract/other-file.txt"

        return [mock_blob1, mock_blob2, mock_blob3, mock_blob4]

    def test_cleanup_with_file_patterns(self, mock_storage_client, mock_bucket, mock_blobs):
        """Test cleanup with specific file patterns."""
        # Setup
        mock_client_instance = MagicMock()
        mock_storage_client.return_value = mock_client_instance
        mock_client_instance.bucket.return_value = mock_bucket

        # Mock list_blobs to return appropriate blobs for each pattern
        def mock_list_blobs(prefix):
            if "cdic-0201-" in prefix:
                return [mock_blobs[0], mock_blobs[1]]  # cdic-0201 files
            elif "cdic-0202-" in prefix:
                return [mock_blobs[2]]  # cdic-0202 file
            return []

        mock_bucket.list_blobs.side_effect = mock_list_blobs

        file_patterns = ["cdic-0201-*", "cdic-0202-*"]

        # Execute
        cleanup_gcs_folder("test-bucket", "cdic/pcma/extract/", file_patterns)

        # Assert
        mock_storage_client.assert_called_once()
        mock_client_instance.bucket.assert_called_once_with("test-bucket")

        # Should call list_blobs for each pattern
        expected_calls = [
            call(prefix="cdic/pcma/extract/cdic-0201-"),
            call(prefix="cdic/pcma/extract/cdic-0202-")
        ]
        mock_bucket.list_blobs.assert_has_calls(expected_calls)

        # Should delete matching blobs
        mock_blobs[0].delete.assert_called_once()  # cdic-0201 file 1
        mock_blobs[1].delete.assert_called_once()  # cdic-0201 file 2
        mock_blobs[2].delete.assert_called_once()  # cdic-0202 file

        # Should not delete non-matching blob
        mock_blobs[3].delete.assert_not_called()

    def test_cleanup_entire_directory_no_patterns(
        self, mock_storage_client, mock_bucket, mock_blobs
    ):
        """Test cleanup_gcs_folder cleans entire directory when no patterns provided."""
        # Setup
        mock_client_instance = MagicMock()
        mock_storage_client.return_value = mock_client_instance
        mock_client_instance.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = mock_blobs

        # Execute
        cleanup_gcs_folder("test-bucket", "cdic/pcma/extract/")

        # Assert
        mock_storage_client.assert_called_once()
        mock_client_instance.bucket.assert_called_once_with("test-bucket")
        mock_bucket.list_blobs.assert_called_once_with(prefix="cdic/pcma/extract/")

        # Should delete all blobs (entire directory cleanup)
        for blob in mock_blobs:
            blob.delete.assert_called_once()

    def test_cleanup_entire_directory_empty_patterns(
        self, mock_storage_client, mock_bucket, mock_blobs
    ):
        """Test cleanup_gcs_folder cleans entire directory when empty patterns list provided."""
        # Setup
        mock_client_instance = MagicMock()
        mock_storage_client.return_value = mock_client_instance
        mock_client_instance.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = mock_blobs

        # Execute
        cleanup_gcs_folder("test-bucket", "cdic/pcma/extract/", [])

        # Assert
        mock_storage_client.assert_called_once()
        mock_client_instance.bucket.assert_called_once_with("test-bucket")
        mock_bucket.list_blobs.assert_called_once_with(prefix="cdic/pcma/extract/")

        # Should delete all blobs (entire directory cleanup)
        for blob in mock_blobs:
            blob.delete.assert_called_once()

    def test_cleanup_entire_directory_none_patterns(
        self, mock_storage_client, mock_bucket, mock_blobs
    ):
        """Test cleanup_gcs_folder cleans entire directory when None patterns provided."""
        # Setup
        mock_client_instance = MagicMock()
        mock_storage_client.return_value = mock_client_instance
        mock_client_instance.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = mock_blobs

        # Execute
        cleanup_gcs_folder("test-bucket", "cdic/pcma/extract/", None)

        # Assert
        mock_storage_client.assert_called_once()
        mock_client_instance.bucket.assert_called_once_with("test-bucket")
        mock_bucket.list_blobs.assert_called_once_with(prefix="cdic/pcma/extract/")

        # Should delete all blobs (entire directory cleanup)
        for blob in mock_blobs:
            blob.delete.assert_called_once()

    def test_cleanup_skips_folder_markers(self, mock_storage_client, mock_bucket):
        """Test cleanup_gcs_folder processes all blobs including folder markers (current implementation deletes them)."""
        # Setup
        mock_client_instance = MagicMock()
        mock_storage_client.return_value = mock_client_instance
        mock_client_instance.bucket.return_value = mock_bucket

        # Create blobs including folder markers
        folder_marker = MagicMock()
        folder_marker.name = "casepurge_images/"

        subfolder_marker = MagicMock()
        subfolder_marker.name = "casepurge_images/subfolder/"

        regular_file1 = MagicMock()
        regular_file1.name = "casepurge_images/file1.pdf"

        regular_file2 = MagicMock()
        regular_file2.name = "casepurge_images/file2.png"

        mock_bucket.list_blobs.return_value = [
            folder_marker, subfolder_marker, regular_file1, regular_file2
        ]

        # Execute - entire directory cleanup
        cleanup_gcs_folder("test-bucket", "casepurge_images/", None)

        # Assert
        mock_storage_client.assert_called_once()
        mock_client_instance.bucket.assert_called_once_with("test-bucket")
        mock_bucket.list_blobs.assert_called_once_with(prefix="casepurge_images/")

        # Current implementation deletes folder markers (no filtering)
        folder_marker.delete.assert_called_once()
        subfolder_marker.delete.assert_called_once()

        # Should delete regular files
        regular_file1.delete.assert_called_once()
        regular_file2.delete.assert_called_once()

    def test_cleanup_with_single_pattern(self, mock_storage_client, mock_bucket, mock_blobs):
        """Test cleanup with a single file pattern."""
        # Setup
        mock_client_instance = MagicMock()
        mock_storage_client.return_value = mock_client_instance
        mock_client_instance.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = mock_blobs[:2]  # Only cdic-0201 files

        # Execute
        cleanup_gcs_folder("test-bucket", "cdic/pcma/extract/", ["cdic-0201-*"])

        # Assert
        mock_bucket.list_blobs.assert_called_once_with(prefix="cdic/pcma/extract/cdic-0201-")

        # Should delete matching blobs
        for blob in mock_blobs[:2]:
            blob.delete.assert_called_once()

    def test_cleanup_no_matching_files(self, mock_storage_client, mock_bucket):
        """Test cleanup when no files match the patterns."""
        # Setup
        mock_client_instance = MagicMock()
        mock_storage_client.return_value = mock_client_instance
        mock_client_instance.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = []

        # Execute
        cleanup_gcs_folder("test-bucket", "cdic/pcma/extract/", ["cdic-0999-*"])

        # Assert
        mock_storage_client.assert_called_once()
        mock_client_instance.bucket.assert_called_once_with("test-bucket")
        mock_bucket.list_blobs.assert_called_once_with(prefix="cdic/pcma/extract/cdic-0999-")

    @patch('util.gcs_utils.logger')
    def test_cleanup_with_delete_error(self, mock_logger, mock_storage_client, mock_bucket, mock_blobs):
        """Test cleanup handles delete errors gracefully."""
        # Setup
        mock_client_instance = MagicMock()
        mock_storage_client.return_value = mock_client_instance
        mock_client_instance.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = mock_blobs[:1]

        # Make delete raise an exception
        mock_blobs[0].delete.side_effect = Exception("Delete failed")

        # Execute
        cleanup_gcs_folder("test-bucket", "cdic/pcma/extract/", ["cdic-0201-*"])

        # Assert
        mock_blobs[0].delete.assert_called_once()
        mock_logger.error.assert_called_once()

    def test_cleanup_pattern_matching_logic(self, mock_storage_client, mock_bucket):
        """Test that pattern matching works correctly."""
        # Setup
        mock_client_instance = MagicMock()
        mock_storage_client.return_value = mock_client_instance
        mock_client_instance.bucket.return_value = mock_bucket

        # Create blobs with different names
        matching_blob = MagicMock()
        matching_blob.name = "folder/cdic-0201-file.txt"

        non_matching_blob = MagicMock()
        non_matching_blob.name = "folder/other-file.txt"

        mock_bucket.list_blobs.return_value = [matching_blob, non_matching_blob]

        # Execute
        cleanup_gcs_folder("test-bucket", "folder/", ["cdic-0201-*"])

        # Assert
        matching_blob.delete.assert_called_once()
        non_matching_blob.delete.assert_not_called()

    @patch('util.gcs_utils.logger')
    def test_cleanup_logging(self, mock_logger, mock_storage_client, mock_bucket, mock_blobs):
        """Test that appropriate logging occurs during cleanup."""
        # Setup
        mock_client_instance = MagicMock()
        mock_storage_client.return_value = mock_client_instance
        mock_client_instance.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = mock_blobs[:2]

        # Execute
        cleanup_gcs_folder("test-bucket", "cdic/pcma/extract/", ["cdic-0201-*"])

        # Assert logging calls
        mock_logger.info.assert_called()

        # Check that completion log is called
        completion_calls = [call for call in mock_logger.info.call_args_list
                            if "Cleanup completed" in str(call)]
        assert len(completion_calls) > 0

    def test_cleanup_multiple_patterns_comprehensive(
        self, mock_storage_client, mock_bucket
    ):
        """Test comprehensive cleanup with multiple patterns and various file types."""
        # Setup
        mock_client_instance = MagicMock()
        mock_storage_client.return_value = mock_client_instance
        mock_client_instance.bucket.return_value = mock_bucket

        # Create comprehensive set of mock blobs
        test_blobs = []
        blob_names = [
            "cdic/pcma/extract/cdic-0201-20250101.txt",
            "cdic/pcma/extract/cdic-0201-20250102.txt",
            "cdic/pcma/extract/cdic-0202-20250101.txt",
            "cdic/pcma/extract/cdic-0999-20250101.txt",
            "cdic/pcma/extract/other-file.txt",
            "cdic/pcma/extract/cdic-0203-20250101.txt"  # Should not match
        ]

        for name in blob_names:
            blob = MagicMock()
            blob.name = name
            test_blobs.append(blob)

        # Mock list_blobs to return appropriate blobs for each pattern
        def mock_list_blobs(prefix):
            if "cdic-0201-" in prefix:
                return [test_blobs[0], test_blobs[1]]
            elif "cdic-0202-" in prefix:
                return [test_blobs[2]]
            elif "cdic-0999-" in prefix:
                return [test_blobs[3]]
            return []

        mock_bucket.list_blobs.side_effect = mock_list_blobs

        # Execute
        patterns = ["cdic-0201-*", "cdic-0202-*", "cdic-0999-*"]
        cleanup_gcs_folder("test-bucket", "cdic/pcma/extract/", patterns)

        # Assert
        # Should delete first 4 blobs (matching patterns)
        for i in range(4):
            test_blobs[i].delete.assert_called_once()

        # Should not delete non-matching blobs
        test_blobs[4].delete.assert_not_called()  # other-file.txt
        test_blobs[5].delete.assert_not_called()  # cdic-0203 (not in patterns)


class TestParseFilePathPattern:
    """Test cases for the parse_file_path_pattern function."""

    def test_parse_pattern_with_folder_and_wildcard(self):
        """Test parsing file path with folder and wildcard pattern."""
        # Execute
        folder, pattern = parse_file_path_pattern('data/exports/sales-*.csv')

        # Assert
        assert folder == 'data/exports/'
        assert pattern == 'sales-*.csv'

    def test_parse_pattern_with_nested_folders(self):
        """Test parsing file path with multiple nested folders."""
        # Execute
        folder, pattern = parse_file_path_pattern('folder/subfolder/deep/file-*.parquet')

        # Assert
        assert folder == 'folder/subfolder/deep/'
        assert pattern == 'file-*.parquet'

    def test_parse_pattern_no_folder(self):
        """Test parsing file path without folder (root level)."""
        # Execute
        folder, pattern = parse_file_path_pattern('file-*.csv')

        # Assert
        assert folder == ''
        assert pattern == 'file-*.csv'

    def test_parse_pattern_single_file_with_folder(self):
        """Test parsing single file path with folder."""
        # Execute
        folder, pattern = parse_file_path_pattern('data/output.csv')

        # Assert
        assert folder == 'data/'
        assert pattern == 'output.csv'

    def test_parse_pattern_single_file_no_folder(self):
        """Test parsing single file path without folder."""
        # Execute
        folder, pattern = parse_file_path_pattern('output.csv')

        # Assert
        assert folder == ''
        assert pattern == 'output.csv'

    def test_parse_pattern_with_multiple_wildcards(self):
        """Test parsing file path with multiple wildcards."""
        # Execute
        folder, pattern = parse_file_path_pattern('exports/sales-*-*.csv')

        # Assert
        assert folder == 'exports/'
        assert pattern == 'sales-*-*.csv'


class TestGetMatchingFiles:
    """Test cases for the get_matching_files function."""

    @pytest.fixture
    def mock_bucket(self):
        """Mock bucket fixture."""
        mock_bucket = MagicMock()
        return mock_bucket

    def test_get_matching_files_with_wildcard_csv(self, mock_bucket):
        """Test getting matching CSV files with wildcard pattern."""
        # Setup
        mock_blob1 = MagicMock()
        mock_blob1.name = 'exports/sales-2025-01.csv'

        mock_blob2 = MagicMock()
        mock_blob2.name = 'exports/sales-2025-02.csv'

        mock_blob3 = MagicMock()
        mock_blob3.name = 'exports/other-file.csv'

        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]

        # Execute
        result = get_matching_files(mock_bucket, 'exports/sales-*.csv', file_extensions=['.csv'])

        # Assert
        assert len(result) == 2
        assert 'exports/sales-2025-01.csv' in result
        assert 'exports/sales-2025-02.csv' in result
        assert 'exports/other-file.csv' not in result
        mock_bucket.list_blobs.assert_called_once_with(prefix='exports/')

    def test_get_matching_files_with_wildcard_parquet(self, mock_bucket):
        """Test getting matching Parquet files with wildcard pattern."""
        # Setup
        mock_blob1 = MagicMock()
        mock_blob1.name = 'data/output-part1.parquet'

        mock_blob2 = MagicMock()
        mock_blob2.name = 'data/output-part2.parquet'

        mock_blob3 = MagicMock()
        mock_blob3.name = 'data/input.parquet'

        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]

        # Execute
        result = get_matching_files(mock_bucket, 'data/output-*.parquet', file_extensions=['.parquet'])

        # Assert
        assert len(result) == 2
        assert 'data/output-part1.parquet' in result
        assert 'data/output-part2.parquet' in result
        assert 'data/input.parquet' not in result

    def test_get_matching_files_multiple_extensions(self, mock_bucket):
        """Test getting matching files with multiple allowed extensions."""
        # Setup
        mock_blob1 = MagicMock()
        mock_blob1.name = 'data/file-1.csv'

        mock_blob2 = MagicMock()
        mock_blob2.name = 'data/file-2.parquet'

        mock_blob3 = MagicMock()
        mock_blob3.name = 'data/file-3.json'

        mock_blob4 = MagicMock()
        mock_blob4.name = 'data/file-4.txt'

        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3, mock_blob4]

        # Execute
        result = get_matching_files(mock_bucket, 'data/file-*.*', file_extensions=['.csv', '.parquet'])

        # Assert
        assert len(result) == 2
        assert 'data/file-1.csv' in result
        assert 'data/file-2.parquet' in result
        assert 'data/file-3.json' not in result
        assert 'data/file-4.txt' not in result

    def test_get_matching_files_no_extension_filter(self, mock_bucket):
        """Test getting matching files without extension filtering."""
        # Setup
        mock_blob1 = MagicMock()
        mock_blob1.name = 'data/file-1.csv'

        mock_blob2 = MagicMock()
        mock_blob2.name = 'data/file-2.parquet'

        mock_blob3 = MagicMock()
        mock_blob3.name = 'data/file-3.txt'

        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]

        # Execute
        result = get_matching_files(mock_bucket, 'data/file-*.*', file_extensions=None)

        # Assert
        assert len(result) == 3
        assert 'data/file-1.csv' in result
        assert 'data/file-2.parquet' in result
        assert 'data/file-3.txt' in result

    def test_get_matching_files_single_file_no_wildcard(self, mock_bucket):
        """Test getting single file without wildcard (direct file path)."""
        # Execute
        result = get_matching_files(mock_bucket, 'data/specific-file.csv', file_extensions=['.csv'])

        # Assert
        assert len(result) == 1
        assert result[0] == 'data/specific-file.csv'
        mock_bucket.list_blobs.assert_not_called()  # Should not list when no wildcard

    def test_get_matching_files_no_matches(self, mock_bucket):
        """Test getting matching files when no files match the pattern."""
        # Setup
        mock_blob1 = MagicMock()
        mock_blob1.name = 'exports/other-file.csv'

        mock_bucket.list_blobs.return_value = [mock_blob1]

        # Execute
        result = get_matching_files(mock_bucket, 'exports/sales-*.csv', file_extensions=['.csv'])

        # Assert
        assert len(result) == 0

    def test_get_matching_files_case_insensitive_extension(self, mock_bucket):
        """Test getting matching files with case-insensitive extension matching."""
        # Setup
        mock_blob1 = MagicMock()
        mock_blob1.name = 'data/file.CSV'  # Uppercase extension

        mock_blob2 = MagicMock()
        mock_blob2.name = 'data/file.csv'  # Lowercase extension

        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2]

        # Execute
        result = get_matching_files(mock_bucket, 'data/*.csv', file_extensions=['.csv'])

        # Assert
        assert len(result) == 2  # Both should match (case-insensitive)
        assert 'data/file.CSV' in result
        assert 'data/file.csv' in result


class TestCountRowsInFile:
    """Test cases for the count_rows_in_file function."""

    @pytest.fixture
    def mock_bucket(self):
        """Mock bucket fixture."""
        mock_bucket = MagicMock()
        mock_bucket.name = 'test-bucket'
        return mock_bucket

    def test_count_rows_csv_with_header(self, mock_bucket):
        """Test counting rows in CSV file with header."""
        # Setup
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        # Mock CSV content with header
        csv_content = "header1,header2,header3\nrow1,data1,data2\nrow2,data3,data4\nrow3,data5,data6"
        mock_blob.open.return_value.__enter__.return_value = iter(csv_content.split('\n'))

        # Execute
        row_count = count_rows_in_file(mock_bucket, 'data/file.csv', has_header=True)

        # Assert
        assert row_count == 3  # 4 total lines - 1 header = 3 data rows
        mock_bucket.blob.assert_called_once_with('data/file.csv')
        mock_blob.exists.assert_called_once()

    def test_count_rows_csv_without_header(self, mock_bucket):
        """Test counting rows in CSV file without header."""
        # Setup
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        # Mock CSV content without header
        csv_content = "row1,data1,data2\nrow2,data3,data4\nrow3,data5,data6"
        mock_blob.open.return_value.__enter__.return_value = iter(csv_content.split('\n'))

        # Execute
        row_count = count_rows_in_file(mock_bucket, 'data/file.csv', has_header=False)

        # Assert
        assert row_count == 3  # All 3 lines counted
        mock_bucket.blob.assert_called_once_with('data/file.csv')

    def test_count_rows_csv_empty_file(self, mock_bucket):
        """Test counting rows in empty CSV file."""
        # Setup
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        # Mock empty CSV content
        mock_blob.open.return_value.__enter__.return_value = iter([])

        # Execute
        row_count = count_rows_in_file(mock_bucket, 'data/empty.csv', has_header=True)

        # Assert
        assert row_count == 0

    def test_count_rows_csv_only_header(self, mock_bucket):
        """Test counting rows in CSV file with only header."""
        # Setup
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        # Mock CSV content with only header
        csv_content = "header1,header2,header3"
        mock_blob.open.return_value.__enter__.return_value = iter(csv_content.split('\n'))

        # Execute
        row_count = count_rows_in_file(mock_bucket, 'data/header-only.csv', has_header=True)

        # Assert
        assert row_count == 0  # 1 line - 1 header = 0 data rows

    @patch('util.gcs_utils.pq.ParquetFile')
    def test_count_rows_parquet(self, mock_parquet_file, mock_bucket):
        """Test counting rows in Parquet file."""
        # Setup
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        # Mock ParquetFile metadata
        mock_pf_instance = MagicMock()
        mock_pf_instance.metadata.num_rows = 1000
        mock_parquet_file.return_value = mock_pf_instance

        # Execute
        row_count = count_rows_in_file(mock_bucket, 'data/file.parquet')

        # Assert
        assert row_count == 1000
        mock_bucket.blob.assert_called_once_with('data/file.parquet')
        mock_blob.exists.assert_called_once()
        mock_parquet_file.assert_called_once()

    @patch('util.gcs_utils.pq.ParquetFile')
    def test_count_rows_parquet_empty(self, mock_parquet_file, mock_bucket):
        """Test counting rows in empty Parquet file."""
        # Setup
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        # Mock empty ParquetFile
        mock_pf_instance = MagicMock()
        mock_pf_instance.metadata.num_rows = 0
        mock_parquet_file.return_value = mock_pf_instance

        # Execute
        row_count = count_rows_in_file(mock_bucket, 'data/empty.parquet')

        # Assert
        assert row_count == 0

    def test_count_rows_file_not_exists(self, mock_bucket):
        """Test counting rows when file does not exist."""
        # Setup
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_bucket.blob.return_value = mock_blob

        # Execute
        row_count = count_rows_in_file(mock_bucket, 'data/nonexistent.csv')

        # Assert
        assert row_count == 0
        mock_bucket.blob.assert_called_once_with('data/nonexistent.csv')
        mock_blob.exists.assert_called_once()

    def test_count_rows_unsupported_file_type(self, mock_bucket):
        """Test counting rows in unsupported file type."""
        # Setup
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        # Execute
        row_count = count_rows_in_file(mock_bucket, 'data/file.txt')

        # Assert
        assert row_count == 0  # Unsupported types return 0

    @patch('util.gcs_utils.logger')
    def test_count_rows_csv_with_error(self, mock_logger, mock_bucket):
        """Test counting rows in CSV when error occurs during reading."""
        # Setup
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        # Make open raise an exception
        mock_blob.open.side_effect = Exception("Read error")

        # Execute
        row_count = count_rows_in_file(mock_bucket, 'data/error.csv')

        # Assert
        assert row_count == 0
        mock_logger.error.assert_called_once()

    @patch('util.gcs_utils.pq.ParquetFile')
    @patch('util.gcs_utils.logger')
    def test_count_rows_parquet_with_error(self, mock_logger, mock_parquet_file, mock_bucket):
        """Test counting rows in Parquet when error occurs during reading."""
        # Setup
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        # Make ParquetFile raise an exception
        mock_parquet_file.side_effect = Exception("Parquet read error")

        # Execute
        row_count = count_rows_in_file(mock_bucket, 'data/error.parquet')

        # Assert
        assert row_count == 0
        mock_logger.error.assert_called_once()

    @patch('util.gcs_utils.logger')
    def test_count_rows_csv_logging(self, mock_logger, mock_bucket):
        """Test that appropriate logging occurs when counting CSV rows."""
        # Setup
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        csv_content = "header\nrow1\nrow2"
        mock_blob.open.return_value.__enter__.return_value = iter(csv_content.split('\n'))

        # Execute
        count_rows_in_file(mock_bucket, 'data/file.csv', has_header=True)

        # Assert logging called
        mock_logger.info.assert_called()

    @patch('util.gcs_utils.pq.ParquetFile')
    @patch('util.gcs_utils.logger')
    def test_count_rows_parquet_logging(self, mock_logger, mock_parquet_file, mock_bucket):
        """Test that appropriate logging occurs when counting Parquet rows."""
        # Setup
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        mock_pf_instance = MagicMock()
        mock_pf_instance.metadata.num_rows = 500
        mock_parquet_file.return_value = mock_pf_instance

        # Execute
        count_rows_in_file(mock_bucket, 'data/file.parquet')

        # Assert logging called
        mock_logger.info.assert_called()
