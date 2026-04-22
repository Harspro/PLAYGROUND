"""
This module contains comprehensive test cases for the fixed width file conversion utility functions
to ensure core functionality works correctly and achieve high test coverage.
"""
import pytest
import sys
import os
from io import StringIO
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowFailException

try:
    from util.gcs_utils import gcs_file_exists
except ImportError:
    from dags.util.gcs_utils import gcs_file_exists

from util.fixed_width_utils import (
    convert_delimited_to_fixed_width,
    _validate_fixed_width_schema,
    is_decimal_and_negative,
    _pad,
    _format_and_pad_value,
    _format_fixed_width,
    _process_header_footer,
    _process_header_footer_item,
    _render_template,
    _process_and_write_file
)


@pytest.fixture
def valid_source_config():
    """Fixture providing valid source configuration for testing."""
    return {
        "bucket": "test-bucket",
        "folder_prefix": "test/folder",
        "file_name": "test.csv",
        "delimiter": ",",
        "column_config": [
            "ID:1:10:0:L:string",
            "NAME:11:25: :L:string",
            "AMOUNT:36:15:0:R:decimal"
        ]
    }


@pytest.fixture
def valid_dest_config():
    """Fixture providing valid destination configuration for testing."""
    return {
        "bucket": "dest-bucket",
        "folder_prefix": "output/folder",
        "file_name": "output.txt"
    }


class TestConvertDelimitedToFixedWidth:
    """Test class for the main convert_delimited_to_fixed_width function."""

    def test_convert_with_none_configs_raises_exception(self, valid_dest_config):
        """Test that convert_delimited_to_fixed_width raises AirflowFailException with None configs."""
        with pytest.raises(AirflowFailException) as ex_info:
            convert_delimited_to_fixed_width(None, valid_dest_config)
        assert "Please provide required source and dest config" in str(ex_info.value)

    @patch('util.fixed_width_utils._process_and_write_file')
    @patch('util.fixed_width_utils.storage.Client')
    @patch('util.fixed_width_utils.read_file_bytes')
    @patch('util.fixed_width_utils.gcs_file_exists')
    def test_convert_reaches_src_path_build_and_calls_processor(self, mock_exists, mock_read_bytes, mock_storage_client, mock_process, valid_source_config, valid_dest_config):
        """Ensure src path build lines (159-162) execute and processing is invoked."""
        mock_exists.return_value = True
        mock_read_bytes.return_value = b'not-empty'  # ensure non-empty file
        mock_storage_client.return_value = MagicMock()

        # Add minimal required column_config for schema validation
        source_config = dict(valid_source_config)
        source_config["column_config"] = [
            "ID:1:5:0:L:string",
            "NAME:6:10: :R:string"
        ]

        convert_delimited_to_fixed_width(source_config, valid_dest_config)

        assert mock_process.called is True

    @patch('util.fixed_width_utils.storage.Client')
    @patch('util.fixed_width_utils.gcs_file_exists')
    def test_convert_raises_when_source_missing_after_path_build(self, mock_exists, mock_storage_client, valid_source_config, valid_dest_config):
        """Ensure missing source triggers failure (after building src path)."""
        mock_exists.return_value = False
        mock_storage_client.return_value = MagicMock()

        source_config = dict(valid_source_config)
        source_config["column_config"] = [
            "ID:1:5:0:L:string"
        ]

        with pytest.raises(AirflowFailException) as ex_info:
            convert_delimited_to_fixed_width(source_config, valid_dest_config)
        assert "not found in bucket" in str(ex_info.value)


class TestValidateColumnConfig:
    """Test class for the _validate_fixed_width_schema function."""

    def test_validate_valid_config(self):
        """Test validation of valid column configuration."""
        config = ["ID:1:10:0:L:string"]
        result = _validate_fixed_width_schema(config)
        assert len(result) == 1
        assert result[0]['name'] == 'ID'
        assert result[0]['length'] == 10

    def test_validate_invalid_config_raises_exception(self):
        """Test that invalid configuration raises ValueError."""
        config = ["ID:1:10"]  # Too few parts
        with pytest.raises(ValueError):
            _validate_fixed_width_schema(config)


class TestDecimalHandling:
    """Test class for decimal handling and validation functions."""

    def test_is_decimal_and_negative_positive(self):
        """Test is_decimal_and_negative function with positive value."""
        result = is_decimal_and_negative("123.45")
        assert result is False

    def test_is_decimal_and_negative_negative(self):
        """Test is_decimal_and_negative function with negative value."""
        result = is_decimal_and_negative("-123.45")
        assert result is True

    def test_is_decimal_and_negative_invalid(self):
        """Test is_decimal_and_negative function with invalid value."""
        result = is_decimal_and_negative("abc")
        assert result is False

    def test_format_and_pad_value_decimal(self):
        """Test decimal formatting."""
        result = _format_and_pad_value("123.45", 10, "0", "left", "decimal")
        assert result == "0000123.45"

    def test_format_and_pad_value_invalid_decimal_padding_raises_exception(self):
        """Test that invalid decimal padding raises ValueError."""
        with pytest.raises(ValueError) as ex_info:
            _format_and_pad_value("123.45", 10, "x", "left", "decimal")
        assert "Invalid padding for decimal type" in str(ex_info.value)

    def test_format_and_pad_value_negative_decimal(self):
        """Test negative decimal formatting with proper sign handling."""
        # Test with string representation
        result = _format_and_pad_value("-123", 8, "0", "left", "decimal")
        assert result == "-0000123", f"Expected '-0000123' but got '{result}'"

        # Test with shorter negative number
        result = _format_and_pad_value("-45", 5, "0", "left", "decimal")
        assert result == "-0045", f"Expected '-0045' but got '{result}'"


class TestPaddingAndFormatting:
    """Test class for padding and formatting functions."""

    def test_pad_function_left(self):
        """Test _pad function with left padding."""
        result = _pad("test", 10, "0", "left")
        assert result == "000000test"

    def test_pad_function_right(self):
        """Test _pad function with right padding."""
        result = _pad("test", 10, " ", "right")
        assert result == "test      "

    def test_format_and_pad_value_string(self):
        """Test string formatting."""
        result = _format_and_pad_value("test", 10, " ", "right", "string")
        assert result == "test      "


class TestFixedWidthFormatting:
    """Test class for fixed width formatting functions."""

    @pytest.fixture
    def sample_schema(self):
        """Fixture providing sample schema for testing."""
        return [
            {'name': 'ID', 'length': 5, 'pad_value': '0', 'pad_type': 'left', 'type': 'string'},
            {'name': 'NAME', 'length': 10, 'pad_value': ' ', 'pad_type': 'right', 'type': 'string'},
            {'name': 'AMOUNT', 'length': 8, 'pad_value': '0', 'pad_type': 'right', 'type': 'decimal'}
        ]

    def test_format_fixed_width_normal_case(self, sample_schema):
        """Test normal case of fixed width formatting."""
        values = ["123", "John", "123.45"]
        result = _format_fixed_width(values, sample_schema, 1)
        assert result == "00123John      123.4500"

    def test_format_fixed_width_column_count_mismatch_raises_exception(self, sample_schema):
        """Test that column count mismatch raises ValueError."""
        values = ["123", "John"]  # Missing third value
        with pytest.raises(ValueError) as ex_info:
            _format_fixed_width(values, sample_schema, 1)
        assert "Column count mismatch" in str(ex_info.value)


class TestHeaderFooterProcessing:
    """Test class for header and footer processing functions."""

    def test_process_header_footer_string_config(self):
        """Test header/footer processing with string configuration."""
        result = _process_header_footer("SIMPLE_HEADER", 0)
        assert result == ["SIMPLE_HEADER"]

    def test_process_header_footer_none_config(self):
        """Test header/footer processing with None configuration."""
        result = _process_header_footer(None, 0)
        assert result == []

    def test_process_header_footer_item_template(self):
        """Test individual header/footer item processing with template."""
        item = {
            "template": "HDR{{record_count}}",
            "length": 10,
            "pad_value": "0",
            "pad_type": "left"
        }
        result = _process_header_footer_item(item, 789)
        assert result == "00HDR{789}"

    def test_process_header_footer_item_value(self):
        """Test individual header/footer item processing with value."""
        item = {
            "value": "TOTAL",
            "length": 8,
            "pad_value": " ",
            "pad_type": "right"
        }
        result = _process_header_footer_item(item, 0)
        assert result == "TOTAL   "


class TestProcessAndWriteFile:
    """Test class for _process_and_write_file function."""

    @patch('dags.util.fixed_width_utils.storage.Client')
    def test_process_and_write_file_basic(self, mock_storage_client):
        """Test basic file processing and writing without headers/footers."""
        # Setup mocks
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_source_blob = MagicMock()

        mock_storage_client.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_source_blob

        # Mock source file content
        mock_file_content = "123,John,123.45\n456,Jane,456.78\n"
        mock_source_blob.open.return_value.__enter__.return_value = StringIO(mock_file_content)

        # Setup configurations
        source_config = {
            "bucket": "test-bucket",
            "folder_prefix": "test/folder",
            "file_name": "test.csv",
            "delimiter": ","
        }

        dest_config = {
            "bucket": "dest-bucket",
            "folder_prefix": "output/folder",
            "file_name": "output.txt"
        }

        schema = [
            {'name': 'ID', 'length': 5, 'pad_value': '0', 'pad_type': 'left', 'type': 'string'},
            {'name': 'NAME', 'length': 10, 'pad_value': ' ', 'pad_type': 'right', 'type': 'string'},
            {'name': 'AMOUNT', 'length': 8, 'pad_value': '0', 'pad_type': 'right', 'type': 'decimal'}
        ]

        # Call the function
        _process_and_write_file(source_config, dest_config, schema, ",", mock_client)

        # Verify upload was called
        upload_call = mock_bucket.blob.return_value.upload_from_string
        assert upload_call.called
        upload_args = upload_call.call_args
        uploaded_content = upload_args[0][0]

        # Verify the content contains the formatted data
        assert "00123John      123.4500" in uploaded_content
        assert "00456Jane      456.7800" in uploaded_content

    @patch('dags.util.fixed_width_utils.storage.Client')
    def test_process_and_write_file_with_headers_and_footers(self, mock_storage_client):
        """Test file processing with headers and footers."""
        # Setup mocks
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_source_blob = MagicMock()

        mock_storage_client.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_source_blob

        # Mock source file content
        mock_file_content = "123,John,123.45\n456,Jane,456.78\n"
        mock_source_blob.open.return_value.__enter__.return_value = StringIO(mock_file_content)

        # Setup configurations with headers and footers
        source_config = {
            "bucket": "test-bucket",
            "folder_prefix": "test/folder",
            "file_name": "test.csv",
            "delimiter": ",",
            "header": "HEADER_LINE",
            "footer": "FOOTER_LINE"
        }

        dest_config = {
            "bucket": "dest-bucket",
            "folder_prefix": "output/folder",
            "file_name": "output.txt"
        }

        schema = [
            {'name': 'ID', 'length': 5, 'pad_value': '0', 'pad_type': 'left', 'type': 'string'},
            {'name': 'NAME', 'length': 10, 'pad_value': ' ', 'pad_type': 'right', 'type': 'string'},
            {'name': 'AMOUNT', 'length': 8, 'pad_value': '0', 'pad_type': 'right', 'type': 'decimal'}
        ]

        # Call the function
        _process_and_write_file(source_config, dest_config, schema, ",", mock_client)

        # Verify upload was called
        upload_call = mock_bucket.blob.return_value.upload_from_string
        assert upload_call.called
        upload_args = upload_call.call_args
        uploaded_content = upload_args[0][0]

        # Verify the content contains headers, data, and footers
        assert "HEADER_LINE" in uploaded_content
        assert "FOOTER_LINE" in uploaded_content
        assert "00123John      123.4500" in uploaded_content
        assert "00456Jane      456.7800" in uploaded_content


class TestProcessAndWriteFileWithSkipHeader:
    """Test class for _process_and_write_file function with skip_header functionality."""

    @pytest.fixture
    def mock_storage_setup(self):
        """Fixture to set up common storage mocks."""
        with patch('dags.util.fixed_width_utils.storage.Client') as mock_storage_client:
            mock_client = MagicMock()
            mock_bucket = MagicMock()
            mock_source_blob = MagicMock()

            mock_storage_client.return_value = mock_client
            mock_client.bucket.return_value = mock_bucket
            mock_bucket.blob.return_value = mock_source_blob

            yield mock_client, mock_bucket, mock_source_blob

    def setup_mock_file_content(self, mock_source_blob, file_content):
        """Helper method to setup mock file content."""
        def mock_open(mode):
            mock_context = MagicMock()
            mock_context.__enter__.return_value = StringIO(file_content)
            mock_context.__exit__.return_value = None
            return mock_context

        mock_source_blob.open = mock_open

    @pytest.fixture
    def common_schema(self):
        """Fixture providing common schema for testing."""
        return [
            {'name': 'ID', 'length': 5, 'pad_value': '0', 'pad_type': 'left', 'type': 'string'},
            {'name': 'NAME', 'length': 10, 'pad_value': ' ', 'pad_type': 'right', 'type': 'string'},
            {'name': 'AMOUNT', 'length': 8, 'pad_value': '0', 'pad_type': 'right', 'type': 'decimal'}
        ]

    @pytest.fixture
    def common_dest_config(self):
        """Fixture providing common destination config for testing."""
        return {
            "bucket": "dest-bucket",
            "folder_prefix": "output/folder",
            "file_name": "output.txt"
        }

    def test_process_and_write_file_with_skip_header_false(self, mock_storage_setup, common_schema, common_dest_config):
        """Test file processing with skip_header=False (write first row with delimiter removed)."""
        mock_client, mock_bucket, mock_source_blob = mock_storage_setup

        # Mock source file content with header row
        mock_file_content = "ID,NAME,AMOUNT\n123,John,123.45\n"
        self.setup_mock_file_content(mock_source_blob, mock_file_content)

        # Setup configurations with skip_header=False
        source_config = {
            "bucket": "test-bucket",
            "folder_prefix": "test/folder",
            "file_name": "test.csv",
            "delimiter": ",",
            "skip_header": False
        }

        # Call the function
        _process_and_write_file(source_config, common_dest_config, common_schema, ",", mock_client)

        # Verify upload was called
        upload_call = mock_bucket.blob.return_value.upload_from_string
        assert upload_call.called
        upload_args = upload_call.call_args
        uploaded_content = upload_args[0][0]

        # Verify the content contains the header row (without delimiter) and formatted data
        assert "IDNAMEAMOUNT" in uploaded_content  # Header with delimiter removed
        assert "00123John      123.4500" in uploaded_content  # Formatted data

    def test_process_and_write_file_with_skip_header_true(self, mock_storage_setup, common_schema, common_dest_config):
        """Test file processing with skip_header=True (skip first row)."""
        mock_client, mock_bucket, mock_source_blob = mock_storage_setup

        # Mock source file content with header row
        mock_file_content = "ID,NAME,AMOUNT\n123,John,123.45\n"
        self.setup_mock_file_content(mock_source_blob, mock_file_content)

        # Setup configurations with skip_header=True
        source_config = {
            "bucket": "test-bucket",
            "folder_prefix": "test/folder",
            "file_name": "test.csv",
            "delimiter": ",",
            "skip_header": True
        }

        # Call the function
        _process_and_write_file(source_config, common_dest_config, common_schema, ",", mock_client)

        # Verify upload was called
        upload_call = mock_bucket.blob.return_value.upload_from_string
        assert upload_call.called
        upload_args = upload_call.call_args
        uploaded_content = upload_args[0][0]

        # Verify the content does NOT contain the header row and only has formatted data
        assert "IDNAMEAMOUNT" not in uploaded_content
        assert "00123John      123.4500" in uploaded_content

    def test_process_and_write_file_header_validation_success(self, mock_storage_setup, common_schema, common_dest_config):
        """Test header validation when first column matches schema."""
        mock_client, mock_bucket, mock_source_blob = mock_storage_setup

        # Mock source file content with matching header
        mock_file_content = "ID,NAME,AMOUNT\n123,John,123.45\n"
        self.setup_mock_file_content(mock_source_blob, mock_file_content)

        # Setup configurations with skip_header=False
        source_config = {
            "bucket": "test-bucket",
            "folder_prefix": "test/folder",
            "file_name": "test.csv",
            "delimiter": ",",
            "skip_header": False
        }

        # Call the function - should not raise exception
        _process_and_write_file(source_config, common_dest_config, common_schema, ",", mock_client)

        # Verify upload was called
        upload_call = mock_bucket.blob.return_value.upload_from_string
        assert upload_call.called

    def test_process_and_write_file_header_validation_failure(self, mock_storage_setup, common_schema, common_dest_config):
        """Test that header is written."""
        mock_client, mock_bucket, mock_source_blob = mock_storage_setup

        # Mock source file content with mismatched header
        mock_file_content = "ID,NAME,AMOUNT\n123,John,123.45\n"
        self.setup_mock_file_content(mock_source_blob, mock_file_content)

        # Setup configurations with skip_header=False
        source_config = {
            "bucket": "test-bucket",
            "folder_prefix": "test/folder",
            "file_name": "test.csv",
            "delimiter": ",",
            "skip_header": False
        }

        _process_and_write_file(source_config, common_dest_config, common_schema, ",", mock_client)

        # Verify upload was called
        upload_call = mock_bucket.blob.return_value.upload_from_string
        assert upload_call.called
        upload_args = upload_call.call_args
        uploaded_content = upload_args[0][0]

        # Verify the header is written without validation
        assert "IDNAMEAMOUNT" in uploaded_content  # Header with delimiter removed, no validation
        assert "00123John      123.4500" in uploaded_content  # Formatted data

    def test_process_and_write_file_skip_header_not_provided(self, mock_storage_setup, common_schema, common_dest_config):
        """Test file processing when skip_header is not provided (process first row as data)."""
        mock_client, mock_bucket, mock_source_blob = mock_storage_setup

        # Mock source file content
        mock_file_content = "ID,NAME,AMOUNT\n123,John,123.45\n456,Jane,456.78\n"
        self.setup_mock_file_content(mock_source_blob, mock_file_content)

        # Setup configurations without skip_header (not provided)
        source_config = {
            "bucket": "test-bucket",
            "folder_prefix": "test/folder",
            "file_name": "test.csv",
            "delimiter": ","
            # skip_header not provided
        }

        # Call the function
        _process_and_write_file(source_config, common_dest_config, common_schema, ",", mock_client)

        # Verify upload was called
        upload_call = mock_bucket.blob.return_value.upload_from_string
        assert upload_call.called
        upload_args = upload_call.call_args
        uploaded_content = upload_args[0][0]

        # Verify the content contains all rows as formatted data (no header processing)
        assert "000IDNAME      AMOUNT00" in uploaded_content  # First row formatted as data
        assert "00123John      123.4500" in uploaded_content  # Second row formatted as data
        assert "00456Jane      456.7800" in uploaded_content  # Third row formatted as data
