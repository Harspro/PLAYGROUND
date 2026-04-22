import logging
from airflow.exceptions import AirflowFailException
from google.cloud import storage
from io import StringIO
from itertools import chain
from decimal import Decimal, InvalidOperation
from util.gcs_utils import gcs_file_exists, read_file_bytes

logger = logging.getLogger(__name__)


"""
Fixed Width File Converter Utility

This module provides functionality to convert delimited files to fixed-width format files.
Headers within the existing files are not supported, please provide source file without headers for processing.
It supports optional headers, footers with dynamic content, and configurable column formatting.

Main Function:
    convert_delimited_to_fixed_width(source_config, dest_config)

Configuration:
    source_config: Dictionary containing source file configuration
        - bucket: GCS bucket name
        - folder_prefix: Path within bucket
        - file_name: Source file name
        - delimiter: Field separator (default: space)
        - column_config: List of column definitions (format: "name:start:length:pad_value:pad_type:data_type")
        - skip_header (optional): Boolean indicating first row behavior:
            - None (not provided): Process first row as data
            - False: Process first row as header (with delimiter removed)
            - True: Skip first row entirely
        - header (optional): Header template string
        - footer (optional): Footer template string

    dest_config: Dictionary containing destination file configuration
        - bucket: GCS bucket name
        - folder_prefix: Path within bucket
        - file_name: Destination file name

    Column Configuration Examples:
        "CLIENT_ID:1:4: :R:string"
        "ACCOUNT_NUMBER:11:19:0:L:decimal"
        "SCORE_RAW:102:7:0:L:string"

Header/Footer Configuration:
(Both header and footer support multiple formats)

    String Format (backward compatible):
        - Static: "HEADER_LINE"
        - Dynamic: "HDR{{ execution_date.now('America/Toronto').strftime('%Y%m%d') }}"
        - With record count: "TOTAL{record_count}"

    Dictionary Array Format (recommended for complex headers/footers):
        - [{"template": "7699SC01"}, {"value": "{record_count}", "length": 10, "pad_value": " ", "pad_type": "right"}, {"template": "76988SC05"}]
        - [{"template": "HDR{{ execution_date.now('America/Toronto').strftime('%Y%m%d') }}"}, {"value": "BATCH", "length": 10, "pad_value": "0", "pad_type": "left"}]

    Single Dictionary Format:
        - {"template": "HDR{{ execution_date.now('America/Toronto').strftime('%Y%m%d') }}"}
        - {"value": "TOTAL", "length": 10, "pad_value": "0", "pad_type": "left", "data_type": "string"}

    Dictionary Configuration Options:
        - template: Basic Jinja2 template /string
        - value: Static value (alternative to template)
        - length: Target length for padding
        - pad_value: Character to use for padding (default: ' ')
        - pad_type: 'left' or 'right' alignment (default: 'right')
        - data_type: data_type of the column (default: 'string')

Usage Examples:
    # String format (backward compatible)
    source_config = {
        "bucket": "source-bucket",
        "folder_prefix": "input/data",
        "file_name": "input.csv",
        "delimiter": ",",
        "skip_header": False,  # Don't skip first row, write it with delimiter removed
        "footer": "TRL{record_count}END",
        "column_config": [
            "ID:1:10:0:L:string",
            "NAME:11:25: :L:string",
            "AMOUNT:36:15:0:R:decimal"
        ]
    }

    # Dictionary array format with mixed static and dynamic content
    source_config = {
        "bucket": "source-bucket",
        "folder_prefix": "input/data",
        "file_name": "input.csv",
        "delimiter": ",",
        "header": [
            {"template": "7699SC01"},
            {"value": "{record_count}", "length": 10, "pad_value": " ", "pad_type": "right"},
            {"template": "76988SC05"}
        ],
        "footer": [
            {"template": "TOTAL{record_count}"},
            {"value": "END", "length": 20, "pad_value": " ", "pad_type": "right"}
        ],
        "column_config": [
            "ID:1:10:0:L:string",
            "NAME:11:25: :L:string",
            "AMOUNT:36:15:0:R:decimal"
        ]
    }

    # Dictionary format with padding
    source_config = {
        "bucket": "source-bucket",
        "folder_prefix": "input/data",
        "file_name": "input.csv",
        "delimiter": ",",
        "header": {
            "template": "HDR{{ 8 * " " }}",
            "length": 15,
            "pad_value": " ",
            "pad_type": "right"
        },
        "footer": {
            "template": "TOTAL{record_count}END",
            "length": 20,
            "pad_value": "0",
            "pad_type": "left",
            "data_type": "string"
        },
        "column_config": [
            "ID:1:10:0:L:string",
            "NAME:11:25: :L:string",
            "AMOUNT:36:15:0:R:decimal"
        ]
    }

     dest_config = {
        "bucket": "dest-bucket",
        "folder_prefix": "output/data",
        "file_name": "output.txt"
    }

    convert_delimited_to_fixed_width(source_config, dest_config)

Error Handling:
    - Raises AirflowFailException for missing configurations or files
    - Validates column configurations and data formats
    - Handles negative decimal values with proper sign positioning
    - Graceful fallback for templates with missing variables
    - Only numeric characters and spaces allowed for decimal types
    - Truncates values if too long to the specified length
"""


def convert_delimited_to_fixed_width(source_config: dict, dest_config: dict, **context):
    """Main callable method to execute the fixed width conversion."""
    logger.info(f"source config: {source_config}")
    logger.info(f"dest config: {dest_config}")
    if source_config is None or dest_config is None:
        raise AirflowFailException("Please provide required source and dest config.")

    src_bucket = source_config["bucket"]
    src_folder_prefix = source_config["folder_prefix"]
    src_file_name = source_config["file_name"]
    src_file_path = f"{src_folder_prefix}/{src_file_name}"

    if not gcs_file_exists(src_bucket, src_file_path):
        raise AirflowFailException(
            f"File {src_folder_prefix}/{src_file_name} not found in bucket {src_bucket}"
        )

    delimiter = source_config.get('delimiter', ' ')

    # Check if source file is 0 bytes
    file_content = read_file_bytes(src_bucket, src_file_path)
    if len(file_content) == 0:
        raise AirflowFailException(
            f"Source file {src_folder_prefix}/{src_file_name} is empty (0 bytes) in bucket {src_bucket}"
        )

    schema = _validate_fixed_width_schema(source_config['column_config'])
    storage_client = storage.Client()
    _process_and_write_file(source_config, dest_config, schema, delimiter, storage_client=storage_client, **context)
    logger.info("Conversion completed successfully.")


def _validate_fixed_width_schema(values):
    schema = []

    for format in values:
        parts = format.split(":")
        if len(parts) < 5:
            raise ValueError(f"Invalid schema format: {parts}, must have at least column_name,start_pos,length"
                             f",pad_value and pad_type")

        col_name, start_pos, length, pad_value, align = parts[:5]
        data_type = parts[5] if len(parts) > 5 else 'string'

        try:
            length_int = int(length)
        except (ValueError, TypeError):
            raise ValueError(f"Invalid length '{length}' for column '{col_name}'. Length must be an integer.")

        schema.append({
            'name': col_name,
            'length': length_int,
            'pad_value': pad_value if pad_value else ' ',
            'pad_type': 'right' if align.upper() == 'R' else 'left',
            'type': data_type.lower()
        })
    logger.info(f"Validated schema: {schema}")
    return schema


def is_decimal_and_negative(value) -> bool:

    try:
        val = Decimal(str(value))
        return val < 0
    except (InvalidOperation, TypeError):
        return False


def _pad(value: str, total_length: int, pad_value: str, pad_type: str) -> str:

    if len(value) >= total_length:
        return value[:total_length]

    padding_needed = total_length - len(value)
    if len(pad_value) == 1:
        padding = pad_value * padding_needed
    else:
        pad_count = padding_needed // len(pad_value)
        remainder = padding_needed % len(pad_value)
        padding = pad_value * pad_count
        if remainder > 0:
            padding += pad_value[:remainder]

    if pad_type == 'left':
        return padding + value
    else:
        return value + padding


def _format_and_pad_value(value: str, total_length: int, pad_value: str, pad_type: str, data_type: str = 'string') -> str:

    if 'decimal' in data_type:
        if not (pad_value.isdigit() or pad_value == ' '):
            raise ValueError(f"Invalid padding for decimal type: '{pad_value}'. Decimal types must use numeric padding (0-9) or space padding.")

    if 'decimal' in data_type and is_decimal_and_negative(value):

        decimal_val = str(abs(Decimal(str(value))))
        max_digits = total_length - 1

        if len(decimal_val) > max_digits:
            decimal_val = decimal_val[:max_digits]

        padded = _pad(decimal_val, max_digits, pad_value, pad_type)
        return '-' + padded
    else:
        str_value = str(value)
        if len(str_value) > total_length:
            str_value = str_value[:total_length]
        return _pad(str_value, total_length, pad_value, pad_type)


def _format_fixed_width(values, schema, line_number):
    if len(values) != len(schema):
        raise ValueError(
            f"[Line {line_number}] Column count mismatch: Expected {len(schema)} values, got {len(values)}"
        )

    fixed_row = ""
    for i, col_def in enumerate(schema):
        value = values[i] if i < len(values) else ''
        length = col_def['length']
        pad_value = col_def['pad_value']
        pad_type = col_def['pad_type']
        data_type = col_def.get('type', 'string')

        formatted = _format_and_pad_value(value, length, pad_value, pad_type, data_type)
        fixed_row += formatted

    return fixed_row


def _process_header_footer(header_footer_config, record_count=0, **context):
    """
    Process header or footer configuration that can be:
    - String: Static or template string (backward compatibility)
    - List of dictionaries: Array of dictionary configurations for complex header/footer
    """
    if not header_footer_config:
        return []

    lines = []
    header_footer_val = ""

    if isinstance(header_footer_config, str):
        lines.append(_render_template(header_footer_config, record_count, **context))

    elif isinstance(header_footer_config, list):
        for item in header_footer_config:
            if isinstance(item, dict):
                processed_line = _process_header_footer_item(item, record_count, **context)
                header_footer_val += processed_line
            else:
                processed_line = _render_template(str(item), record_count, **context)
                header_footer_val += processed_line

        lines.append(header_footer_val)

    elif isinstance(header_footer_config, dict):
        processed_line = _process_header_footer_item(header_footer_config, record_count, **context)
        lines.append(processed_line)

    return lines


def _process_header_footer_item(item, record_count=0, **context):
    """
    Process a single header/footer item (string or dictionary)
    """
    if isinstance(item, str):
        return _render_template(item, record_count, **context)

    elif isinstance(item, dict):
        template = item.get('template', '')
        value = item.get('value', '')
        length = item.get('length', 0)
        pad_value = item.get('pad_value', ' ')
        pad_type = item.get('pad_type', 'right')
        if pad_type not in ('left', 'right'):
            raise ValueError(f"Invalid pad_type '{pad_type}'. Must be 'left' or 'right'.")
        data_type = item.get('data_type', 'string')

        if template:
            rendered_value = _render_template(template, record_count, **context)
        else:
            rendered_value = _render_template(value, record_count, **context)

        if length > 0:
            return _format_and_pad_value(rendered_value, length, pad_value, pad_type, data_type)
        else:
            return rendered_value

    return str(item)


def _render_template(template_str, record_count=0, **context):
    """
    Render template string with available variables and functions
    """
    if not template_str:
        return ''
    # Simple string replacement for supported placeholders
    footer_replacement_dict = {
        '{record_count}': str(record_count),
        'record_count': str(record_count)
    }

    preprocessed_str = template_str
    for placeholder, value in footer_replacement_dict.items():
        preprocessed_str = preprocessed_str.replace(placeholder, value)

    return preprocessed_str


def _process_and_write_file(source_config, dest_config, schema, delimiter, storage_client, **context):

    src_bucket = source_config["bucket"]
    src_folder_prefix = source_config["folder_prefix"]
    src_file_name = source_config["file_name"]
    src_file_path = f"{src_folder_prefix}/{src_file_name}"

    source_blob = storage_client.bucket(src_bucket).blob(src_file_path)

    output_stream = StringIO()
    record_count = 0

    skip_header = source_config.get("skip_header")
    with source_blob.open("r") as infile:
        first_line = infile.readline()

        if first_line and skip_header is False:
            header_values = first_line.strip().split(delimiter)
            header_row = "".join(header_values)
            output_stream.write(header_row + "\n")
            logger.info(f"Writing provided header: {header_row}")

        # Process header lines
        header_config = source_config.get("header")
        if header_config:
            header_lines = _process_header_footer(header_config, record_count, **context)
            for line in header_lines:
                output_stream.write(line + "\n")

        # Build iterator & index for data lines
        if not first_line:
            lines_iter = infile
            start_index = 1
        elif skip_header is True or skip_header is False:
            # first line consumed and not part of data
            lines_iter = infile
            start_index = 2
        else:
            # skip_header not provided: process first line as data too
            lines_iter = chain([first_line], infile)
            start_index = 1

        # Process Data segment
        for index, line in enumerate(lines_iter, start=start_index):
            values = line.strip().split(delimiter)
            fixed_line = _format_fixed_width(values, schema, index)
            output_stream.write(fixed_line + '\n')
            record_count += 1

    # Process footer
    footer_config = source_config.get("footer")
    if footer_config:
        footer_lines = _process_header_footer(footer_config, record_count, **context)
        for line in footer_lines:
            output_stream.write(line + "\n")

    dest_bucket = dest_config['bucket']
    dest_path = dest_config['folder_prefix']
    dest_file = dest_config['file_name']
    full_dest_path = f"{dest_path}/{dest_file}"

    bucket = storage_client.bucket(dest_bucket)
    blob = bucket.blob(full_dest_path)

    blob.upload_from_string(
        output_stream.getvalue(),
        content_type="application/octet-stream"
    )

    logger.info(f" Fixed-width file written successfully to: gs://{dest_bucket}/{full_dest_path}")
