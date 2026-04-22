import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any
from util.gcs_utils import read_file_bytes, read_file
from util.miscutils import (read_yamlfile_env_suffix, read_env_filepattern)
from airflow import DAG, settings
from alm_processing.utils import constants
from etl_framework.utils.misc_util import get_latest_file
import re
from util.datetime_utils import current_datetime_toronto

logger = logging.getLogger(__name__)


def get_skip_validation(config, context):
    """Determines the value of skip_validation with precedence to DAG run context."""
    skip_validation_context = context['dag_run'].conf.get('skip_validation')
    if skip_validation_context is not None:
        logger.info(f"Overriding YAML 'skip_validation' with DAG Run Context value: {skip_validation_context}")
        return skip_validation_context
    skip_validation_yaml = config[constants.INBOUND_FILE].get(constants.SKIP_VALIDATION, False)
    logger.info(f"Using 'skip_validation' from YAML configuration: {skip_validation_yaml}")
    return skip_validation_yaml


def get_validation_config(config):
    """Reads the validation configuration YAM file and returns the parsed content"""
    validation_config_dir = f'{settings.DAGS_FOLDER}/alm_processing/validator/config'
    validation_config_filename = config[constants.INBOUND_FILE].get(constants.VALIDATION_FILE, None)
    if not validation_config_filename:
        raise ValueError("Validation configuration filename is missing in the config")
    validation_config = read_yamlfile_env_suffix(f'{validation_config_dir}/{validation_config_filename}', constants.DEPLOY_ENV, constants.DEPLOY_ENV_SUFFIX)
    logger.info(f"Validation configuration loaded successfully from {validation_config_filename}.")
    return validation_config


def validate_vendor(config, **context):
    """Validates the vendor_name and push it to xcomm"""
    logger.info(config)

    bucket_name = context['dag_run'].conf.get('bucket')
    object_name = context['dag_run'].conf.get('name')
    if not bucket_name or not object_name:
        raise ValueError("Missing required 'bucket' or 'name' in DAG run configuration.")
    folder_name, file_name, *_ = object_name.split('/') + ['']
    vendor_name_lower = file_name.split('_')[0]
    vendor_name_upper = vendor_name_lower.upper()
    logging.info(f"Extracted Vendor Name lower case: {vendor_name_lower}")
    skip_validation = get_skip_validation(config, context)
    if skip_validation:
        logging.info("Skipping vendor validation as 'skip_validation' is set to True in the config.")
    else:
        valid_vendors = config[constants.VENDOR]
        if vendor_name_lower not in valid_vendors:
            raise ValueError(f"Vendor name '{vendor_name_lower}' is not in the allowed vendor list: {valid_vendors}")
    context['ti'].xcom_push(key='vendor_name_lower', value=vendor_name_lower)
    context['ti'].xcom_push(key='vendor_name_upper', value=vendor_name_upper)
    logging.info(f"vendor name lowercase:'{vendor_name_lower}' pushed to XCom.")


def is_valid_filename(filename, pattern):
    """Compare expected pattern with received file name."""
    if not re.match(pattern, filename):
        raise ValueError(f"File name '{filename}' does not match the expected pattern: {pattern}")
    return True


def validate_file_name(config, **context):
    """Validates the latest File received naming pattern"""
    logger.info(config)

    bucket_name = context['dag_run'].conf.get('bucket')
    object_name = context['dag_run'].conf.get('name')
    if not bucket_name or not object_name:
        raise ValueError("Missing required 'bucket' or 'name' in DAG run configuration.")

    # Push filename to XCom before validation
    context['ti'].xcom_push(key='latest_filename', value=object_name)
    logging.info(f"File name '{object_name}' pushed to XCom.")

    skip_validation = get_skip_validation(config, context)
    if skip_validation:
        logger.info("Skipping file validation as 'skip_validation' is set to True")
        return

    try:
        days_delta = config[constants.FILE].get(constants.DAYS_DELTA, 0)
        prefix_date = current_datetime_toronto() + timedelta(days=days_delta)
        current_date = prefix_date.strftime('%Y%m%d')
    except Exception as e:
        raise ValueError(f"Error processing date with days_delta {days_delta}: {e}")

    extension = read_env_filepattern(config[constants.FILE][constants.EXTENSION], constants.DEPLOY_ENV)
    format = config[constants.INBOUND_FILE][constants.FORMAT]
    file_prefix_str = f"{config[constants.INBOUND_FOLDER]}/{config[constants.INBOUND_FILE][constants.PREFIX]}"
    validation_config = get_validation_config(config)
    file_regex_template = validation_config.get(constants.FILE_REGEX_PATTERN)

    if not file_regex_template:
        raise ValueError("File regex pattern is not defined in the validation configuration.")

    regex_kwargs = {
        'file_prefix_str': re.escape(file_prefix_str),
        'current_date': re.escape(current_date),
        'extension': re.escape(extension),
        'format': re.escape(format)
    }

    try:
        file_regex_pattern = file_regex_template.format(**regex_kwargs)
    except KeyError as e:
        raise KeyError(f"Missing required placeholder in regex pattern: {e}")
    try:
        is_valid_filename(object_name, file_regex_pattern)
        logging.info(f"File name '{object_name}' matches the expected pattern {file_regex_pattern}.")
    except ValueError as e:
        logging.error(f"File name '{object_name}' validation failed: {e}")
        raise


def validate_file_size(config, **context):
    """Validates that the file size is greater than zero in a cloud storage bucket."""
    skip_validation = get_skip_validation(config, context)
    if skip_validation:
        logger.info("Skipping file validation as 'skip_validation' is set to True")
        return
    latest_filename = context['ti'].xcom_pull(task_ids='validate_file_name', key='latest_filename')
    source_bucket = config[constants.INBOUND_BUCKET]
    try:
        latest_file_bytes = read_file_bytes(source_bucket, latest_filename)
        latest_file_size = len(latest_file_bytes)
        logging.info(f"Submit file size: {latest_file_size} bytes for file '{latest_filename}' in bucket '{source_bucket}'.")
    except Exception as e:
        logging.error(f"Error reading file '{latest_filename}' from bucket '{source_bucket}': {e}")
        raise
    if latest_file_size > 0:
        logging.info(f"File '{latest_filename}' is present in bucket '{source_bucket}' and has a size greater than 0 bytes.")
    else:
        logging.error(f"File '{latest_filename}' is present but has a size of zero bytes.")
        raise ValueError(f"File '{latest_filename}' in bucket '{source_bucket}' is of zero bytes.")


def validate_header(lines, separator, header_config):
    """Validates the header of the file based on the YAML configuration."""
    header_line = lines[0].strip()
    line_parts = header_line.split(separator)

    for element_name, element_config in header_config.items():
        validation_type = element_config.get("validation", "none")
        description = element_config.get("description", element_name)

        # Skip validation if set to 'none'
        if validation_type == "none":
            logging.info(f"Skipping validation for '{description}'.")
            continue

        # Get the column index for validation
        column = element_config.get("column")
        if column is None or column >= len(line_parts):
            raise ValueError(f"Header validation error: Missing column {column} for '{description}'.")

        actual_value = line_parts[column].strip()
        expected = element_config.get("expected")

        # Perform validations
        if validation_type == "exact":
            if actual_value != expected:
                raise ValueError(f"Header validation failed for '{description}': Expected '{expected}', got '{actual_value}'.")
        elif validation_type == "regex":
            if not re.match(expected, actual_value):
                raise ValueError(f"Header validation failed for '{description}': Value '{actual_value}' does not match pattern '{expected}'.")

    logging.info("Header validated successfully.")


def validate_footer(lines, separator, footer_config, header_count, trailer_count):
    """Validates the footer of the file based on the YAML configuration."""
    footer_line = lines[-1].strip()
    line_parts = footer_line.split(separator)
    total_record_count = len(lines)
    actual_record_count = total_record_count - (header_count + trailer_count)

    for element_name, element_config in footer_config.items():
        validation_type = element_config.get("validation", "none")
        description = element_config.get("description", element_name)

        # Skip validation if set to 'none'
        if validation_type == "none":
            logging.info(f"Skipping validation for '{description}'.")
            continue

        # Get the column index for validation
        column = element_config.get("column")
        if column is None or column >= len(line_parts):
            raise ValueError(f"Footer validation error: Missing column {column} for '{description}'.")

        actual_value = line_parts[column].strip()
        if not actual_value:
            raise ValueError(f"Footer validation error for '{description}': Value is missing or empty.")

        expected = element_config.get("expected")

        # Perform validations
        if validation_type == "exact":
            if actual_value != expected:
                raise ValueError(f"Footer validation failed for '{description}': Expected '{expected}', got '{actual_value}'.")
        elif validation_type == "regex":
            if not re.match(expected, actual_value):
                raise ValueError(f"Footer validation failed for '{description}': Value '{actual_value}' does not match pattern '{expected}'.")
        elif validation_type == "custom" and element_name == "record_count":
            if not actual_value.isdigit():
                raise ValueError(f"Footer validation error for '{description}': Value '{actual_value}' is not a valid integer.")
            if int(actual_value) != actual_record_count:
                raise ValueError(f"Footer validation failed for '{description}': Footer count = {actual_value}, actual count = {actual_record_count}.")

    logging.info("Footer validated successfully.")


def validate_hdr_trl(config, **context):
    """Validates both the header and footer of a file if they are enabled in the configuration."""
    latest_filename = context['ti'].xcom_pull(task_ids='validate_file_name', key='latest_filename')
    skip_validation = get_skip_validation(config, context)
    if skip_validation:
        logger.info("Skipping file validation as 'skip_validation' is set to True")
        return
    validation_config = get_validation_config(config)
    header_count = validation_config.get(constants.HEADER_COUNT, 0)
    trailer_count = validation_config.get(constants.TRAILER_COUNT, 0)

    if not header_count and not trailer_count:
        logging.info("Skipping header and footer validation as both are set to False in the config.")
        return

    separator = config[constants.INBOUND_FILE].get(constants.DELIMITER, '|')
    source_bucket = config[constants.INBOUND_BUCKET]
    file_content = read_file(source_bucket, latest_filename)
    lines = file_content.splitlines()
    if not lines:
        raise ValueError(f"File '{latest_filename}' is empty and lacks content for header/footer validation.")

    lines = [line.strip() for line in lines]
    errors = []

    # Validate header
    if header_count:
        try:
            header_config = validation_config.get(constants.HEADER_CONFIG, {})
            validate_header(lines, separator, header_config)
        except ValueError as e:
            errors.append(f"Header error: {e}")

    # Validate footer
    if trailer_count:
        try:
            footer_config = validation_config.get(constants.FOOTER_CONFIG, {})
            validate_footer(lines, separator, footer_config, header_count, trailer_count)
        except ValueError as e:
            errors.append(f"Footer error: {e}")

    # Raise all errors if any were encountered
    if errors:
        raise ValueError(" | ".join(errors))

    logging.info(f"Header and footer validated successfully with {len(lines)} records.")
