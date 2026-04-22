import os
import pandas as pd
import logging

import random
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any
import json
from io import StringIO
from util.gcs_utils import write_file_bytes


# Set up logging
logger = logging.getLogger(__name__)

generated_pools = {}


def generate_random_int(column: Dict[str, Any]) -> int:
    """Generate a random unique integer within a specified range."""
    column_name = column['name']
    min_value = column.get('min', 0)
    max_value = column.get('max', 100)
    num_records = column.get('num_records', 1)
    range_key = (column_name, min_value, max_value)
    if range_key not in generated_pools:
        if max_value - min_value + 1 < num_records:
            raise ValueError("given Range is too small ")
        pool = random.sample(range(min_value, max_value + 1), num_records)
        generated_pools[range_key] = pool

    if not generated_pools[range_key]:
        raise ValueError("All possible unique integers have been used.")
    return generated_pools[range_key].pop()


def generate_options(column: Dict[str, Any]) -> str:
    """Randomly select an option from a predefined list."""
    return random.choice(column['values'])


def generate_decimal(column: Dict[str, Any]) -> float:
    """Generate a random decimal within a specified range and precision."""
    min_value = column.get('min', 0)
    max_value = column.get('max', 100)
    scale_value = column.get('scale', 2)
    value = random.uniform(min_value, max_value)
    return round(value, scale_value)


def generate_date(column: Dict[str, Any]) -> str:
    """Generate a random date within a specified range and format."""
    start_date_str = column.get('start_date', '2000-01-01')
    end_date_str = column.get('end_date', '2024-12-31')
    date_format = column.get('date_format', '%Y-%m-%d')

    # Parse the start and end date strings to date objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    # Calculate the random date directly using timedelta
    random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

    # Return the formatted random date
    return random_date.strftime(date_format)


def generate_percentage(column: Dict[str, Any]) -> str:
    """Generate a random percentage within a specified range and precision."""
    min_value = column.get('min', 0)
    max_value = column.get('max', 100)
    precision = column.get('precision', 2)
    value = random.uniform(min_value, max_value)
    return f"{value:.{precision}f}%"


# Dictionary mapping column types to their respective generation functions
column_generators = {
    'random_unique_int': generate_random_int,
    'options': generate_options,
    'decimal': generate_decimal,
    'date': generate_date,
    'percentage': generate_percentage
}


def generate_mock_data(columns: List[Dict[str, Any]], num_records: int) -> List[Dict[str, Any]]:
    """Generate a list of mock data rows based on the columns configuration."""
    data_rows = []
    for column in columns:
        column['num_records'] = num_records
    for _ in range(num_records):
        data_row = {}
        for column in columns:
            column_name = column['name']
            column_type = column['type']
            try:
                if column_type in column_generators:
                    data_row[column_name] = column_generators[column_type](
                        column)
            except Exception as e:
                logging.error(
                    f"Failed to generate data for column '{column_name}': {e}")
                data_row[column_name] = None
        data_rows.append(data_row)
    return data_rows


def generate_dataset(
        config_str: str,
        bucket: str,
        prefix: str, file_name: str,
        **context) -> None:
    """Generate a dataset based on the configuration and save it to a file."""

    config = json.loads(config_str)
    columns = config.get('columns', [])
    num_records = config.get('num_records', 20)
    include_header = config.get('header', {}).get('include_header', False)
    header_line = config.get('header', {}).get('header_line', '')
    include_trailer = config.get('trailer', {}).get('include_trailer', False)
    trailer_line_template = config.get('trailer', {}).get('trailer_line', '')
    csv_options = config.get('csv_options', {})
    include_header_csv = csv_options.get('include_header_csv', False)
    separator = csv_options.get('separator', '|')

    try:
        data_rows = generate_mock_data(columns, num_records)
        logging.info(f"Generated {num_records} mock data records.")
    except Exception as e:
        logging.error(f"Error generating mock data: {e}")
        return

    try:
        df = pd.DataFrame(data_rows)
        content_buffer = StringIO()
        if include_header and header_line:
            num_records += 1
            content_buffer.write(header_line + '\n')

        if include_header_csv:
            num_records += 1

        df.to_csv(
            content_buffer,
            index=False,
            sep=separator,
            header=include_header_csv)

        if include_trailer:
            num_records += 1
            trailer_line = trailer_line_template.format(
                num_records=num_records)
            content_buffer.write(trailer_line + '\n')

        combined_content = content_buffer.getvalue().encode('utf-8')
        write_file_bytes(bucket, prefix, file_name, combined_content)

        logging.info(f"Dataset saved to '{file_name}' successfully.")
        print(f"Generated {num_records} records saved to {file_name}")
    except Exception as e:
        logging.error(f"Error saving dataset to '{file_name}': {e}")
