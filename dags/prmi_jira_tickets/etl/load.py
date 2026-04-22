"""
This module defines methods used to load files to landing zone
at different stages of the ETL workflow.
"""

import logging
from dataclasses import fields as dataclass_fields
from datetime import datetime
from typing import List, Any, get_args, get_origin

from google.cloud import bigquery

from prmi_jira_tickets.config import PRMIJiraTicketsConfig as Config
from util.bq_utils import get_bq_schema, load_records_to_bq, run_bq_query

logger = logging.getLogger(__name__)


def load_records(records: List[Any], table_id: str, chunk: int = 0, batch_size: int = 0, force_truncate: bool = False) -> None:
    """
    This function loads records from JIRA cloud to BigQuery's landing zone using MERGE operation.
    This will UPDATE existing records (based on issue_key) or INSERT new ones.

    Strategy:
    1. Load records to a temporary staging table
    2. Use MERGE to update existing records or insert new ones
    3. Clean up the temporary table

    :param records: List of dataclass objects to be uploaded.
    :param table_id: The destination table.
    :param chunk: If we are using chunk loading, the index of the chunk.
    :param batch_size: The maximum number of records to load at once.
    :param force_truncate: Flag for always truncating the table before writing (not used with MERGE)
    """
    if len(records) == 0:
        logger.info("No records to load")
        return

    # Create a temporary staging table for the new/updated records
    # Use chunk number and timestamp to ensure uniqueness
    import time
    temp_table_id = f"{table_id}_temp_{chunk}_{int(time.time())}"

    logger.info(f"Loading {len(records)} records to temporary staging table: {temp_table_id}")

    # Load records to temporary table (always truncate temp table)
    if batch_size != 0 and len(records) > batch_size:
        batches = ((len(records) - 1) // batch_size) + 1
        load_method = bigquery.WriteDisposition.WRITE_TRUNCATE
        for i, records_batch in enumerate([
            records[j * batch_size:min((j + 1) * batch_size, len(records))]
            for j in range(batches)
        ]):
            load_records_to_bq(records=records_batch, table_id=temp_table_id, load_method=load_method, timestamp=True)
            load_method = bigquery.WriteDisposition.WRITE_APPEND  # Append for subsequent batches
    else:
        load_records_to_bq(
            records=records,
            table_id=temp_table_id,
            load_method=bigquery.WriteDisposition.WRITE_TRUNCATE,
            timestamp=True
        )

    # Ensure target table has all columns before MERGE
    target_schema = ensure_columns_exist(table_id, records[0])
    source_schema = get_bq_schema(records[0], timestamp=True)
    datetime_columns = _get_datetime_columns(records[0])
    validate_schema_compatibility(table_id, source_schema, target_schema, datetime_columns)

    # Perform MERGE operation: UPDATE existing records or INSERT new ones
    logger.info(f"Merging records from {temp_table_id} into {table_id} (UPDATE existing or INSERT new)")

    merge_sql = build_merge_sql(table_id, temp_table_id, records[0], target_schema, datetime_columns)

    try:
        run_bq_query(merge_sql)
        logger.info(f"Successfully merged {len(records)} records into {table_id}")

        # Clean up temporary table
        logger.info(f"Dropping temporary staging table: {temp_table_id}")
        drop_sql = f"DROP TABLE IF EXISTS `{temp_table_id}`"
        run_bq_query(drop_sql)
    except Exception as e:
        logger.error(f"Error during MERGE operation: {e}")
        # Try to clean up temp table even if merge fails
        try:
            drop_sql = f"DROP TABLE IF EXISTS `{temp_table_id}`"
            run_bq_query(drop_sql)
        except Exception as cleanup_error:
            logger.warning(f"Failed to clean up temporary table: {cleanup_error}")
        raise


def ensure_columns_exist(table_id: str, sample_record: Any) -> dict:
    """
    Ensure the target table has all columns required for MERGE.
    Adds any missing columns based on the dataclass schema.
    """
    client = bigquery.Client()
    try:
        table = client.get_table(table_id)
        if _is_terraform_managed_table(table):
            raise RuntimeError(
                f"Refusing to alter terraform-managed table {table_id}."
            )
        existing_columns = {field.name for field in table.schema}
    except Exception as exc:
        logger.error(f"Failed to read schema for {table_id}: {exc}")
        raise

    schema = get_bq_schema(sample_record, timestamp=True)
    for column_name, column_type in schema.items():
        if column_name in existing_columns:
            continue
        if isinstance(column_type, str):
            column_type_name = column_type
        else:
            column_type_name = getattr(column_type, "name", str(column_type))
        column_type_name = str(column_type_name).replace("SqlTypeNames.", "")
        alter_sql = (
            f"ALTER TABLE `{table_id}` "
            f"ADD COLUMN IF NOT EXISTS {column_name} {column_type_name}"
        )
        logger.info(f"Adding missing column to {table_id}: {column_name} {column_type_name}")
        run_bq_query(alter_sql)
    return {field.name: field.field_type for field in client.get_table(table_id).schema}


def _is_terraform_managed_table(table: "bigquery.Table") -> bool:
    labels = table.labels or {}
    managed_by = (
        labels.get("managed_by")
        or labels.get("managed-by")
        or labels.get("provisioned_by")
    )
    if not managed_by:
        return False
    return str(managed_by).lower() in {"terraform", "tf"}


def _normalize_bq_type(type_value: Any) -> str:
    if isinstance(type_value, str):
        return type_value.upper()
    return str(getattr(type_value, "name", type_value)).upper().replace("SQLTYPENAMES.", "")


def _is_datetime_type(type_value: Any) -> bool:
    if type_value is datetime:
        return True
    origin = get_origin(type_value)
    if origin is None:
        return False
    return datetime in get_args(type_value)


def _get_datetime_columns(sample_record: Any) -> set:
    return {
        fld.name
        for fld in dataclass_fields(sample_record.__class__)
        if _is_datetime_type(fld.type)
    }


def validate_schema_compatibility(
    table_id: str, source_schema: dict, target_schema: dict, datetime_columns: set
) -> None:
    """
    Validate that the source and target schemas are compatible for MERGE.
    Raises ValueError if incompatible types are detected.
    """
    mismatches = []
    for column_name, source_type in source_schema.items():
        if column_name not in target_schema:
            continue
        source_type_name = _normalize_bq_type(source_type)
        target_type_name = _normalize_bq_type(target_schema.get(column_name))
        if column_name in datetime_columns:
            if target_type_name not in {"STRING", "DATETIME", "TIMESTAMP"}:
                mismatches.append(
                    (column_name, source_type_name, target_type_name, "datetime-cast")
                )
            continue
        if source_type_name != target_type_name:
            mismatches.append(
                (column_name, source_type_name, target_type_name, "exact-match")
            )
    if mismatches:
        mismatch_lines = ", ".join(
            f"{col} (source {src} -> target {tgt}, rule {rule})"
            for col, src, tgt, rule in mismatches
        )
        logger.error(
            "Schema mismatch detected for %s: %s", table_id, mismatch_lines
        )
        raise ValueError(
            f"Schema mismatch detected for {table_id}: {mismatch_lines}"
        )


def build_merge_sql(
    table_id: str,
    temp_table_id: str,
    sample_record: Any,
    target_schema: dict,
    datetime_columns: set,
) -> str:
    schema = get_bq_schema(sample_record, timestamp=True)
    columns = list(schema.keys())

    def value_expr(column_name: str) -> str:
        if column_name not in datetime_columns:
            return f"source.{column_name}"
        target_type = str(target_schema.get(column_name, "")).upper()
        col_ref = f"source.{column_name}"

        # For datetime_columns (source is DATETIME/TIMESTAMP), only check for NULL
        # DATETIME columns won't contain string 'None' values, so no need to check for it
        null_check = f"{col_ref} IS NULL"
        if target_type == "STRING":
            return f"CASE WHEN {null_check} THEN NULL ELSE CAST({col_ref} AS STRING) END"
        if target_type == "DATETIME":
            return f"CASE WHEN {null_check} THEN NULL ELSE CAST({col_ref} AS DATETIME) END"
        return f"CASE WHEN {null_check} THEN NULL ELSE CAST({col_ref} AS TIMESTAMP) END"

    update_assignments = [
        f"{col} = {value_expr(col)}" for col in columns if col != "issue_key"
    ]
    update_assignments_sql = ",\n                ".join(update_assignments)
    insert_columns = ",\n                ".join(columns)
    insert_values = ",\n                ".join(value_expr(col) for col in columns)

    return f"""
        MERGE `{table_id}` AS target
        USING `{temp_table_id}` AS source
        ON target.issue_key = source.issue_key
        WHEN MATCHED THEN
            UPDATE SET
                {update_assignments_sql}
        WHEN NOT MATCHED THEN
            INSERT (
                {insert_columns}
            )
            VALUES (
                {insert_values}
            )
    """
