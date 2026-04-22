"""
BigQuery Table Copy Utility using Service Account with Audit Logging.

Provides copy, audit, and impersonation helpers for GA4 table replication.
Uses native BigQuery client API and service account impersonation for cross-project operations.
"""
import logging
from typing import Optional

import pendulum
import google.auth
from google.auth import impersonated_credentials
from google.cloud import bigquery
from google.cloud.bigquery.table import TableReference
from google.cloud.exceptions import NotFound

import util.constants as consts
from util.bq_utils import run_bq_query
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from switch_growth_processing.google_analytics_data_transfer.utils.audit_log import (
    GA4DataTransferAuditUtil,
)
from switch_growth_processing.google_analytics_data_transfer.utils.constants import (
    AUDIT_TABLE_REF,
    CREATE_DISPOSITION,
    LANDING,
    LOAD_TYPE,
    SOURCE,
    START_DATE,
    END_DATE,
    CHUNK_SIZE,
    LOAD_DATE,
    VENDOR,
    LAST_SCHEDULED_RUN,
    SKIP_VENDOR_REPLICATION
)

logger = logging.getLogger(__name__)

# Config key for iteration (single table per DAG, same as operations bigquery_table_copy_with_audit)
TABLE_NAME = consts.TABLE_NAME

# Config keys from repo-level util.constants (shared)
BIGQUERY = consts.BIGQUERY
SERVICE_ACCOUNT = consts.SERVICE_ACCOUNT
PROJECT_ID = consts.PROJECT_ID
DATASET_ID = consts.DATASET_ID
WRITE_DISPOSITION = consts.WRITE_DISPOSITION


def extract_and_validate_config(dag_config: dict, deploy_env: str) -> dict:
    """
    Extract and validate configuration from dag_config.

    :param dag_config: Full DAG configuration dictionary
    :param deploy_env: Deployment environment (dev, uat, prod)
    :return: Dictionary containing all extracted configuration values:
        - audit_table_ref: Audit table reference
        - service_account: Service account with {env} replaced
        - source_project: Source project ID
        - source_dataset: Source dataset ID
        - landing_project: Landing project ID
        - landing_dataset: Landing dataset ID
        - vendor_project: Vendor project ID
        - vendor_dataset: Vendor dataset ID
        - write_disposition: Write disposition (default: WRITE_TRUNCATE)
        - create_disposition: Create disposition (default: CREATE_IF_NEEDED)
    :raises AirflowException: If any required configuration is missing or invalid
    """
    bigquery_config = dag_config.get(BIGQUERY)
    if not bigquery_config:
        raise AirflowException(f"Missing {BIGQUERY} configuration in DAG config")

    audit_table_ref = bigquery_config.get(AUDIT_TABLE_REF)
    if not audit_table_ref:
        raise AirflowException(f"Missing {AUDIT_TABLE_REF} in bigquery config")
    try:
        GA4DataTransferAuditUtil().create_audit_table(audit_table_ref)
    except Exception as e:
        logger.warning("Could not create audit table (may already exist): %s", e)

    service_account = bigquery_config.get(SERVICE_ACCOUNT)
    if not service_account:
        raise AirflowException(f"Missing {SERVICE_ACCOUNT} in bigquery config")
    service_account = service_account.replace('{env}', deploy_env)

    source_config = bigquery_config.get(SOURCE)
    if not source_config:
        raise AirflowException(f"Missing {SOURCE} configuration in bigquery config")
    env_source_config = source_config.get(deploy_env)
    if not env_source_config:
        raise AirflowException(
            f"Missing {SOURCE} configuration for environment '{deploy_env}' in bigquery config"
        )
    source_project = env_source_config.get(PROJECT_ID)
    source_dataset = env_source_config.get(DATASET_ID)
    if not all([source_project, source_dataset]):
        raise AirflowException(
            f"Missing required fields in {SOURCE} config for '{deploy_env}': project_id, dataset_id"
        )

    landing_config = bigquery_config.get(LANDING)
    if not landing_config:
        raise AirflowException(f"Missing {LANDING} configuration in bigquery config")
    landing_project = landing_config.get(PROJECT_ID)
    landing_dataset = landing_config.get(DATASET_ID)

    vendor_config = bigquery_config.get(VENDOR)
    if not vendor_config:
        raise AirflowException(f"Missing {VENDOR} configuration in bigquery config")
    env_vendor_config = vendor_config.get(deploy_env)
    if not env_vendor_config:
        raise AirflowException(
            f"Missing {VENDOR} configuration for environment '{deploy_env}' in bigquery config"
        )
    vendor_project = env_vendor_config.get(PROJECT_ID)
    vendor_dataset = env_vendor_config.get(DATASET_ID)

    write_disposition = bigquery_config.get(WRITE_DISPOSITION, "WRITE_TRUNCATE")
    create_disposition = bigquery_config.get(CREATE_DISPOSITION, "CREATE_IF_NEEDED")
    skip_vendor_replication = dag_config.get(SKIP_VENDOR_REPLICATION, False) or False

    return {
        'audit_table_ref': audit_table_ref,
        'service_account': service_account,
        'source_project': source_project,
        'source_dataset': source_dataset,
        'landing_project': landing_project,
        'landing_dataset': landing_dataset,
        'vendor_project': vendor_project,
        'vendor_dataset': vendor_dataset,
        'write_disposition': write_disposition,
        'create_disposition': create_disposition,
        'skip_vendor_replication': skip_vendor_replication
    }


def build_table_ref(project_id: str, dataset_id: str, table_name: str) -> str:
    """
    Build a BigQuery table reference string.

    :param project_id: Project ID
    :param dataset_id: Dataset ID
    :param table_name: Table name
    :return: Full table reference (project.dataset.table)
    """
    return f"{project_id}.{dataset_id}.{table_name}"


def append_datestamp(table_name: str, date: Optional[pendulum.Date] = None) -> str:
    """
    Append datestamp to table name.
    For daily loads (date=None): uses current date minus 1 day in Toronto timezone.
    For one_time loads (date set): uses the given date from start_date–end_date range.

    :param table_name: Base table name (e.g. 'events' or 'CALENDAR')
    :param date: Optional date. None = daily (use current-1); set = one_time (use that date).
    :return: Table name with datestamp (e.g. 'events_20251201' for one_time, 'events_20260122' for daily)
    """
    base = table_name
    if date is not None:
        if hasattr(date, 'strftime'):
            datestamp = date.strftime('%Y%m%d')
        else:
            datestamp = pendulum.instance(date).strftime('%Y%m%d')
        return f"{base}_{datestamp}"
    # Daily loads (date=None): uses current date minus 1 day in Toronto timezone.
    toronto_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)
    d = pendulum.now(toronto_tz).subtract(days=1)
    return f"{base}_{d.strftime('%Y%m%d')}"


def build_chunk_date_ranges(start_date, end_date, chunk_size_days: int):
    """Yield (chunk_start, chunk_end) inclusive date ranges of chunk_size_days."""
    current = start_date
    while current <= end_date:
        chunk_end = current.add(days=chunk_size_days - 1)
        if chunk_end > end_date:
            chunk_end = end_date
        yield (current, chunk_end)
        current = chunk_end.add(days=1)


def build_iteration_items(dag_config: dict) -> list:
    """
    Build list of items to iterate over based on load_type.

    Daily: one TaskGroup for the single table (table_name from dag_config['bigquery']).
    One_time: chunk_size is required; one TaskGroup per chunk (date range).

    :param dag_config: Full DAG configuration dictionary (contains 'bigquery' and top-level load_type, start_date, end_date, chunk_size)
    :return: List of dicts with task_group_id, table_name, date (daily) or chunk_start_date/chunk_end_date (one_time)
    """
    bigquery_config = dag_config.get(consts.BIGQUERY) or {}
    load_type = dag_config.get(LOAD_TYPE, 'daily')
    table_name = bigquery_config.get(TABLE_NAME)
    if not table_name:
        raise AirflowException(f"Missing {TABLE_NAME} in bigquery config")
    iteration_items = []

    if load_type == 'daily':
        iteration_items.append({
            'task_group_id': table_name,
            'table_name': table_name,
            'date': None,
        })
    elif load_type == 'one_time':
        start_dt = dag_config.get(START_DATE)
        end_dt = dag_config.get(END_DATE)
        if not start_dt or not end_dt:
            raise AirflowException("Missing start_date or end_date for one_time load type")
        start_date = pendulum.parse(str(start_dt)).date()
        end_date = pendulum.parse(str(end_dt)).date()
        if start_date > end_date:
            raise AirflowException(f"start_date ({start_dt}) must be <= end_date ({end_dt})")
        total_days = (end_date - start_date).days + 1
        chunk_size = dag_config.get(CHUNK_SIZE)
        if chunk_size is None:
            raise AirflowException("chunk_size is required for one_time load type")
        chunk_size = int(chunk_size)
        if chunk_size < 1:
            raise AirflowException(f"chunk_size must be >= 1, got {chunk_size}")
        if total_days == 1 and chunk_size != 1:
            raise AirflowException(
                f"When start_date equals end_date (date range is 1 day), chunk_size must be 1, got {chunk_size}"
            )
        if chunk_size > total_days:
            raise AirflowException(
                f"chunk_size ({chunk_size}) cannot exceed date range ({total_days} day(s)) "
                f"between start_date ({start_dt}) and end_date ({end_dt})"
            )
        for chunk_start, chunk_end in build_chunk_date_ranges(start_date, end_date, chunk_size):
            iteration_items.append({
                'task_group_id': f"{chunk_start.strftime('%Y%m%d')}_{chunk_end.strftime('%Y%m%d')}",
                'table_name': table_name,
                'chunk_start_date': chunk_start.strftime('%Y-%m-%d'),
                'chunk_end_date': chunk_end.strftime('%Y-%m-%d'),
            })
    else:
        raise AirflowException(f"Unknown load_type: {load_type}. Must be 'daily' or 'one_time'")
    return iteration_items


def create_chunk_task_group(
    task_group_id: str,
    table_name: str,
    chunk_start_date: str,
    chunk_end_date: str,
    config: dict,
    entry_trigger_rule: Optional[TriggerRule] = None,
) -> TaskGroup:
    """
    Create a TaskGroup for chunk replication mode (one_time loads).

    :param task_group_id: Task group identifier (e.g., "20251201_20251202")
    :param table_name: Base table name
    :param chunk_start_date: Start date of chunk range
    :param chunk_end_date: End date of chunk range
    :param config: Configuration dictionary from extract_and_validate_config
    :param entry_trigger_rule: Optional trigger rule for the entry (check) task; use ALL_DONE so
        downstream groups run even when upstream group fails.
    :return: TaskGroup with check, landing replicate, and vendor replicate tasks
    """
    check_table_existence_chunk_id = f"{task_group_id}.check_table_existence_{task_group_id}"
    check_chunk_kwargs = {}
    if entry_trigger_rule is not None:
        check_chunk_kwargs["trigger_rule"] = entry_trigger_rule

    with TaskGroup(group_id=task_group_id) as tgrp:
        check_chunk_task = PythonOperator(
            task_id=f"check_table_existence_{task_group_id}",
            python_callable=check_source_table_and_fetch_row_count_callable,
            op_kwargs={
                'chunk_start_date': chunk_start_date,
                'chunk_end_date': chunk_end_date,
                'table_name': table_name,
                'source_project': config['source_project'],
                'source_dataset': config['source_dataset'],
                'service_account': config['service_account'],
            },
            **check_chunk_kwargs,
        )
        replicate_landing_chunk_task = PythonOperator(
            task_id=f"replicate_and_validate_for_landing_{task_group_id}",
            python_callable=replicate_and_validate_table_callable,
            op_kwargs={
                'check_source_existence_task_id': check_table_existence_chunk_id,
                'chunk_start_date': chunk_start_date,
                'chunk_end_date': chunk_end_date,
                'table_name': table_name,
                'source_project': config['source_project'],
                'source_dataset': config['source_dataset'],
                'target_project': config['landing_project'],
                'target_dataset': config['landing_dataset'],
                'audit_table_ref': config['audit_table_ref'],
                'write_disposition': config['write_disposition'],
                'create_disposition': config['create_disposition'],
                'service_account': config['service_account'],
            },
        )
        replicate_vendor_chunk_task = PythonOperator(
            task_id=f"replicate_and_validate_for_vendor_{task_group_id}",
            python_callable=replicate_and_validate_table_callable,
            op_kwargs={
                'check_source_existence_task_id': check_table_existence_chunk_id,
                'chunk_start_date': chunk_start_date,
                'chunk_end_date': chunk_end_date,
                'table_name': table_name,
                'source_project': config['source_project'],
                'source_dataset': config['source_dataset'],
                'target_project': config['vendor_project'],
                'target_dataset': config['vendor_dataset'],
                'audit_table_ref': config['audit_table_ref'],
                'write_disposition': config['write_disposition'],
                'create_disposition': config['create_disposition'],
                'service_account': config['service_account'],
                'skip_replication': config.get('skip_vendor_replication', False),
            },
        )
        check_chunk_task >> [replicate_landing_chunk_task, replicate_vendor_chunk_task]

    return tgrp


def create_single_table_task_group(
    task_group_id: str,
    table_name: str,
    config: dict,
    entry_trigger_rule: Optional[TriggerRule] = None,
) -> TaskGroup:
    """
    Create a TaskGroup for single-table replication mode (daily loads).

    Load date is resolved at runtime in the check task: dag_run.conf load_date if present,
    else current_date-1 (Toronto). Check task builds source/landing/vendor refs and pushes
    landing/vendor refs to xcom; replicate tasks pull target ref from xcom.

    :param task_group_id: Task group identifier (e.g., "events")
    :param table_name: Base table name
    :param config: Configuration dictionary from extract_and_validate_config
    :param entry_trigger_rule: Optional trigger rule for the entry (check) task; use ALL_DONE so
        downstream groups run even when upstream group fails.
    :return: TaskGroup with check, landing replicate, and vendor replicate tasks
    """
    check_existence_task_id = f"{task_group_id}.check_table_existence"
    check_task_kwargs = {}
    if entry_trigger_rule is not None:
        check_task_kwargs["trigger_rule"] = entry_trigger_rule

    # Refs built at runtime in check task (load_date from conf or current_date-1)
    check_op_kwargs = {
        'table_name': table_name,
        'source_project': config['source_project'],
        'source_dataset': config['source_dataset'],
        'landing_project': config['landing_project'],
        'landing_dataset': config['landing_dataset'],
        'vendor_project': config['vendor_project'],
        'vendor_dataset': config['vendor_dataset'],
        'service_account': config['service_account'],
    }
    with TaskGroup(group_id=task_group_id) as tgrp:
        table_checking_task = PythonOperator(
            task_id='check_table_existence',
            python_callable=check_source_table_and_fetch_row_count_callable,
            op_kwargs=check_op_kwargs,
            **check_task_kwargs,
        )
        landing_copy_task = PythonOperator(
            task_id='replicate_and_validate_for_landing',
            python_callable=replicate_and_validate_table_callable,
            op_kwargs={
                'check_source_existence_task_id': check_existence_task_id,
                'target_table_xcom_key': 'landing_table_ref',
                'audit_table_ref': config['audit_table_ref'],
                'write_disposition': config['write_disposition'],
                'create_disposition': config['create_disposition'],
                'service_account': config['service_account'],
                'table_name': table_name,
                'target_project': config['landing_project'],
                'target_dataset': config['landing_dataset'],
            },
        )
        vendor_copy_task = PythonOperator(
            task_id='replicate_and_validate_for_vendor',
            python_callable=replicate_and_validate_table_callable,
            op_kwargs={
                'check_source_existence_task_id': check_existence_task_id,
                'target_table_xcom_key': 'vendor_table_ref',
                'audit_table_ref': config['audit_table_ref'],
                'write_disposition': config['write_disposition'],
                'create_disposition': config['create_disposition'],
                'service_account': config['service_account'],
                'table_name': table_name,
                'target_project': config['vendor_project'],
                'target_dataset': config['vendor_dataset'],
                'skip_replication': config.get('skip_vendor_replication', False)
            },
        )
        table_checking_task >> [landing_copy_task, vendor_copy_task]

    return tgrp


def replicate_source_to_target(
    source_table_ref: str,
    target_table_ref: str,
    target_service_account: str,
    write_disposition: str = "WRITE_TRUNCATE",
    create_disposition: str = "CREATE_IF_NEEDED"
) -> None:
    """
    Copy a BigQuery table from source to target using native BigQuery client API with service account impersonation.

    Uses the native copy_job API and service account impersonation for cross-project operations.
    Target project is extracted from target_table_ref automatically.

    :param source_table_ref: Full table reference (project.dataset.table)
    :param target_table_ref: Full table reference (project.dataset.table)
    :param target_service_account: Service account to impersonate for target operations (required)
    :param write_disposition: Write disposition for copy job (default: WRITE_TRUNCATE)
    :param create_disposition: Create disposition for copy job (default: CREATE_IF_NEEDED)
    """
    try:
        target_table_reference = TableReference.from_string(target_table_ref)
        target_project = target_table_reference.project

        logger.info(f"""Starting copy operation: {source_table_ref} -> {target_table_ref}
                    Using service account: {target_service_account}""")

        scopes = [
            consts.SCOPE_CLOUD_PLATFORM,
            consts.SCOPE_BIGQUERY
        ]
        source_credentials, _ = google.auth.default()
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=target_service_account,
            target_scopes=scopes
        )
        client = bigquery.Client(credentials=impersonated_creds, project=target_project)

        source_table = TableReference.from_string(source_table_ref)
        target_table = TableReference.from_string(target_table_ref)

        job_config = bigquery.CopyJobConfig()
        job_config.write_disposition = write_disposition
        job_config.create_disposition = create_disposition

        target_table_obj = bigquery.Table(target_table)
        copy_job = client.copy_table(
            sources=[source_table],
            destination=target_table_obj,
            job_config=job_config
        )
        copy_job.result()
        logger.info(f"Copy job completed: {copy_job.job_id}")

    except Exception as e:
        raise AirflowException(f"Copy operation failed: {source_table_ref} -> {target_table_ref}: {e}")


def check_table_exists(
    table_ref: str,
    service_account: str
) -> bool:
    """
    Check if a BigQuery table exists using service account impersonation.

    :param table_ref: Full table reference (project.dataset.table)
    :param service_account: Service account to impersonate (required)
    :return: True if table exists, False otherwise
    """
    try:
        table_reference = TableReference.from_string(table_ref)
        table_project = table_reference.project

        scopes = [
            consts.SCOPE_CLOUD_PLATFORM,
            consts.SCOPE_BIGQUERY
        ]
        source_credentials, _ = google.auth.default()
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=service_account,
            target_scopes=scopes
        )
        client = bigquery.Client(credentials=impersonated_creds, project=table_project)
        client.get_table(table_reference)
        logger.info(f"Table {table_ref} exists.")
        return True
    except NotFound:
        logger.info(f"Table {table_ref} does not exist.")
        return False


def get_row_count(
    table_ref: str,
    target_service_account: str = None
) -> Optional[int]:
    """
    Get row count using SQL query with service account impersonation.

    :param table_ref: Full table reference (project.dataset.table)
    :param target_service_account: Service account to impersonate for query (required)
    :return: Row count as integer, or None if query fails
    """
    try:
        table_reference = TableReference.from_string(table_ref)
        target_project = table_reference.project

        query = f"SELECT COUNT(*) as row_count FROM `{table_ref}`"
        logger.info(f"Getting row count for {table_ref} using service account: {target_service_account}")

        query_job = run_bq_query(query, target_service_account, target_project)

        for row in query_job.result():
            row_count = row['row_count']
            logger.info(f"Retrieved row count for {table_ref}: {row_count}")
            return row_count

        logger.warning(f"No rows returned from row count query for {table_ref}")
        return 0

    except Exception as e:
        logger.error(f"Row count query failed for {table_ref}: {e}")
        return None


def check_table_and_fetch_row_count(table_ref: str, service_account: str) -> tuple:
    """
    Check if table exists and return (table_ref, row_count) using service account impersonation.
    Shared by single-table and chunk modes.
    """
    if not check_table_exists(table_ref, service_account):
        raise AirflowException(f"Source table does not exist: {table_ref}")
    row_count = get_row_count(table_ref, target_service_account=service_account)
    logger.info("Source table exists: %s, rows: %s", table_ref, row_count)
    return (table_ref, row_count)


def has_successful_target_replication(
    dag_id: str,
    audit_table_ref: str,
    service_account: str,
    target_table_ref: str,
) -> bool:
    """
    Return True if this specific target replication was already successful.

    Checks if a prior run of this DAG has successfully replicated to the specified target_table_ref.
    Since target_table_ref includes the date suffix (e.g., events_20260218), checking for a specific
    dated table naturally prevents duplicates without needing a created_at filter.

    :param dag_id: DAG ID.
    :param audit_table_ref: Full audit table reference (project.dataset.table).
    :param service_account: Service account for BigQuery query (impersonation).
    :param target_table_ref: Full target table reference to check (e.g., landing or vendor table).
    :return: True if a prior run successfully replicated to this target; False otherwise.
    """
    # Escape single quotes for SQL safety
    dag_id_esc = dag_id.replace("'", "''")
    target_table_ref_esc = target_table_ref.replace("'", "''")

    # Check if this specific target replication was successful
    query = f"""
        SELECT dag_run_id
        FROM `{audit_table_ref}`
        WHERE dag_id = '{dag_id_esc}'
          AND target_table_ref = '{target_table_ref_esc}'
          AND status = 'SUCCESS'
        LIMIT 1
    """
    try:
        table_ref = TableReference.from_string(audit_table_ref)
        target_project = table_ref.project
        logger.info(f"Query to check for prior successful target replication: {query}")
        query_job = run_bq_query(query, service_account, target_project)
        rows = list(query_job.result())
        logger.info(f"rows returned from query: {rows}")
        if rows:
            logger.info(
                "Found prior successful target replication: dag_id=%s, target_table_ref=%s, dag_run_id=%s",
                dag_id,
                target_table_ref,
                rows[0]["dag_run_id"],
            )
            return True
        return False
    except Exception as e:
        logger.warning(
            "Audit table check failed for dag_id=%s, target_table_ref=%s: %s; proceeding with replication.",
            dag_id,
            target_table_ref,
            e,
        )
        return False


def check_source_table_and_fetch_row_count_callable(
    service_account: str,
    source_table_ref: str = None,
    chunk_start_date: str = None,
    chunk_end_date: str = None,
    table_name: str = None,
    source_project: str = None,
    source_dataset: str = None,
    landing_project: str = None,
    landing_dataset: str = None,
    vendor_project: str = None,
    vendor_dataset: str = None,
    **context
) -> dict:
    """
    Check if source table(s) exist and get source row count(s) using service account impersonation.

    Two modes (by parameters):
    - Chunk: pass chunk_start_date and chunk_end_date (and table_name, source_project, source_dataset, service_account).
      For each date in [chunk_start_date, chunk_end_date] checks table_name_YYYYMMDD, collects
      row counts, pushes to xcom key 'chunk_source_row_counts'.
    - Single table (daily): no chunk_start_date/chunk_end_date. Pass table_name + source/landing/vendor
      project/dataset. Date is resolved from dag_run.conf load_date if present, else current_date-1 (Toronto).
      Refs are built and pushed to xcom (source_table_ref, landing_table_ref, vendor_table_ref).
    """
    ti = context.get("ti")

    if chunk_start_date is None and chunk_end_date is None:
        # Daily path: resolve load_date first, then optionally run "already loaded" check
        if not all([table_name, source_project, source_dataset, landing_project, landing_dataset,
                    vendor_project, vendor_dataset]):
            raise AirflowException(
                "For daily load: table_name, source/landing/vendor project and dataset are required"
            )
        dag_run = context.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run else {}
        if conf.get(LOAD_DATE):
            load_date = pendulum.parse(str(conf[LOAD_DATE])).date()
            logger.info("Using load_date from dag_run.conf for daily load: %s", conf[LOAD_DATE])
        else:
            load_date = None  # append_datestamp(table_name, None) uses current_date-1 (Toronto)
        source_table_ref = build_table_ref(
            source_project, source_dataset, append_datestamp(table_name, load_date)
        )
        landing_table_ref = build_table_ref(
            landing_project, landing_dataset, append_datestamp(table_name, load_date)
        )
        vendor_table_ref = build_table_ref(
            vendor_project, vendor_dataset, append_datestamp(table_name, load_date)
        )

        try:
            ref, cnt = check_table_and_fetch_row_count(source_table_ref, service_account)

        except AirflowException as e:
            current_time_toronto = pendulum.now(consts.TORONTO_TIMEZONE_ID)

            logger.info(f"""DAG Run ID: { getattr(dag_run, "run_id", None)}
                        DAG Run Type: {getattr(dag_run, "run_type", None)}
                        current_time (Toronto): {current_time_toronto}
                        current_hour (Toronto): {current_time_toronto.hour}""")

            # Early Scheduled runs of the day → SKIP
            if not is_final_scheduled_run(context):
                logger.warning(f"Early Scheduled Run - source table yet to be created: {source_table_ref}. Skipping..")
                raise AirflowSkipException(str(e))
            # Final Scheduled run → FAIL (trigger alert)
            else:
                logger.error(f"Final scheduled run for today and source table still missing: {source_table_ref}.")
                raise AirflowException(str(e))

        # Return full dict so downstream can xcom_pull(task_ids=check_task_id) once and get everything
        return {
            "source_table_ref": ref,
            "source_row_count": cnt,
            "landing_table_ref": landing_table_ref,
            "vendor_table_ref": vendor_table_ref,
        }

    start_dt, end_dt = parse_chunk_dates(chunk_start_date, chunk_end_date)
    result = {}
    current = start_dt
    while current <= end_dt:
        table_name_dated = append_datestamp(table_name, current)
        ref = build_table_ref(source_project, source_dataset, table_name_dated)
        ref, cnt = check_table_and_fetch_row_count(ref, service_account)
        date_str = current.strftime("%Y%m%d")
        result[date_str] = {"source_table_ref": ref, "source_row_count": cnt}
        current = current.add(days=1)
    if ti:
        ti.xcom_push(key="chunk_source_row_counts", value=result)
    return result


def is_final_scheduled_run(context) -> bool:
    dag_run = context.get("dag_run")

    if not dag_run or getattr(dag_run, "run_type", None) != "scheduled":
        return False

    current_time_toronto = pendulum.now(consts.TORONTO_TIMEZONE_ID)
    return current_time_toronto.hour == LAST_SCHEDULED_RUN


def parse_chunk_dates(chunk_start_date, chunk_end_date):
    """Parse chunk date strings into pendulum date objects."""
    start_dt = pendulum.parse(chunk_start_date).date() if isinstance(chunk_start_date, str) else chunk_start_date
    end_dt = pendulum.parse(chunk_end_date).date() if isinstance(chunk_end_date, str) else chunk_end_date
    return start_dt, end_dt


def process_single_table_replication(
    ti,
    check_source_existence_task_id: str,
    audit_table_ref: str,
    dag_run_id: str,
    dag_id: str,
    task_id_for_audit: str,
    target_table_ref: str,
    service_account: str,
    write_disposition: str,
    create_disposition: str,
) -> None:
    """
    Process single table replication (daily loads).

    :param ti: Task instance from context.
    :param check_source_existence_task_id: Task ID to pull xcom from.
    :param audit_table_ref: Audit table reference.
    :param dag_run_id: DAG run ID.
    :param dag_id: DAG ID.
    :param task_id_for_audit: Task ID for audit logging.
    :param target_table_ref: Target table reference.
    :param service_account: Service account for impersonation.
    :param write_disposition: BigQuery write disposition.
    :param create_disposition: BigQuery create disposition.
    :raises AirflowException: If source_table_ref is missing or copy/validation fails.
    """
    check_result = ti.xcom_pull(task_ids=check_source_existence_task_id) or {}
    source_table_ref = check_result.get("source_table_ref")
    source_row_count = check_result.get("source_row_count")

    if not source_table_ref:
        raise AirflowException(
            f"Missing source_table_ref from check task {check_source_existence_task_id}; cannot replicate"
        )

    copy_status = "SUCCESS"
    target_row_count = None
    copy_failed = False
    error_message = None
    try:
        replicate_source_to_target(
            source_table_ref=source_table_ref,
            target_table_ref=target_table_ref,
            target_service_account=service_account,
            write_disposition=write_disposition,
            create_disposition=create_disposition,
        )
        logger.info("Getting target row count for %s", target_table_ref)
        target_row_count = get_row_count(target_table_ref, target_service_account=service_account)
        logger.info(
            "Copy completed: %s -> %s, rows: %s",
            source_table_ref,
            target_table_ref,
            target_row_count,
        )
    except Exception as e:
        error_message = str(e)
        logger.error(
            "Copy or row count failed: %s -> %s: %s",
            source_table_ref,
            target_table_ref,
            error_message,
        )
        copy_failed = True
        copy_status = "FAILED"

    validation_status = get_validation_status(source_row_count, target_row_count)

    # Build error message for validation failures
    error_msg = None
    if copy_failed:
        error_msg = f"Copy failed: {error_message}"
    elif validation_status != "SUCCESS":
        error_msg = f"Validation failed: source_row_count={source_row_count}, target_row_count={target_row_count}"

    copy_results = [
        (
            task_id_for_audit,
            {
                "source_table": source_table_ref,
                "target_table": target_table_ref,
                "source_row_count": source_row_count,
                "target_row_count": target_row_count,
                "copy_status": copy_status,
                "validation_status": validation_status,
                "error_msg": error_msg,
            },
        )
    ]
    audit_util = GA4DataTransferAuditUtil()
    audit_util.log_changes_to_audit_table(audit_table_ref, dag_run_id, dag_id, copy_results)

    if copy_failed or validation_status != "SUCCESS":
        raise AirflowException(
            f"Copy or validation failed: {source_table_ref} -> {target_table_ref} "
            f"(copy={copy_status}, validation={validation_status})"
        )


def process_chunk_replication(
    chunk_data: dict,
    start_dt,
    end_dt,
    task_id_for_audit: str,
    table_name: str,
    source_project: str,
    source_dataset: str,
    target_project: str,
    target_dataset: str,
    service_account: str,
    write_disposition: str,
    create_disposition: str,
    dag_id: str = None,
    audit_table_ref: str = None,
) -> tuple[list, list, int, int]:
    """
    Process chunk replication for a date range.

    For each date in the chunk, checks if that specific target table was already successfully
    replicated today; if yes, skips replication for that date. Otherwise, replicates normally.

    :param chunk_data: Chunk data dictionary from xcom.
    :param start_dt: Start date of chunk range.
    :param end_dt: End date of chunk range.
    :param task_id_for_audit: Task ID for audit logging.
    :param table_name: Base table name.
    :param source_project: Source project ID.
    :param source_dataset: Source dataset ID.
    :param target_project: Target project ID.
    :param target_dataset: Target dataset ID.
    :param service_account: Service account for impersonation.
    :param write_disposition: BigQuery write disposition.
    :param create_disposition: BigQuery create disposition.
    :param dag_id: DAG ID for skip check (optional).
    :param audit_table_ref: Audit table reference for skip check (optional).
    :return: Tuple of (copy_results, failed_tables, skipped_table_count, processed_table_count).
    """
    copy_results = []
    failed_tables = []
    skipped_table_count = 0
    processed_table_count = 0
    current = start_dt

    while current <= end_dt:
        date_str = current.strftime("%Y%m%d")
        info = chunk_data.get(date_str, {})
        src_ref = info.get("source_table_ref") or build_table_ref(
            source_project, source_dataset, append_datestamp(table_name, current)
        )
        source_row_count = info.get("source_row_count")
        tgt_ref = build_table_ref(target_project, target_dataset, append_datestamp(table_name, current))

        # Skip check: check if this specific target table was already successfully replicated
        if dag_id and audit_table_ref:
            if has_successful_target_replication(
                dag_id=dag_id,
                audit_table_ref=audit_table_ref,
                service_account=service_account,
                target_table_ref=tgt_ref,
            ):
                logger.info(
                    "Skipping replication for date %s: target_table_ref=%s already successfully replicated",
                    date_str,
                    tgt_ref,
                )
                skipped_table_count += 1
                current = current.add(days=1)
                continue

        # Table is being processed (not skipped)
        processed_table_count += 1

        copy_status = "FAILED"
        target_row_count = None
        error_msg = None
        try:
            replicate_source_to_target(
                source_table_ref=src_ref,
                target_table_ref=tgt_ref,
                target_service_account=service_account,
                write_disposition=write_disposition,
                create_disposition=create_disposition,
            )
            copy_status = "SUCCESS"
            target_row_count = get_row_count(tgt_ref, target_service_account=service_account)
        except Exception as e:
            error_message = str(e)
            logger.error("Copy failed %s -> %s: %s", src_ref, tgt_ref, error_message)
            error_msg = f"Copy failed: {error_message}"

        validation_status = get_validation_status(source_row_count, target_row_count)
        if copy_status != "SUCCESS" or validation_status != "SUCCESS":
            failed_tables.append(f"{src_ref} -> {tgt_ref} (copy={copy_status}, validation={validation_status})")
            # Update error_msg if validation failed but copy succeeded
            if copy_status == "SUCCESS" and validation_status != "SUCCESS":
                error_msg = f"Validation failed: source_row_count={source_row_count}, target_row_count={target_row_count}"

        copy_results.append(
            (
                task_id_for_audit,
                {
                    "source_table": src_ref,
                    "target_table": tgt_ref,
                    "source_row_count": source_row_count,
                    "target_row_count": target_row_count,
                    "copy_status": copy_status,
                    "validation_status": validation_status,
                    "error_msg": error_msg,
                },
            )
        )
        current = current.add(days=1)

    return copy_results, failed_tables, skipped_table_count, processed_table_count


def replicate_and_validate_table_callable(
    check_source_existence_task_id: str,
    audit_table_ref: str,
    write_disposition: str,
    create_disposition: str,
    service_account: str,
    target_table_xcom_key: str = None,
    chunk_start_date: str = None,
    chunk_end_date: str = None,
    table_name: str = None,
    source_project: str = None,
    source_dataset: str = None,
    target_project: str = None,
    target_dataset: str = None,
    skip_replication: bool = False,
    **context
) -> None:
    """
    Replicate source table(s) to target using impersonation, validate row counts, and log to audit.

    Two modes (by chunk_start_date / chunk_end_date):
    - Single-table (daily): chunk dates are None. Pass target_table_xcom_key (e.g. 'landing_table_ref' or
      'vendor_table_ref'). Pulls full xcom from check_existence_task_id, then target_table_ref = result.get(key).
      For daily runs without load_date in conf: checks if this specific target replication was already
      successful; if so, skips. Runs with load_date in conf never skip (allow overwrite).
      Then calls process_single_table_replication (which gets source_table_ref/source_row_count from same xcom).
    - Chunk (one_time): pass chunk dates and table/source/target config. Pulls chunk_source_row_counts
      from check_existence_task_id xcom. For each date in the chunk: checks if that specific target table
      was already successfully replicated; if yes, skips that date. Otherwise, copies with impersonation,
      gets target row count with impersonation, validates, appends to copy_results. Logs batch, raises
      AirflowException with failed table list if any fail.
    """
    if skip_replication:
        logger.info("Skipping vendor replication (skip_vendor_replication=True)")
        return

    ti = context["ti"]
    dag_run_id = context["dag_run"].run_id
    dag_id = context["dag"].dag_id
    task_id_for_audit = ti.task_id

    if chunk_start_date is None and chunk_end_date is None:
        # Single-table (daily): pull full xcom from check task, get target ref by passed key (e.g. 'landing_table_ref')
        if not target_table_xcom_key:
            raise AirflowException(
                "Single-table replication requires target_table_xcom_key (e.g. 'landing_table_ref' or 'vendor_table_ref')"
            )
        check_result = ti.xcom_pull(task_ids=check_source_existence_task_id) or {}
        target_table_ref = check_result.get(target_table_xcom_key)
        if not target_table_ref:
            raise AirflowException(
                f"Missing target ref for single-table replication (key={target_table_xcom_key}, "
                f"check task id: {check_source_existence_task_id})"
            )

        # Skip check: only for runs WITHOUT load_date in conf (preserve overwrite capability)
        dag_run = context.get("dag_run")
        if dag_run and audit_table_ref and table_name and target_project and target_dataset:
            conf = (dag_run.conf or {}) if dag_run else {}
            if not conf.get(LOAD_DATE):
                # Build expected target_table_ref using yesterday's date for skip check
                tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)
                yesterday_str = pendulum.now(tz).subtract(days=1).date().strftime('%Y%m%d')
                expected_table_name = f"{table_name}_{yesterday_str}"
                expected_target_table_ref = build_table_ref(target_project, target_dataset, expected_table_name)

                if has_successful_target_replication(
                    dag_id=dag_id,
                    audit_table_ref=audit_table_ref,
                    service_account=service_account,
                    target_table_ref=expected_target_table_ref,
                ):
                    logger.info(
                        "Skipping %s replication: already successfully replicated today; dag_id=%s, target_table_ref=%s",
                        target_table_xcom_key.replace('_table_ref', ''),
                        dag_id,
                        expected_target_table_ref,
                    )
                    raise AirflowSkipException(
                        f"{target_table_xcom_key.replace('_table_ref', '').title()} replication for this table already completed successfully today."
                    )

        process_single_table_replication(
            ti=ti,
            check_source_existence_task_id=check_source_existence_task_id,
            audit_table_ref=audit_table_ref,
            dag_run_id=dag_run_id,
            dag_id=dag_id,
            task_id_for_audit=task_id_for_audit,
            target_table_ref=target_table_ref,
            service_account=service_account,
            write_disposition=write_disposition,
            create_disposition=create_disposition,
        )
        return

    chunk_data = ti.xcom_pull(task_ids=check_source_existence_task_id, key="chunk_source_row_counts") or {}
    start_dt, end_dt = parse_chunk_dates(chunk_start_date, chunk_end_date)

    copy_results, failed_tables, skipped_table_count, processed_table_count = process_chunk_replication(
        chunk_data=chunk_data,
        start_dt=start_dt,
        end_dt=end_dt,
        task_id_for_audit=task_id_for_audit,
        table_name=table_name,
        source_project=source_project,
        source_dataset=source_dataset,
        target_project=target_project,
        target_dataset=target_dataset,
        service_account=service_account,
        write_disposition=write_disposition,
        create_disposition=create_disposition,
        dag_id=dag_id,
        audit_table_ref=audit_table_ref,
    )

    # Only skip the task if ALL tables in the chunk were skipped (processed_table_count == 0)
    # If any table was processed (processed_table_count > 0), continue with the task
    if processed_table_count == 0 and skipped_table_count > 0:
        logger.info(
            "Skipping task: all %d table(s) in chunk [%s, %s] were already successfully replicated",
            skipped_table_count,
            chunk_start_date or start_dt.strftime("%Y-%m-%d"),
            chunk_end_date or end_dt.strftime("%Y-%m-%d"),
        )
        raise AirflowSkipException(
            f"All tables in chunk [{chunk_start_date or start_dt.strftime('%Y-%m-%d')}, {chunk_end_date or end_dt.strftime('%Y-%m-%d')}] were already successfully replicated."
        )

    if copy_results:
        audit_util = GA4DataTransferAuditUtil()
        audit_util.log_changes_to_audit_table(audit_table_ref, dag_run_id, dag_id, copy_results)
    if failed_tables:
        raise AirflowException("One or more copies failed in chunk:\n  " + "\n  ".join(failed_tables))


def get_validation_status(
    source_row_count: Optional[int],
    target_row_count: Optional[int],
) -> str:
    """
    Return validation status from source and target row counts.

    :param source_row_count: Row count from source table (or None).
    :param target_row_count: Row count from target table (or None).
    :return: "SUCCESS" if both are not None and equal; "ERROR" if target_row_count is None;
             "FAILED" if counts are present but do not match.
    """
    if (
        source_row_count is not None
        and target_row_count is not None
        and source_row_count == target_row_count
    ):
        return "SUCCESS"
    if target_row_count is None:
        return "ERROR"
    return "FAILED"
