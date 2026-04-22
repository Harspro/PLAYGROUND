"""
BigQuery Table Copy DAG with Service Account Impersonation and Audit Logging

This DAG copies tables from source to vendor using native BigQuery client API
with service account impersonation for cross-project operations and maintains an audit table for tracking.
Row counts are retrieved using SQL queries with service account impersonation.

DAGs are created via dag_factory (DAGFactory.create_dynamic_dags) from
ga4_table_copy_impersonation_config.yaml
"""

import logging
import pendulum
from copy import deepcopy
from datetime import timedelta
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator  # type: ignore[import-untyped]
from airflow.utils.trigger_rule import TriggerRule

import util.constants as consts
from switch_growth_processing.google_analytics_data_transfer.utils.util import (
    build_iteration_items,
    extract_and_validate_config,
    create_chunk_task_group,
    create_single_table_task_group,
)
from dag_factory import DAGFactory
from dag_factory.abc import BaseDagBuilder

logger = logging.getLogger(__name__)

# Config keys from repo-level util.constants (shared)
BIGQUERY = consts.BIGQUERY
SERVICE_ACCOUNT = consts.SERVICE_ACCOUNT
PROJECT_ID = consts.PROJECT_ID
DATASET_ID = consts.DATASET_ID
WRITE_DISPOSITION = consts.WRITE_DISPOSITION

DAG_DEFAULT_ARGS = {
    'owner': None,
    'capability': 'TBD',
    'severity': 'P2',
    'sub_capability': 'Terminus Data Platform',
    'business_impact': 'NA',
    'customer_impact': 'NA',
    'depends_on_past': False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "execution_timeout": timedelta(minutes=30),
    "retries": 0,
    'retry_delay': timedelta(minutes=3)
}

"""
This is a tool dag that replicates bigquery tables from source to landing and vendor
using native BigQuery client API with service account impersonation for cross-project operations.

Pattern: Replicate tables from source to landing (backup) and vendor (replication) using service account impersonation.
"""


class GoogleAnalyticsVendorDataTransfer(BaseDagBuilder):
    """Builds table-copy-with-impersonation DAGs from config; used by DAGFactory.create_dynamic_dags."""

    def __init__(self, environment_config):
        super().__init__(environment_config)
        self.deploy_env = self.environment_config.deploy_env

    def create_table_copying_tasks(self, dag_config: dict):
        """
        Create tasks for copying tables with audit logging using service account impersonation.

        Pattern: Replicate tables from source to landing (backup) and vendor (replication) using
        service account impersonation. After each replication, row count is validated (source vs target)
        and audit records are logged. Iteration is driven by load_type: daily (one TaskGroup per table)
        or one_time (one TaskGroup per chunk; chunk_size required). dag_id for audit is taken from context at runtime.

        :param dag_config: Full DAG configuration (bigquery, load_type, start_date, end_date, chunk_size for one_time)
        :return: List of task groups
        """
        config = extract_and_validate_config(dag_config, self.deploy_env)

        task_groups = []
        iteration_items = build_iteration_items(dag_config)

        for idx, item in enumerate(iteration_items):
            task_group_id = item['task_group_id']
            table_name = item['table_name']
            # Run downstream groups even when upstream fails (no cascade of "upstream failed")
            entry_trigger_rule = TriggerRule.ALL_DONE if idx > 0 else None

            if 'chunk_start_date' in item and 'chunk_end_date' in item:
                # Chunk mode (one_time loads)
                tgrp = create_chunk_task_group(
                    task_group_id=task_group_id,
                    table_name=table_name,
                    chunk_start_date=item['chunk_start_date'],
                    chunk_end_date=item['chunk_end_date'],
                    config=config,
                    entry_trigger_rule=entry_trigger_rule,
                )
            else:
                # Single-table mode (daily loads); load date resolved in check task (conf or current_date-1)
                tgrp = create_single_table_task_group(
                    task_group_id=task_group_id,
                    table_name=table_name,
                    config=config,
                    entry_trigger_rule=entry_trigger_rule,
                )
            task_groups.append(tgrp)

        return task_groups

    def build(self, dag_id: str, config: dict) -> DAG:
        """
        Build a DAG for table copying with audit logging.
        Called by DAGFactory.create_dynamic_dags; tags are added by the factory.

        :param dag_id: DAG ID
        :param config: DAG configuration dictionary
        :return: Configured DAG
        """
        default_args = deepcopy(DAG_DEFAULT_ARGS)
        default_args.update(config.get(consts.DEFAULT_ARGS, {}))

        schedule_interval = config.get('schedule_interval', None)

        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            schedule=schedule_interval,
            start_date=pendulum.datetime(2026, 1, 1, tz=consts.TORONTO_TIMEZONE_ID),
            max_active_runs=1,
            catchup=False,
            dagrun_timeout=timedelta(minutes=180),
            is_paused_upon_creation=True,
            max_active_tasks=5
        )

        with dag:
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            # End runs only when ALL task groups succeeded (ALL_SUCCESS); if any group failed, end is skipped → DAG fails
            end_point = EmptyOperator(
                task_id=consts.END_TASK_ID,
                trigger_rule=TriggerRule.ALL_SUCCESS,
            )

            table_copying_tasks = self.create_table_copying_tasks(config)

            # Sequential chain: start >> TG0 >> TG1 >> ... >> TGn; end depends on all groups so DAG fails if any failed
            if table_copying_tasks:
                start_point >> table_copying_tasks[0]
                for idx in range(len(table_copying_tasks) - 1):
                    table_copying_tasks[idx] >> table_copying_tasks[idx + 1]
                table_copying_tasks >> end_point

        return dag


globals().update(
    DAGFactory().create_dynamic_dags(
        GoogleAnalyticsVendorDataTransfer,
        'google_analytics_vendor_data_transfer_config.yaml',
        f'{settings.DAGS_FOLDER}/switch_growth_processing/google_analytics_data_transfer/config'
    )
)
