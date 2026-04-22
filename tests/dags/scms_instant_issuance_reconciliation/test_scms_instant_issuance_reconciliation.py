import os
from datetime import datetime
from typing import Final
from unittest.mock import patch, MagicMock, call

import numpy as np
import pandas as pd
from airflow import settings
from google.cloud.bigquery import SchemaField

import util.constants as consts
from scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation import InstantIssuanceReconciliation
from util.miscutils import read_file_env

DEPLOYMENT_CONFIG: Final[dict] = {
    'deployment_environment_name': 'test-deployment-environment',
    'network_tag': 'test-network-tag',
    'processing_zone_connection_id': 'google_cloud_default',
    'location': 'northamerica-northeast1',
    'project_id': 'test-project'
}

JOB_CONFIG: Final[dict] = {
    "description": "Find partially missing card entries for instant issuance and send card replacement requests to Kafka.",
    "tags": [
        "instant-issuance",
        "SCMS"
    ],
    "source_type": "orchestration",
    "schedule_interval": "0 21 * * *",
    "tolerance_window": 90,
    "timestamp_safeguard": 0,
    "read_pause_deploy_config": True,
    "kafka_writer": {
        "dag_id": "instant_issuance_orchestration_bq_to_kafka",
        "dataproc_cluster_name": "scms-instant-issuance-reconciliation-orchest-c",
        "gcs": {
            "bucket": "pcb-{env}-staging-extract",
            "folder_name": "instant_issuance_reconciliation/orchestration",
            "file_name": "instant-issuance-reconciliation-snapshot"
        }
    },
    "big_query": {
        "audit_table_id": "pcb-{env}-curated.domain_card_management.INSTANT_ISSUANCE_RECONCILIATION_AUDIT",
        "mapping": {
            "orchestration_table_id": "pcb-{env}-landing.domain_card_management.INSTANT_ISSUANCE_ORCHESTRATION",
            "view_id": "pcb-{env}-landing.domain_card_management.INSTANT_ISSUANCE_RECONCILIATION_VIEW",
            "audit_table_id": "pcb-{env}-curated.domain_card_management.INSTANT_ISSUANCE_RECONCILIATION_AUDIT",
            "control_table_id": 'pcb-{env}-landing.domain_card_management.INSTANT_ISSUANCE_DAGS_CONTROL',
            "output_uri_landing": "gs://pcb-{env}-staging-extract/instant_issuance_reconciliation/orchestration/instant-issuance-reconciliation-snapshot-*.parquet"
        },
        "audit_table_schema": [
            {
                "name": "orchestrationId",
                "type": "INTEGER",
                "mode": "NULLABLE"
            }
        ],
        "control_table_schema": [
            {
                "name": "dagId",
                "type": "STRING",
                "mode": "NULLABLE"
            }
        ]
    }
}

CONFIG_FILENAME: Final[str] = "scms_instant_issuance_reconciliation_config.yaml"
CONFIG_DIR: Final[str] = f"{settings.DAGS_FOLDER}/scms_instant_issuance_reconciliation/config"
SQL_DIR: Final[str] = f"{settings.DAGS_FOLDER}/scms_instant_issuance_reconciliation/sql"
DAG_ID: Final[str] = "instant_issuance_reconciliation_orchestration"


class TestInstantIssuanceReconciliation:

    @staticmethod
    def get_sql_test_dir(sql_filename: str) -> str:
        return f'{os.path.dirname(os.path.abspath(__file__))}/sql/{sql_filename}.sql'

    @staticmethod
    def setup_common_mocks(mock_read_variable_or_file, mock_read_yamlfile_env):
        mock_read_variable_or_file.return_value = DEPLOYMENT_CONFIG
        mock_read_yamlfile_env.return_value = JOB_CONFIG

    @staticmethod
    def setup_mock_query_job(mock_run_bq_query, dataframe=pd.DataFrame({'total': [0]})):
        mock_result = MagicMock()
        mock_run_bq_query.return_value = mock_result
        mock_result.to_dataframe.return_value = dataframe

    @staticmethod
    def setup_mock_context(context, return_value):
        mock_task_instance = MagicMock()
        context['ti'] = mock_task_instance
        mock_task_instance.xcom_pull.return_value = return_value

    @staticmethod
    def verify_common_mocks(mock_read_variable_or_file, mock_read_yamlfile_env):
        mock_read_variable_or_file.assert_called_with(consts.GCP_CONFIG)
        mock_read_yamlfile_env.assert_called_with(f'{CONFIG_DIR}/{CONFIG_FILENAME}',
                                                  DEPLOYMENT_CONFIG['deployment_environment_name'])

    @staticmethod
    def verify_missing_card_mocks(mock_run_bq_query,
                                  mock_read_file_env,
                                  queries, total_query_count):
        assert mock_read_file_env.call_args_list == queries
        assert mock_run_bq_query.call_count == total_query_count

    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.read_variable_or_file")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.read_yamlfile_env")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.read_file_env")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.run_bq_query")
    def test_query_last_processed_run(self, mock_run_bq_query, mock_read_file_env, mock_read_yamlfile_env,
                                      mock_read_variable_or_file):
        self.setup_common_mocks(mock_read_variable_or_file, mock_read_yamlfile_env)
        find_last_processed_run_query = self.get_sql_test_dir("test_find_last_processed_run")
        mock_read_file_env.return_value = find_last_processed_run_query

        reconciliation = InstantIssuanceReconciliation(CONFIG_FILENAME, f'{CONFIG_DIR}')
        reconciliation.query_last_processed_run(DAG_ID, JOB_CONFIG)

        self.verify_common_mocks(mock_read_variable_or_file, mock_read_yamlfile_env)
        mock_read_file_env.assert_called_with(
            f'{SQL_DIR}/find_last_processed_run.sql')
        mock_run_bq_query.assert_called_with(
            find_last_processed_run_query.replace(f"{{{'audit_table_id'}}}",
                                                  "pcb-{env}-curated.domain_card_management.INSTANT_ISSUANCE_RECONCILIATION_AUDIT"))

    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.read_variable_or_file")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.read_yamlfile_env")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.read_file_env")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.run_bq_query")
    def test_log_replacement_cards_to_audit_table(self, mock_run_bq_query, mock_read_file_env,
                                                  mock_read_yamlfile_env,
                                                  mock_read_variable_or_file):
        self.setup_common_mocks(mock_read_variable_or_file, mock_read_yamlfile_env)
        log_card_to_audit_query = read_file_env(self.get_sql_test_dir("test_log_cards_to_audit"))
        mock_read_file_env.return_value = log_card_to_audit_query

        reconciliation = InstantIssuanceReconciliation(CONFIG_FILENAME, f'{CONFIG_DIR}')
        reconciliation.log_replacement_cards_to_audit_table(JOB_CONFIG)

        self.verify_common_mocks(mock_read_variable_or_file, mock_read_yamlfile_env)
        mock_read_file_env.assert_called_with(
            f'{SQL_DIR}/{JOB_CONFIG["source_type"]}/log_cards_to_audit.sql')
        mock_run_bq_query.assert_called_with(
            log_card_to_audit_query
            .replace(f"{{{'audit_table_id'}}}",
                     "pcb-{env}-curated.domain_card_management.INSTANT_ISSUANCE_RECONCILIATION_AUDIT")
            .replace(f"{{{'view_id'}}}",
                     "pcb-{env}-landing.domain_card_management.INSTANT_ISSUANCE_RECONCILIATION_VIEW")
            .replace(f"{{{'orchestration_table_id'}}}",
                     "pcb-{env}-landing.domain_card_management.INSTANT_ISSUANCE_ORCHESTRATION"))

    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.read_variable_or_file")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.read_yamlfile_env")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.read_file_env")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.run_bq_query")
    def test_query_missing_cards_from_source_type_table(self,
                                                        mock_run_bq_query,
                                                        mock_read_file_env,
                                                        mock_read_yamlfile_env,
                                                        mock_read_variable_or_file):
        self.setup_common_mocks(mock_read_variable_or_file, mock_read_yamlfile_env)
        mock_read_file_env.side_effect = [read_file_env(self.get_sql_test_dir("test_find_missing_cards")),
                                          read_file_env(self.get_sql_test_dir("test_total_entries_count"))]

        context = {'params': {'tolerance_window': 10, 'lookback_window': 0, 'request_threshold': 1000}}
        self.setup_mock_context(context, 0)
        self.setup_mock_query_job(mock_run_bq_query)

        reconciliation = InstantIssuanceReconciliation(CONFIG_FILENAME, f'{CONFIG_DIR}')
        reconciliation.query_missing_cards_from_source_type_table(JOB_CONFIG, **context)

        query_calls = [
            call(f'{SQL_DIR}/{JOB_CONFIG["source_type"]}/find_missing_cards.sql'),
            call(f'{SQL_DIR}/total_entries_count.sql')]

        self.verify_common_mocks(mock_read_variable_or_file, mock_read_yamlfile_env)
        self.verify_missing_card_mocks(mock_run_bq_query, mock_read_file_env, query_calls, 2)

    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.read_variable_or_file")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.read_yamlfile_env")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.read_file_env")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.run_bq_query")
    def test_query_missing_cards_from_orchestration_table_with_window(self,
                                                                      mock_run_bq_query,
                                                                      mock_read_file_env,
                                                                      mock_read_yamlfile_env,
                                                                      mock_read_variable_or_file):
        self.setup_common_mocks(mock_read_variable_or_file, mock_read_yamlfile_env)
        mock_read_file_env.side_effect = [read_file_env(self.get_sql_test_dir("test_find_missing_cards_with_window")),
                                          read_file_env(self.get_sql_test_dir("test_total_entries_count"))]

        context = {'params': {'tolerance_window': 10, 'lookback_window': 0, 'request_threshold': 1000}}
        self.setup_mock_context(context, "")
        self.setup_mock_query_job(mock_run_bq_query)

        reconciliation = InstantIssuanceReconciliation(CONFIG_FILENAME, f'{CONFIG_DIR}')
        reconciliation.query_missing_cards_from_source_type_table(JOB_CONFIG, **context)

        query_calls = [
            call(f'{SQL_DIR}/{JOB_CONFIG["source_type"]}/find_missing_cards_with_window.sql'),
            call(f'{SQL_DIR}/total_entries_count.sql')]

        self.verify_common_mocks(mock_read_variable_or_file, mock_read_yamlfile_env)
        self.verify_missing_card_mocks(mock_run_bq_query, mock_read_file_env, query_calls, 2)

    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.read_variable_or_file")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.read_yamlfile_env")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.read_file_env")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.run_bq_query")
    @patch("scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation.delete_folder")
    @patch('google.cloud.bigquery.Client', autospec=True)
    @patch('google.cloud.bigquery.Table', autospec=True)
    def test_validate_dag_run(self, mock_bq_table, mock_bq_client, mock_delete_folder, mock_run_bq_query,
                              mock_read_file_env, mock_read_yamlfile_env,
                              mock_read_variable_or_file):
        self.setup_common_mocks(mock_read_variable_or_file, mock_read_yamlfile_env)
        find_last_processed_run_query = self.get_sql_test_dir("test_find_last_processed_run")
        mock_read_file_env.return_value = find_last_processed_run_query

        df_result = pd.DataFrame(
            {'windowEndTime': [np.datetime64(datetime.now())]})
        mock_job = MagicMock()
        mock_result = MagicMock()
        mock_run_bq_query.return_value = mock_job
        mock_job.result.return_value = mock_result
        mock_result.total_rows.return_value = 1
        mock_result.to_dataframe.return_value = df_result

        context = {'params': {'tolerance_window': 10, 'lookback_window': 0, 'request_threshold': 1000}}
        self.setup_mock_context(context, 1)

        reconciliation = InstantIssuanceReconciliation(CONFIG_FILENAME, f'{CONFIG_DIR}')
        reconciliation.validate_dag_run(DAG_ID, JOB_CONFIG, **context)

        mock_read_file_env.assert_called_with(
            f'{SQL_DIR}/find_last_processed_run.sql')

        mock_bq_table.assert_called_with(
            'pcb-{env}-landing.domain_card_management.INSTANT_ISSUANCE_DAGS_CONTROL',
            schema=[SchemaField('dagId', 'STRING', 'NULLABLE', None, None, (), None)])
        mock_bq_client().create_table.assert_called_with(mock_bq_table.return_value, exists_ok=True)
        mock_delete_folder.assert_called_with(JOB_CONFIG["kafka_writer"]["gcs"]["bucket"],
                                              JOB_CONFIG["kafka_writer"]["gcs"]["folder_name"])

        mock_run_bq_query.assert_called_with(
            find_last_processed_run_query.replace(f"{{{'control_table_id'}}}",
                                                  "pcb-{env}-landing.domain_card_management.INSTANT_ISSUANCE_DAGS_CONTROL"))
