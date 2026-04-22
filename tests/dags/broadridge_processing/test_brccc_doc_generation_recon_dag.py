import os
from datetime import datetime
from unittest.mock import patch, MagicMock, call
import io

import pytest
from airflow import settings
from airflow.exceptions import AirflowSkipException

import util.constants as consts

from broadridge_processing.brccc_doc_generation_recon_dag import BroadridgeDocGenerationReconDagBuilder
from util.miscutils import read_file_env

current_script_directory = os.path.dirname(os.path.abspath(__file__))

CONFIG_PATH = f"{current_script_directory}/config/test_config.yaml"


@patch('broadridge_processing.brccc_doc_generation_recon_dag.storage.Client')
def test_extract_value_from_csv(mock_storage_client):
    mock_blob = MagicMock()
    mock_file = io.StringIO(read_file_env(f"{current_script_directory}/config/test_csv_file.csv"))

    mock_blob.open.return_value.__enter__.return_value = mock_file

    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob

    mock_storage_client.return_value.bucket.return_value = mock_bucket

    mock_ti = MagicMock()
    kwargs = {'ti': mock_ti}

    instance = BroadridgeDocGenerationReconDagBuilder(CONFIG_PATH)

    instance._extract_value_from_csv(
        bucket='my-bucket',
        folder_name='folder',
        file_name='file.csv',
        column_index=6,
        **kwargs
    )

    mock_storage_client.assert_called_once()
    mock_bucket.blob.assert_called_once_with('folder/file.csv')
    mock_blob.open.assert_called_once_with('r')
    mock_ti.xcom_push.assert_called_once_with(key='broadridge_outbound_filename', value='desired_value')


@patch('broadridge_processing.brccc_doc_generation_recon_dag.run_bq_query')
@patch('broadridge_processing.brccc_doc_generation_recon_dag.read_file_env')
def test_run_sql_to_insert(mock_read_file_env, mock_run_bq_query):
    sql_query = read_file_env(
        f"{current_script_directory}/sql/test_mail_print_status_insert_query.sql",
        "dev"
    )
    mock_read_file_env.return_value = sql_query

    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = 'test_outbound_filename'

    mock_dag_run = MagicMock()
    mock_dag_run.run_id = 'test_dag_run_id'

    kwargs = {
        'ti': mock_ti,
        'dag_run': mock_dag_run
    }

    instance = BroadridgeDocGenerationReconDagBuilder(CONFIG_PATH)
    instance._run_sql_to_insert(dag_id='test_dag_id', **kwargs)

    mock_read_file_env.assert_called_once_with(f'{settings.DAGS_FOLDER}/broadridge_processing/sql/mail_print_status_insert_query.sql', instance.deploy_env)
    mock_ti.xcom_pull.assert_called_once_with(
        key="broadridge_outbound_filename",
        task_ids="task_extract_outbound_filename"
    )
    expected_sql = (sql_query
                    .replace('<<OUTBOUND_FILE_NAME>>', 'test_outbound_filename')
                    .replace('<<DAG_ID>>', 'test_dag_id')
                    .replace('<<DAG_RUN_ID>>', 'test_dag_run_id'))
    mock_run_bq_query.assert_called_once_with(expected_sql)


@patch('broadridge_processing.brccc_doc_generation_recon_dag.run_bq_query')
@patch('broadridge_processing.brccc_doc_generation_recon_dag.read_file_env')
def test_run_sql_to_create_parquet_success(mock_read_file_env, mock_run_bq_query):
    count_sql_query = read_file_env(
        f"{current_script_directory}/sql/test_mail_print_status_count_query.sql", "dev"
    )
    parquet_sql_query = read_file_env(
        f"{current_script_directory}/sql/test_mail_print_status_parquet_query.sql",
        "dev",
    )

    def read_file_env_side_effect(file_path, deploy_env):
        if "parquet" in file_path:
            return parquet_sql_query
        else:
            return count_sql_query

    mock_read_file_env.side_effect = read_file_env_side_effect

    mock_row = MagicMock()
    mock_row.record_count = 5
    mock_job = MagicMock()
    mock_job.result.return_value = [mock_row]
    mock_run_bq_query.return_value = mock_job

    instance = BroadridgeDocGenerationReconDagBuilder(CONFIG_PATH)
    instance.est_zone = "America/Toronto"

    outbound_file_name = "test_outbound_filename"
    bucket = "my-bucket"
    folder_name = "my-folder"
    file_name = "my-file.parquet"
    dag_id = "test_dag_id"

    instance._run_sql_to_create_parquet(
        outbound_file_name,
        bucket,
        folder_name,
        file_name,
        dag_id,
        dag_run=MagicMock(run_id="test_run_id"),
    )

    mock_read_file_env.assert_has_calls(
        [
            call(
                f"{settings.DAGS_FOLDER}/broadridge_processing/sql/mail_print_status_count_query.sql",
                instance.deploy_env,
            ),
            call(
                f"{settings.DAGS_FOLDER}/broadridge_processing/sql/mail_print_status_parquet_query.sql",
                instance.deploy_env,
            ),
        ],
        any_order=True,
    )

    expected_calls = [
        call(count_sql_query.format(dag_id=dag_id, dag_run_id="test_run_id")),
        call(
            parquet_sql_query.format(
                parquet_file_path="my-bucket/my-folder/my-file.parquet",
                dag_id=dag_id,
                dag_run_id="test_run_id",
                outbound_file_name="test_outbound_filename",
            )
        ),
    ]
    mock_run_bq_query.assert_has_calls(expected_calls, any_order=True)


@patch('broadridge_processing.brccc_doc_generation_recon_dag.run_bq_query')
@patch('broadridge_processing.brccc_doc_generation_recon_dag.read_file_env')
def test_run_sql_to_create_parquet_skip(mock_read_file_env, mock_run_bq_query):
    count_sql_query = read_file_env(
        f"{current_script_directory}/sql/test_mail_print_status_count_query.sql", "dev"
    )

    mock_read_file_env.return_value = count_sql_query

    mock_row = MagicMock()
    mock_row.record_count = 0
    mock_job = MagicMock()
    mock_job.result.return_value = [mock_row]
    mock_run_bq_query.return_value = mock_job

    outbound_file_name = "test_outbound_filename"
    bucket = "my-bucket"
    folder_name = "my-folder"
    file_name = "my-file.parquet"
    dag_id = "test_dag_id"

    instance = BroadridgeDocGenerationReconDagBuilder(CONFIG_PATH)
    instance.est_zone = "America/Toronto"

    with pytest.raises(AirflowSkipException):
        instance._run_sql_to_create_parquet(
            outbound_file_name,
            bucket,
            folder_name,
            file_name,
            dag_id,
            dag_run=MagicMock(run_id="test_run_id"),
        )

    mock_read_file_env.assert_called_once_with(
        f"{settings.DAGS_FOLDER}/broadridge_processing/sql/mail_print_status_count_query.sql",
        instance.deploy_env,
    )

    expected_count_sql = count_sql_query.format(
        dag_id=dag_id, dag_run_id="test_run_id"
    )
    mock_run_bq_query.assert_called_once_with(expected_count_sql)


@patch("broadridge_processing.brccc_doc_generation_recon_dag.TriggerDagRunOperator")
@patch("broadridge_processing.brccc_doc_generation_recon_dag.PythonOperator")
@patch("broadridge_processing.brccc_doc_generation_recon_dag.EmptyOperator")
def test_create_dag(mock_empty,
                    mock_python,
                    mock_trigger_dag_run):
    builder = BroadridgeDocGenerationReconDagBuilder(CONFIG_PATH)
    builder.default_args = {}
    xcom_outbound_file_name = "{{ task_instance.xcom_pull(task_ids='task_extract_outbound_filename', key='broadridge_outbound_filename') }}"

    fixed_now = datetime(2024, 4, 2, 9, 0, 0)
    with patch("broadridge_processing.brccc_doc_generation_recon_dag.datetime") as mock_datetime:
        mock_datetime.now.return_value = fixed_now
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        builder.create_dag("dag_id", builder.dag_config)

    mock_empty.assert_has_calls([
        call(task_id="start"),
        call(task_id="end")
    ])

    mock_python.assert_has_calls([
        call(task_id="task_extract_outbound_filename",
             python_callable=builder._extract_value_from_csv,
             op_kwargs={
                 'bucket': builder.dag_config[consts.DAG].get(consts.TRIGGER_CONFIG).get(consts.BUCKET),
                 'folder_name': builder.dag_config[consts.DAG].get(consts.TRIGGER_CONFIG).get(consts.FOLDER_NAME),
                 'file_name': builder.dag_config[consts.DAG].get(consts.TRIGGER_CONFIG).get(consts.FILE_NAME),
                 'column_index': builder.dag_config[consts.DAG].get('file_extract_column_index')
             }),
        call(task_id="task_run_sql_to_insert_mailed_status",
             python_callable=builder._run_sql_to_insert,
             op_kwargs={
                 'dag_id': "dag_id",
             }),
        call(task_id="task_run_sql_to_create_parquet",
             python_callable=builder._run_sql_to_create_parquet,
             op_kwargs={
                 'outbound_file_name': xcom_outbound_file_name,
                 'bucket': builder.dag_config[consts.DAG].get(consts.BIGQUERY).get(consts.BUCKET),
                 'folder_name': f"{builder.dag_config[consts.DAG].get(consts.BIGQUERY).get(consts.FOLDER_PREFIX)}/{fixed_now.strftime('%Y%m%d')}/{xcom_outbound_file_name}",
                 'file_name': builder.dag_config[consts.DAG].get(consts.BIGQUERY).get(consts.FILE_NAME),
                 'dag_id': 'dag_id'
             })
    ])

    mock_trigger_dag_run.assert_called_with(
        task_id=builder.dag_config[consts.DAG].get("kafka_config").get(consts.KAFKA_WRITER_TASK_ID),
        trigger_dag_id=builder.dag_config[consts.DAG].get("kafka_config").get(consts.KAFKA_TRIGGER_DAG_ID),
        logical_date=fixed_now,
        conf={
            'bucket': builder.dag_config[consts.DAG].get(consts.BIGQUERY).get(consts.BUCKET),
            'folder_name': f"{builder.dag_config[consts.DAG].get(consts.BIGQUERY).get(consts.FOLDER_PREFIX)}/{fixed_now.strftime('%Y%m%d')}/{xcom_outbound_file_name}",
            'file_name': builder.dag_config[consts.DAG].get(consts.BIGQUERY).get(consts.FILE_NAME),
            'cluster_name': builder.dag_config[consts.DAG].get("kafka_config").get(consts.KAFKA_CLUSTER_NAME)
        },
        wait_for_completion=True,
        poke_interval=30
    )
