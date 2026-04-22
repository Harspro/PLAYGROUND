import json
import os
from datetime import timedelta, datetime
from unittest.mock import patch, MagicMock, call, ANY, mock_open

import pytest
import requests
from airflow import settings
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import V1LocalObjectReference, V1SecretVolumeSource, V1Toleration, V1Volume, V1VolumeMount

import util.constants as consts
import doc_generation_processing.util.constants as doc_gen_consts

from doc_generation_processing.document_generation_dag import DocumentGeneratorDagBuilder
from util.miscutils import read_file_env

current_script_directory = os.path.dirname(os.path.abspath(__file__))

UNIQUE_FOLDER_PATH = 'manual__2025_06_30T14_02_47_00_00'
CONFIG_PATH = f"{current_script_directory}/config/test_config.yaml"


def test_create_unique_folder_path():
    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)

    mock_dag_run = MagicMock()
    mock_dag_run.run_id = 'manual__2025-06-30T14:02:47+00:00'

    unique_folder_path = instance.create_unique_folder_path(dag_run=mock_dag_run)

    assert unique_folder_path == UNIQUE_FOLDER_PATH


@patch("doc_generation_processing.document_generation_dag.run_bq_query")
@patch('doc_generation_processing.document_generation_dag.read_file_env')
def test_fetch_and_export_data_with_defaults(mock_read_file_env,
                                             mock_run_bq_query):
    sql_query = read_file_env(
        f"{current_script_directory}/sql/test_mail_print_export_json_query.sql",
        "dev"
    )
    mock_read_file_env.return_value = sql_query

    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    instance.est_zone = "America/Toronto"

    fixed_now = datetime(2024, 4, 2, 9, 0, 0)
    with patch("doc_generation_processing.document_generation_dag.datetime") as mock_datetime:
        mock_datetime.now.return_value = fixed_now
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

        instance.fetch_and_export_data(unique_folder_path=UNIQUE_FOLDER_PATH)

    expected_yesterday = fixed_now - timedelta(days=1)
    expected_start = expected_yesterday.replace(hour=8, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
    expected_end = fixed_now.replace(hour=8, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')

    mock_read_file_env.assert_called_once_with(
        f'{settings.DAGS_FOLDER}/doc_generation_processing/sql/mail_print_export_json_query.sql', instance.deploy_env)

    expected_sql = sql_query.format(
        unique_folder_path=UNIQUE_FOLDER_PATH,
        start_time=expected_start,
        end_time=expected_end,
        est_zone=instance.est_zone
    )
    mock_run_bq_query.assert_called_once_with(expected_sql)


@patch("doc_generation_processing.document_generation_dag.run_bq_query")
@patch('doc_generation_processing.document_generation_dag.read_file_env')
def test_fetch_and_export_data_with_provided_value(mock_read_file_env,
                                                   mock_run_bq_query):
    sql_query = read_file_env(
        f"{current_script_directory}/sql/test_mail_print_export_json_query.sql",
        "dev"
    )
    mock_read_file_env.return_value = sql_query

    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    instance.est_zone = "America/Toronto"

    start = "2025-03-01 08:00:00"
    end = "2025-03-01 08:00:00"
    instance.fetch_and_export_data(start_time=start, end_time=end, unique_folder_path=UNIQUE_FOLDER_PATH)

    mock_read_file_env.assert_called_once_with(doc_gen_consts.MAIL_PRINT_EXPORT_JSON_SQL_PATH, instance.deploy_env)

    expected_sql = sql_query.format(
        unique_folder_path=UNIQUE_FOLDER_PATH,
        start_time=start,
        end_time=end,
        est_zone=instance.est_zone
    )
    mock_run_bq_query.assert_called_once_with(expected_sql)


@patch("doc_generation_processing.document_generation_dag.google.oauth2.id_token.fetch_id_token")
@patch("doc_generation_processing.document_generation_dag.google.auth.transport.requests.Request")
def test_get_identity_token(mock_request_class,
                            mock_fetch_id_token):
    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    audience = "https://example.com"
    expected_token = "fake-token"

    mock_request_instance = MagicMock()
    mock_request_class.return_value = mock_request_instance
    mock_fetch_id_token.return_value = expected_token

    token = instance._get_identity_token(audience)

    assert token == expected_token
    mock_request_class.assert_called_once()
    mock_fetch_id_token.assert_called_once_with(mock_request_instance, audience)


@patch("doc_generation_processing.document_generation_dag.requests.post")
def test_process_record_success(mock_post, caplog):
    mock_path = 'mock/path'
    mock_processing_time_ms = 12345
    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    mock_response = MagicMock()
    mock_response.status_code = 201
    mock_response.json.return_value = {
        doc_gen_consts.PATH_JSON_KEY: mock_path,
        doc_gen_consts.PROCESSING_TIME_MS_JSON_KEY: mock_processing_time_ms
    }
    mock_post.return_value = mock_response

    code, uuid, status, path = instance._process_record(
        bucket_name="bucket",
        unique_folder_path=UNIQUE_FOLDER_PATH,
        audience="http://mock-url",
        identity_token="token",
        line='{"name": "A", "code": "123", "uuid": "abc-123"}'
    )

    mock_post.assert_called_once()
    assert status == doc_gen_consts.PDF_GENERATED
    assert path == mock_path
    assert code == '123'
    assert uuid == 'abc-123'
    assert f'PDF generation succeeded: processingTimeMs={mock_processing_time_ms}ms' in caplog.text


@patch("doc_generation_processing.document_generation_dag.requests.post")
def test_process_record_success_no_processing_time(mock_post, caplog):
    mock_path = 'mock/path'
    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    mock_response = MagicMock()
    mock_response.status_code = 201
    mock_response.json.return_value = {
        doc_gen_consts.PATH_JSON_KEY: mock_path
    }
    mock_post.return_value = mock_response

    code, uuid, status, path = instance._process_record(
        bucket_name="bucket",
        unique_folder_path=UNIQUE_FOLDER_PATH,
        audience="http://mock-url",
        identity_token="token",
        line='{"name": "A", "code": "123", "uuid": "abc-123"}'
    )

    mock_post.assert_called_once()
    assert status == doc_gen_consts.PDF_GENERATED
    assert path == mock_path
    assert code == '123'
    assert uuid == 'abc-123'
    assert '' in caplog.text


@patch("doc_generation_processing.document_generation_dag.requests.post")
def test_process_record_failure_json_response(mock_post, caplog):
    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    mock_error_message = "Failed to produce PDF"
    mock_processing_time_ms = 12345
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.headers.get.return_value = 'application/json'
    mock_response.json.return_value = {
        doc_gen_consts.ERROR_MESSAGE_JSON_KEY: mock_error_message,
        doc_gen_consts.PROCESSING_TIME_MS_JSON_KEY: mock_processing_time_ms
    }
    mock_post.return_value = mock_response

    code, uuid, status, path = instance._process_record(
        bucket_name="bucket",
        unique_folder_path=UNIQUE_FOLDER_PATH,
        audience="http://mock-url",
        identity_token="token",
        line='{"name": "A", "code": "123", "uuid": "abc-123"}'
    )

    mock_post.assert_called_once()
    assert status == doc_gen_consts.PDF_GENERATION_FAILED
    assert code == '123'
    assert uuid == 'abc-123'
    assert path is None
    assert f'PDF generation failed: {mock_response.status_code} - errorMessage={mock_error_message}, processingTimeMs={mock_processing_time_ms}ms' in caplog.text


@patch("doc_generation_processing.document_generation_dag.requests.post")
def test_process_record_failure_json_response_no_error_message(mock_post, caplog):
    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    mock_processing_time_ms = 12345
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.headers.get.return_value = 'application/json'
    mock_response.json.return_value = {
        doc_gen_consts.PROCESSING_TIME_MS_JSON_KEY: mock_processing_time_ms
    }
    mock_post.return_value = mock_response

    code, uuid, status, path = instance._process_record(
        bucket_name="bucket",
        unique_folder_path=UNIQUE_FOLDER_PATH,
        audience="http://mock-url",
        identity_token="token",
        line='{"name": "A", "code": "123", "uuid": "abc-123"}'
    )

    mock_post.assert_called_once()
    assert status == doc_gen_consts.PDF_GENERATION_FAILED
    assert code == '123'
    assert uuid == 'abc-123'
    assert path is None
    assert f'PDF generation failed: {mock_response.status_code} - errorMessage=Unknown error, processingTimeMs={mock_processing_time_ms}ms' in caplog.text


@patch("doc_generation_processing.document_generation_dag.requests.post")
def test_process_record_failure_json_response_no_processing_time(mock_post, caplog):
    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    mock_error_message = "Failed to produce PDF"
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.headers.get.return_value = 'application/json'
    mock_response.json.return_value = {
        doc_gen_consts.ERROR_MESSAGE_JSON_KEY: mock_error_message
    }
    mock_post.return_value = mock_response

    code, uuid, status, path = instance._process_record(
        bucket_name="bucket",
        unique_folder_path=UNIQUE_FOLDER_PATH,
        audience="http://mock-url",
        identity_token="token",
        line='{"name": "A", "code": "123", "uuid": "abc-123"}'
    )

    mock_post.assert_called_once()
    assert status == doc_gen_consts.PDF_GENERATION_FAILED
    assert code == '123'
    assert uuid == 'abc-123'
    assert path is None
    assert f'PDF generation failed: {mock_response.status_code} - errorMessage={mock_error_message}' in caplog.text


@patch("doc_generation_processing.document_generation_dag.requests.post")
def test_process_record_failure_text_response(mock_post, caplog):
    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.headers.get.return_value = 'text/plain'
    mock_response.text = "Failed to produce PDF"
    mock_post.return_value = mock_response

    code, uuid, status, path = instance._process_record(
        bucket_name="bucket",
        unique_folder_path=UNIQUE_FOLDER_PATH,
        audience="http://mock-url",
        identity_token="token",
        line='{"name": "A", "code": "123", "uuid": "abc-123"}'
    )

    mock_post.assert_called_once()
    assert status == doc_gen_consts.PDF_GENERATION_FAILED
    assert code == '123'
    assert uuid == 'abc-123'
    assert path is None
    assert f'PDF generation failed: {mock_response.status_code} - {mock_response.text}' in caplog.text


@patch("doc_generation_processing.document_generation_dag.requests.post")
def test_process_record_invalid_json_exception(mock_post):
    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)

    with pytest.raises(json.decoder.JSONDecodeError):
        instance._process_record(
            bucket_name="bucket",
            unique_folder_path=UNIQUE_FOLDER_PATH,
            audience="http://mock-url",
            identity_token="token",
            line='{"name": "A"'
        )
    mock_post.assert_not_called()


@patch("doc_generation_processing.document_generation_dag.requests.post")
def test_process_record_exception(mock_post, caplog):
    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    mock_post.side_effect = requests.exceptions.RequestException("Exception")

    code, uuid, status, path = instance._process_record(
        bucket_name="bucket",
        unique_folder_path=UNIQUE_FOLDER_PATH,
        audience="http://mock-url",
        identity_token="token",
        line='{"name": "A", "code": "123", "uuid": "abc-123"}'
    )
    mock_post.assert_called_once()
    assert status == doc_gen_consts.PDF_GENERATION_FAILED
    assert code == '123'
    assert uuid == 'abc-123'
    assert path is None
    assert "Error while generating PDF" in caplog.text


@patch.object(DocumentGeneratorDagBuilder, "_get_identity_token", return_value="token")
@patch.object(DocumentGeneratorDagBuilder, "_process_record")
@patch("doc_generation_processing.document_generation_dag.run_bq_query")
@patch("doc_generation_processing.document_generation_dag.read_file_env")
@patch("doc_generation_processing.document_generation_dag.storage.Client")
def test_process_json_to_pdf(mock_storage_client,
                             mock_read_file_env,
                             mock_run_bq_query,
                             mock_process_record,
                             mock_get_identity_token):
    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)

    mock_blob = MagicMock()
    mock_blob.download_to_file.side_effect = lambda f: f.write(
        b'{"id":1, "code": "001", "uuid": "abc-1"}\n{"id":2, "code": "002", "uuid": "abc-2"}\n{"id":3, "code": "003", "uuid": "abc-3"}\n'
    )
    mock_storage_client.return_value.bucket.return_value.list_blobs.return_value = [mock_blob]

    process_results = [
        ("001", "abc-1", "SUCCESS", "path/to/file1"),
        ("002", "abc-2", "FAILURE", None),
        ("003", "abc-3", "SUCCESS", "path/to/file2")
    ]
    mock_process_record.side_effect = process_results
    sql_query = read_file_env(
        f"{current_script_directory}/sql/test_mail_print_status_insert_pdf_status_query.sql",
        "dev"
    )
    mock_read_file_env.return_value = sql_query

    with patch("builtins.open", new_callable=mock_open,
               read_data='{"id":1, "code": "001", "uuid": "abc-1"}\n{"id":2, "code": "002", "uuid": "abc-2"}\n{"id":3, "code": "003", "uuid": "abc-3"}\n'):
        instance.process_json_to_pdf(
            dag_id="test_dag_id",
            bucket_name="input-bucket",
            unique_folder_path=UNIQUE_FOLDER_PATH,
            prefix="prefix",
            audience="audience-url",
            max_concurrent_records=2,
            dag_run=MagicMock(run_id="test_run_id")
        )

    mock_get_identity_token.assert_called_once_with("audience-url")
    assert mock_process_record.call_count == 3
    mock_process_record.assert_has_calls([
        call("input-bucket", UNIQUE_FOLDER_PATH, "audience-url", "token", '{"id":1, "code": "001", "uuid": "abc-1"}\n'),
        call("input-bucket", UNIQUE_FOLDER_PATH, "audience-url", "token", '{"id":2, "code": "002", "uuid": "abc-2"}\n'),
        call("input-bucket", UNIQUE_FOLDER_PATH, "audience-url", "token", '{"id":3, "code": "003", "uuid": "abc-3"}\n')
    ])
    mock_read_file_env.assert_has_calls([
        call(doc_gen_consts.MAIL_PRINT_STATUS_INSERT_PDF_STATUS_SQL_PATH, instance.deploy_env),
        call(doc_gen_consts.MAIL_PRINT_STATUS_INSERT_PDF_STATUS_SQL_PATH, instance.deploy_env)
    ])

    expected_values_1 = (
        "('001', 'abc-1', 'SUCCESS', 'path/to/file1', 'test_dag_id', 'test_run_id'), "
        "('002', 'abc-2', 'FAILURE', NULL, 'test_dag_id', 'test_run_id')"
    )
    expected_sql_1 = sql_query.format(
        insert_values=expected_values_1
    )

    expected_values_2 = (
        "('003', 'abc-3', 'SUCCESS', 'path/to/file2', 'test_dag_id', 'test_run_id')"
    )
    expected_sql_2 = sql_query.format(
        insert_values=expected_values_2
    )
    mock_run_bq_query.assert_has_calls([
        call(expected_sql_1),
        call(expected_sql_2)
    ])


@patch("doc_generation_processing.document_generation_dag.storage.Client")
def test_get_valid_env_variables(mock_storage_client):
    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)

    mock_bucket = MagicMock()
    mock_storage_client.return_value.bucket.return_value = mock_bucket
    mock_blob = MagicMock()
    mock_bucket.list_blobs.side_effect = lambda prefix: [mock_blob] if 'LETTER/SIMPLEX/EN' in prefix else []

    input_bucket = instance.dag_config['dag']['input_bucket']
    output_bucket = instance.dag_config['dag']['workload']['uat']['output_bucket']
    output_file_prefix = instance.dag_config['dag']['workload']['uat']['afp_file_prefix']
    output_file_extension = instance.dag_config['dag']['workload']['uat']['afp_file_extension']

    fixed_now = datetime(2024, 4, 2, 9, 0, 0)
    with patch("doc_generation_processing.document_generation_dag.datetime") as mock_datetime:
        mock_datetime.now.return_value = fixed_now
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        valid_env_variables = instance.get_valid_env_variables(
            input_bucket=input_bucket,
            output_bucket=output_bucket,
            unique_folder_path=UNIQUE_FOLDER_PATH,
            output_file_prefix=output_file_prefix,
            output_file_extension=output_file_extension,
            afp_config=instance.dag_config['dag']['workload']['afp']
        )

    expected_env_variables = [{
        'INPUT_BUCKET_NAME': input_bucket,
        'INPUT_PDFS_PATH': f"{UNIQUE_FOLDER_PATH}/LETTER/SIMPLEX/EN",
        'OUTPUT_BUCKET_NAME': output_bucket,
        'OUTPUT_FILE_NAME': f"{output_file_prefix}simplex_eng_letters_afp_20240402090001",
        'OUTPUT_FILE_EXTENSION': output_file_extension,
        'LICENSE_PATH': '/vault/secrets/prolcnse.lic'
    }]

    assert valid_env_variables == expected_env_variables


@patch("doc_generation_processing.document_generation_dag.run_bq_query")
@patch('doc_generation_processing.document_generation_dag.read_file_env')
def test_insert_afp_status_and_check_failures(mock_read_file_env,
                                              mock_run_bq_query):
    dag_id = 'dag_id'
    dag_run_id = 'test_run_id'
    env_var_list = [
        {
            "path": "LETTER/SIMPLEX/EN",
            "outbound_filename": "psf_t_pcb_dst_simplex_eng_letters_afp_20250724152102.uatv",
            "job_status": doc_gen_consts.SUCCESS_TASK_INSTANCE_STATE
        },
        {
            "path": "LETTER/SIMPLEX/FR",
            "outbound_filename": "psf_t_pcb_dst_simplex_frc_letters_afp_20250724152102.uatv",
            "job_status": "failure"
        }
    ]

    success_sql_query = read_file_env(
        f"{current_script_directory}/sql/test_mail_print_status_insert_afp_success_query.sql",
        "dev"
    )
    failure_sql_query = read_file_env(
        f"{current_script_directory}/sql/test_mail_print_status_insert_afp_failure_query.sql",
        "dev"
    )

    def read_file_env_side_effect(file_path, deploy_env):
        if "success" in file_path:
            return success_sql_query
        else:
            return failure_sql_query

    mock_read_file_env.side_effect = read_file_env_side_effect

    ti_mock = MagicMock()

    def ti_xcom_pull_side_effect(task_ids, map_indexes=None, key=None):
        if task_ids == doc_gen_consts.AFP_PROCESSING_VALID_ENV_VAR_TASK_ID and key == 'return_value':
            return env_var_list
        elif task_ids == doc_gen_consts.AFP_PROCESSING_WORKLOAD_TASK_ID:
            return env_var_list[map_indexes][key]

    ti_mock.xcom_pull.side_effect = ti_xcom_pull_side_effect

    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    instance.est_zone = "America/Toronto"
    instance.deploy_env = "dev"

    with pytest.raises(AirflowFailException):
        instance.insert_afp_status_and_check_failures(dag_id=dag_id, dag_run=MagicMock(run_id=dag_run_id), ti=ti_mock)

    expected_calls = [
        call(success_sql_query.format(
            path=env_var_list[0]["path"],
            outbound_filename=env_var_list[0]["outbound_filename"],
            dag_id=dag_id,
            dag_run_id=dag_run_id
        )),
        call(failure_sql_query.format(
            path=env_var_list[1]["path"],
            dag_id=dag_id,
            dag_run_id=dag_run_id
        ))
    ]

    mock_read_file_env.assert_has_calls([
        call(doc_gen_consts.MAIL_PRINT_STATUS_INSERT_AFP_SUCCESS_SQL_PATH, instance.deploy_env),
        call(doc_gen_consts.MAIL_PRINT_STATUS_INSERT_AFP_FAILURE_SQL_PATH, instance.deploy_env)
    ], any_order=True)
    mock_run_bq_query.assert_has_calls(expected_calls, any_order=True)


@patch("doc_generation_processing.document_generation_dag.run_bq_query")
@patch('doc_generation_processing.document_generation_dag.read_file_env')
def test_insert_afp_status_and_check_failures_no_failure(mock_read_file_env,
                                                         mock_run_bq_query):
    dag_id = 'dag_id'
    dag_run_id = 'test_run_id'
    env_var_list = [
        {
            "path": "LETTER/SIMPLEX/EN",
            "outbound_filename": "psf_t_pcb_dst_simplex_eng_letters_afp_20250724152102.uatv",
            "job_status": doc_gen_consts.SUCCESS_TASK_INSTANCE_STATE
        }
    ]

    success_sql_query = read_file_env(
        f"{current_script_directory}/sql/test_mail_print_status_insert_afp_success_query.sql",
        "dev"
    )
    mock_read_file_env.return_value = success_sql_query

    ti_mock = MagicMock()

    def ti_xcom_pull_side_effect(task_ids, map_indexes=None, key=None):
        if task_ids == doc_gen_consts.AFP_PROCESSING_VALID_ENV_VAR_TASK_ID and key == 'return_value':
            return env_var_list
        elif task_ids == doc_gen_consts.AFP_PROCESSING_WORKLOAD_TASK_ID:
            return env_var_list[map_indexes][key]

    ti_mock.xcom_pull.side_effect = ti_xcom_pull_side_effect

    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    instance.est_zone = "America/Toronto"
    instance.deploy_env = "dev"

    instance.insert_afp_status_and_check_failures(dag_id=dag_id, dag_run=MagicMock(run_id=dag_run_id), ti=ti_mock)

    expected_sql = success_sql_query.format(
        path=env_var_list[0]["path"],
        outbound_filename=env_var_list[0]["outbound_filename"],
        dag_id=dag_id,
        dag_run_id=dag_run_id)

    mock_read_file_env.assert_called_once_with(doc_gen_consts.MAIL_PRINT_STATUS_INSERT_AFP_SUCCESS_SQL_PATH, instance.deploy_env)
    mock_run_bq_query.assert_called_once_with(expected_sql)


def test_test_insert_afp_status_and_check_failures_skip_empty_list():
    dag_id = 'dag_id'
    dag_run_id = 'test_run_id'
    env_var_list = []

    ti_mock = MagicMock()
    ti_mock.xcom_pull.return_value = env_var_list

    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    instance.est_zone = "America/Toronto"
    instance.deploy_env = "dev"

    with pytest.raises(AirflowSkipException):
        instance.insert_afp_status_and_check_failures(dag_id=dag_id, dag_run=MagicMock(run_id=dag_run_id), ti=ti_mock)


def test_test_insert_afp_status_and_check_failures_skip_no_list():
    dag_id = 'dag_id'
    dag_run_id = 'test_run_id'

    ti_mock = MagicMock()
    ti_mock.xcom_pull.return_value = None

    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    instance.est_zone = "America/Toronto"
    instance.deploy_env = "dev"

    with pytest.raises(AirflowSkipException):
        instance.insert_afp_status_and_check_failures(dag_id=dag_id, dag_run=MagicMock(run_id=dag_run_id), ti=ti_mock)


@patch("doc_generation_processing.document_generation_dag.run_bq_query")
@patch('doc_generation_processing.document_generation_dag.read_file_env')
def test_run_sql_to_create_parquet_success(mock_read_file_env,
                                           mock_run_bq_query):
    count_sql_query = read_file_env(
        f"{current_script_directory}/sql/test_mail_print_status_count_query.sql",
        "dev"
    )
    parquet_sql_query = read_file_env(
        f"{current_script_directory}/sql/test_mail_print_status_parquet_query.sql",
        "dev"
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

    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    instance.est_zone = "America/Toronto"

    instance.run_sql_to_create_parquet(unique_folder_path=UNIQUE_FOLDER_PATH,
                                       dag_id='dag_id',
                                       dag_run=MagicMock(run_id="test_run_id"))

    mock_read_file_env.assert_has_calls([
        call(doc_gen_consts.MAIL_PRINT_STATUS_COUNT_SQL_PATH, instance.deploy_env),
        call(doc_gen_consts.MAIL_PRINT_STATUS_PARQUET_SQL_PATH, instance.deploy_env)
    ], any_order=True)

    expected_calls = [
        call(count_sql_query.format(
            dag_id='dag_id',
            dag_run_id='test_run_id'
        )),
        call(parquet_sql_query.format(
            unique_folder_path=UNIQUE_FOLDER_PATH,
            dag_id='dag_id',
            dag_run_id='test_run_id'
        ))
    ]
    mock_run_bq_query.assert_has_calls(expected_calls, any_order=True)


@patch("doc_generation_processing.document_generation_dag.run_bq_query")
@patch('doc_generation_processing.document_generation_dag.read_file_env')
def test_run_sql_to_create_parquet_skip(mock_read_file_env,
                                        mock_run_bq_query):
    count_sql_query = read_file_env(
        f"{current_script_directory}/sql/test_mail_print_status_count_query.sql",
        "dev"
    )

    mock_read_file_env.return_value = count_sql_query

    mock_row = MagicMock()
    mock_row.record_count = 0
    mock_job = MagicMock()
    mock_job.result.return_value = [mock_row]
    mock_run_bq_query.return_value = mock_job

    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    instance.est_zone = "America/Toronto"

    with pytest.raises(AirflowSkipException):
        instance.run_sql_to_create_parquet(unique_folder_path=UNIQUE_FOLDER_PATH, dag_id='dag_id', dag_run=MagicMock(run_id="test_run_id"))

    mock_read_file_env.assert_called_once_with(doc_gen_consts.MAIL_PRINT_STATUS_COUNT_SQL_PATH, instance.deploy_env)

    expected_count_sql = count_sql_query.format(
        dag_id='dag_id',
        dag_run_id='test_run_id'
    )
    mock_run_bq_query.assert_called_once_with(expected_count_sql)


@patch("doc_generation_processing.document_generation_dag.PythonOperator")
@patch("doc_generation_processing.document_generation_dag.EmptyOperator")
@patch("doc_generation_processing.document_generation_dag.CustomGKEStartPodOperator")
@patch("doc_generation_processing.document_generation_dag.TriggerDagRunOperator")
def test_create_dag_non_dev_environment(mock_trigger_dag_run,
                                        mock_gke_start_pod,
                                        mock_empty,
                                        mock_python):
    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    instance.default_args = {}
    instance.deploy_env = 'uat'
    dag_config = instance.dag_config[consts.DAG]
    xcom_unique_folder_path = "{{ task_instance.xcom_pull(task_ids='create_unique_folder_path', key='return_value') }}"

    fixed_now = datetime(2024, 4, 2, 9, 0, 0)
    with patch("doc_generation_processing.document_generation_dag.datetime") as mock_datetime:
        mock_datetime.now.return_value = fixed_now
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        instance.create_dag("dag_id", instance.dag_config)

    mock_empty.assert_has_calls([
        call(task_id=consts.START_TASK_ID),
        call(task_id=consts.END_TASK_ID)
    ])

    mock_python.assert_has_calls([
        call(task_id="create_unique_folder_path", python_callable=instance.create_unique_folder_path),
        call(task_id="export_and_transform_bq_data", python_callable=instance.fetch_and_export_data, op_kwargs={
            'start_time': "{{ dag_run.conf.get('start_time') }}",
            'end_time': "{{ dag_run.conf.get('end_time') }}",
            'unique_folder_path': xcom_unique_folder_path
        }),
        call(task_id="process_data_and_generate_pdfs",
             python_callable=instance.process_json_to_pdf,
             op_kwargs={
                 'dag_id': 'dag_id',
                 'bucket_name': instance.dag_config['dag']['input_bucket'],
                 'unique_folder_path': xcom_unique_folder_path,
                 'prefix': "data/",
                 'audience': instance.dag_config['dag']["cloud_function"].get("audience"),
                 'max_concurrent_records': instance.dag_config['dag']["cloud_function"].get("max_concurrent_records")
             }),
        call(task_id="get_valid_env_variables",
             python_callable=instance.get_valid_env_variables,
             op_kwargs={
                 'input_bucket': dag_config.get('input_bucket'),
                 'output_bucket': dag_config.get('workload').get('uat').get('output_bucket'),
                 'unique_folder_path': xcom_unique_folder_path,
                 'output_file_prefix': dag_config.get('workload').get('uat').get('afp_file_prefix'),
                 'output_file_extension': dag_config.get('workload').get('uat').get('afp_file_extension'),
                 'afp_config': dag_config.get('workload').get('afp')
             }),
        call(task_id="insert_afp_status_and_check_failures",
             trigger_rule=TriggerRule.ALL_DONE,
             python_callable=instance.insert_afp_status_and_check_failures,
             op_kwargs={
                 consts.DAG_ID: 'dag_id'
             }),
        call(task_id="run_sql_to_create_status_parquet",
             trigger_rule=TriggerRule.ALL_DONE,
             python_callable=instance.run_sql_to_create_parquet,
             op_kwargs={
                 'unique_folder_path': xcom_unique_folder_path,
                 consts.DAG_ID: 'dag_id'
             })
    ])

    mock_gke_start_pod.partial.assert_called_once_with(
        task_id="generate_and_upload_afp",
        name="pdf-to-afp-converter-pod",
        namespace=dag_config['workload'].get('namespace'),
        project_id=dag_config['workload'].get(instance.deploy_env).get(consts.PROJECT_ID),
        location=dag_config.get(consts.LOCATION),
        cluster_name=dag_config['workload'].get(instance.deploy_env).get(consts.CLUSTER_NAME),
        image=dag_config['workload'].get(instance.deploy_env).get('image'),
        service_account_name="pdf-job-runner",
        node_selector={'nodepoolpurpose': 'batch'},
        image_pull_secrets=[V1LocalObjectReference(name='docker-registry')],
        tolerations=[V1Toleration(key='nodepoolpurpose', operator='Equal', value='batch', effect='NoSchedule')],
        volumes=[V1Volume(name='license-volume', secret=V1SecretVolumeSource(secret_name='secretkv', items=[{
            'key': 'prolcnse.lic.b64',
            'path': 'prolcnse.lic'
        }]))],
        volume_mounts=[V1VolumeMount(mount_path='/vault/secrets', name='license-volume', read_only=True)],
        on_success_callback=ANY,
        is_delete_operator_pod=True
    )
    mock_gke_start_pod.partial().expand.assert_called_once_with(env_vars=ANY)

    mock_trigger_dag_run.assert_called_with(
        task_id=dag_config.get("kafka_config").get(consts.KAFKA_WRITER_TASK_ID),
        trigger_dag_id=dag_config.get("kafka_config").get(consts.KAFKA_TRIGGER_DAG_ID),
        logical_date=fixed_now,
        conf={
            'bucket': dag_config['kafka_config'].get(consts.BUCKET),
            'folder_name': f"{dag_config['kafka_config'].get(consts.FOLDER_PREFIX)}/dag_id/{xcom_unique_folder_path}",
            'file_name': dag_config['kafka_config'].get(consts.FILE_NAME),
            'cluster_name': dag_config['kafka_config'].get(consts.KAFKA_CLUSTER_NAME)
        },
        wait_for_completion=True,
        poke_interval=30
    )


@patch("doc_generation_processing.document_generation_dag.PythonOperator")
@patch("doc_generation_processing.document_generation_dag.EmptyOperator")
@patch("doc_generation_processing.document_generation_dag.TriggerDagRunOperator")
def test_create_dag_dev_environment(mock_trigger_dag_run,
                                    mock_empty,
                                    mock_python):
    instance = DocumentGeneratorDagBuilder(CONFIG_PATH)
    instance.default_args = {}
    instance.deploy_env = 'dev'
    xcom_unique_folder_path = "{{ task_instance.xcom_pull(task_ids='create_unique_folder_path', key='return_value') }}"

    fixed_now = datetime(2024, 4, 2, 9, 0, 0)
    with patch("doc_generation_processing.document_generation_dag.datetime") as mock_datetime:
        mock_datetime.now.return_value = fixed_now
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        instance.create_dag("dag_id", instance.dag_config)

    mock_empty.assert_has_calls([
        call(task_id="start"),
        call(task_id="no_op_afp"),
        call(task_id="end")
    ])

    mock_python.assert_has_calls([
        call(task_id="create_unique_folder_path", python_callable=instance.create_unique_folder_path),
        call(task_id="export_and_transform_bq_data", python_callable=instance.fetch_and_export_data, op_kwargs={
            'start_time': "{{ dag_run.conf.get('start_time') }}",
            'end_time': "{{ dag_run.conf.get('end_time') }}",
            'unique_folder_path': xcom_unique_folder_path
        }),
        call(task_id="process_data_and_generate_pdfs",
             python_callable=instance.process_json_to_pdf,
             op_kwargs={
                 'dag_id': 'dag_id',
                 'bucket_name': instance.dag_config['dag']['input_bucket'],
                 'unique_folder_path': xcom_unique_folder_path,
                 'prefix': "data/",
                 'audience': instance.dag_config['dag']["cloud_function"].get("audience"),
                 'max_concurrent_records': instance.dag_config['dag']["cloud_function"].get("max_concurrent_records")
             }),
        call(task_id="run_sql_to_create_status_parquet",
             trigger_rule=TriggerRule.ALL_DONE,
             python_callable=instance.run_sql_to_create_parquet,
             op_kwargs={
                 'unique_folder_path': xcom_unique_folder_path,
                 consts.DAG_ID: 'dag_id'
             })
    ])

    dag_config = instance.dag_config[consts.DAG]
    mock_trigger_dag_run.assert_called_with(
        task_id=dag_config.get("kafka_config").get(consts.KAFKA_WRITER_TASK_ID),
        trigger_dag_id=dag_config.get("kafka_config").get(consts.KAFKA_TRIGGER_DAG_ID),
        logical_date=fixed_now,
        conf={
            'bucket': dag_config['kafka_config'].get(consts.BUCKET),
            'folder_name': f"{dag_config['kafka_config'].get(consts.FOLDER_PREFIX)}/dag_id/{xcom_unique_folder_path}",
            'file_name': dag_config['kafka_config'].get(consts.FILE_NAME),
            'cluster_name': dag_config['kafka_config'].get(consts.KAFKA_CLUSTER_NAME)
        },
        wait_for_completion=True,
        poke_interval=30
    )
