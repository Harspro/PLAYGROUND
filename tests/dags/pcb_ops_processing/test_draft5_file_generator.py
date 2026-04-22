import pytest
import pandas as pd
import util.constants as consts
from unittest.mock import patch, call, MagicMock
from dataclasses import asdict
from pathlib import Path
from util.auditing_utils.model.audit_record import AuditRecord
from util.miscutils import sanitize_string
from pcb_ops_processing.fee_reversal.draft5_file_generator import (
    RECORD_LENGTH,
    ESID_HEADER_LENGTH,
    build_transmit_header_str,
    build_batch_header_str,
    assemble_output_file_path,
    create_audit_record,
    Draft5FileGenerator
)


def test_build_transmit_header_str():
    bank_number = "7237"
    julian_date = "123"
    gregorian_date = "010125"
    esid = "7234AD"

    result = build_transmit_header_str(bank_number, julian_date, gregorian_date, esid)
    expected = (
        f"00000019010{bank_number}0001I7607INS            {julian_date}{gregorian_date}{esid.ljust(ESID_HEADER_LENGTH)}"
        + "                                           000                           N"
    )
    assert result == expected
    assert len(result) == RECORD_LENGTH


def test_build_batch_header_str():
    bank_number = "7637"
    julian_date = "123"

    result = build_batch_header_str(julian_date, bank_number)
    expected = f"00000029012{bank_number}DRFT5{julian_date}0001   CLR00000000000".ljust(RECORD_LENGTH)

    assert result == expected
    assert len(result) == RECORD_LENGTH


def test_assemble_output_file_path():
    product = "pcmc"
    ticket_id = "INC12345678"
    file_suffix = "uatv"
    timestamp = "20230101000000"

    output_file_path, output_file_name = assemble_output_file_path(product, ticket_id, file_suffix, timestamp)

    expected_output_file_name = "pcb_tsys_pcmc_monsbm_INC12345678_20230101000000.uatv"
    expected_output_file_path = f"pcb_tsys_pcmc_monsbm/{expected_output_file_name}"

    assert output_file_name == expected_output_file_name
    assert output_file_path == expected_output_file_path


@pytest.fixture
def dag_config_fixture():
    mock_dag = MagicMock()

    mock_dag_run = MagicMock()
    mock_dag_run.conf = {
        "staging_bucket": "pcb-dev-staging-extract"
    }

    mock_task_instance = MagicMock()
    mock_task_instance.xcom_push.return_value = None

    return {
        'dag_run': mock_dag_run,
        'dag': mock_dag,
        'ti': mock_task_instance,
        'run_id': 'manual__2025-02-11T16:15:10+00:00'
    }


@pytest.fixture
def dag_context_fixture():
    mock_dag = MagicMock()
    mock_dag.dag_id = "pcb_ops_monsbm_file_generation"

    mock_dag_run = MagicMock()
    mock_dag_run.conf = {
        "bucket": "pcb-ops-inbound-dev",
        "file_name": "pcb_ops_pcmc_monsbm_inc12345678_20250621122600.uatv",
        "folder_name": "pcb_ops_pcmc_monsbm",
        "name": "pcb_ops_pcmc_monsbm/pcb_ops_pcmc_monsbm_inc12345678_20250621122600.uatv",
    }

    mock_task_instance = MagicMock()
    mock_task_instance.xcom_push.return_value = None

    return {
        'dag_run': mock_dag_run,
        'dag': mock_dag,
        'ti': mock_task_instance,
        'run_id': 'manual__2025-02-11T16:15:10+00:00'
    }


@pytest.fixture
def audit_record_fixture():
    expected_audit_record_values = """
                        'pcb-dev-landing',
                        '',
                        '',
                        'file',
                        'gs://pcb-ops-inbound-dev/pcb_ops_pcmc_monsbm/pcb_ops_pcmc_monsbm_inc12345678_20250621122600.uatv',
                        'pcb_ops_pcmc_monsbm_inc12345678_20250621122600.uatv',
                        'pcb-dev-processing',
                        'gs://pcb-dev-staging-extract/pcb_tsys_pcmc_monsbm/pcb_tsys_pcmc_monsbm_inc12345678_20250621122611.uatv',
                        'pcb_ops_monsbm_file_generation',
                        'manual__2025-02-11T16:15:10+00:00',
                        'FEE_REVERSAL',
                        'manual',
                        'pcb-dev-landing',
                        CURRENT_TIMESTAMP()
    """

    audit_record = AuditRecord(dataset_id='domain_technical',
                               table_id='DATA_TRANSFER_AUDIT',
                               audit_record_values=expected_audit_record_values)

    return audit_record


@patch('pcb_ops_processing.fee_reversal.draft5_file_generator.AuditUtil')
@patch('pcb_ops_processing.fee_reversal.draft5_file_generator.create_data_transfer_audit_table_if_not_exists')
@patch('pcb_ops_processing.fee_reversal.draft5_file_generator.assemble_output_file_path')
def test_create_audit_record(
        mock_assemble_output_file_path,
        mock_create_audit_table,
        mock_audit_util,
        dag_context_fixture: dict,
        audit_record_fixture: AuditRecord
):
    mock_assemble_output_file_path.return_value = ('pcb_tsys_pcmc_monsbm/pcb_tsys_pcmc_monsbm_inc12345678_20250621122611.uatv',
                                                   'pcb_tsys_pcmc_monsbm_inc12345678_20250621122611.uatv')
    config = {
        'staging_bucket': 'pcb-dev-staging',
        'destination_bucket': 'pcb-dev-staging-extract',
        'filename_prefix': 'pcb_tsys_pcmc_monsbm'
    }
    # Call the create_audit_record function
    create_audit_record(mock_audit_util, config, 'dev', 'pcb-dev-landing', 'pcb-dev-processing', **dag_context_fixture)

    mock_create_audit_table.assert_called_once()

    mock_audit_util.record_request_received.assert_called_once()
    audit_record_call_arg = mock_audit_util.record_request_received.call_args.args[0]
    assert (audit_record_call_arg.dataset_id == audit_record_fixture.dataset_id)
    assert (audit_record_call_arg.table_id == audit_record_fixture.table_id)
    assert (sanitize_string(audit_record_call_arg.audit_record_values) == sanitize_string(audit_record_fixture.audit_record_values))

    mock_ti = dag_context_fixture['ti']

    audit_record_call = call.xcom_push(key='audit_record', value=asdict(audit_record_call_arg))
    source_path_call = call.xcom_push(key='source_path',
                                      value='pcb_ops_pcmc_monsbm/pcb_ops_pcmc_monsbm_inc12345678_20250621122600.uatv')
    output_file_name_call = call.xcom_push(key='output_file_name', value='pcb_tsys_pcmc_monsbm_inc12345678_20250621122611.uatv')
    output_file_path_call = call.xcom_push(key='output_file_path',
                                           value='pcb_tsys_pcmc_monsbm/pcb_tsys_pcmc_monsbm_inc12345678_20250621122611.uatv')
    product_call = call.xcom_push(key='product', value='pcmc')

    mock_ti.assert_has_calls([product_call, audit_record_call, source_path_call, output_file_path_call, output_file_name_call])


@patch("google.cloud.bigquery.Client")
@patch('etl_framework.etl_dag_base.read_yamlfile_env_suffix')
@patch('pcb_ops_processing.fee_reversal.draft5_file_generator.run_bq_query')
def test_get_trailer_data(mock_run_bq_query, mock_read_yamlfile_env_suffix, mock_bq_client):
    mock_read_yamlfile_env_suffix.return_value = {
        'fee.reversal.staging.sum.view.id': 'test_project.test_dateset.test_view'
    }

    query_job = MagicMock()
    mock_run_bq_query.return_value = query_job

    query_job.result.to_dateframe.return_value = pd.DataFrame({
        'RECORD_COUNT': [0],
        'NET_AMOUNT': [0],
        'TOTAL_REFUND': [0]
    })

    draft5_gen = Draft5FileGenerator(module_name='pcb_ops_processing',
                                     config_path='fee_reversal/config',
                                     config_filename='pcb_ops_fee_reversal_dags_config.yaml',
                                     dag_default_args={})

    draft5_gen.transformation_config = {
        'fee.reversal.staging.sum.view.id': 'test_project.test_dateset.test_view'
    }

    draft5_gen.get_trailer_data()

    mock_read_yamlfile_env_suffix.assert_called()


@patch("google.cloud.bigquery.Client")
@patch('etl_framework.etl_dag_base.read_yamlfile_env_suffix')
@patch('pcb_ops_processing.fee_reversal.draft5_file_generator.run_bq_query')
def test_get_transaction_type(mock_run_bq_query, mock_read_yamlfile_env_suffix, mock_bq_client):
    mock_read_yamlfile_env_suffix.return_value = {
        'fee.reversal.staging.input.external.table.id': 'test_project.test_dateset.test_view',
        'draft5.ref.transaction.code.table.id': 'test_project.test_dateset.test_view'
    }

    query_job = MagicMock()
    mock_run_bq_query.return_value = query_job

    query_job.result.to_dateframe.return_value = pd.DataFrame({
        'TRANSACTION_CODE': [0],
        'TRANSACTION_TYPE': [0],
    })

    draft5_gen = Draft5FileGenerator(module_name='pcb_ops_processing',
                                     config_path='fee_reversal/config',
                                     config_filename='pcb_ops_fee_reversal_dags_config.yaml',
                                     dag_default_args={})

    draft5_gen.transformation_config = {
        'context_parameter': {
            'fee.reversal.staging.input.external.table.id': 'test_project.test_dataset.input_table',
            'draft5.ref.transaction.code.table.id': 'test_project.test_dataset.transaction_code_table'
        }
    }

    draft5_gen.get_transaction_type()

    mock_read_yamlfile_env_suffix.assert_called()


@patch('pcb_ops_processing.fee_reversal.draft5_file_generator.read_file_env')
@patch.object(Draft5FileGenerator, 'get_trailer_data', return_value=('0', '0', '0'))
@patch.object(Draft5FileGenerator, 'get_transaction_type', return_value=('REVERSAL', '397'))
def test_create_email(mock_get_transaction_type, mock_get_trailer_data, mock_read_file_env, dag_context_fixture):
    mock_read_file_env.return_value = '<html>test</html>'
    draft5_gen = Draft5FileGenerator(module_name='pcb_ops_processing',
                                     config_path='fee_reversal/config',
                                     config_filename='pcb_ops_fee_reversal_dags_config.yaml',
                                     dag_default_args={})

    draft5_gen.transformation_config = {
        'fee.reversal.staging.sum.view.id': 'test_project.test_dateset.test_view',
        'fee.reversal.staging.input.external.table.id': 'project.dataset.input_table',
        'draft5.ref.transaction.code.table.id': 'project.dataset.transaction_code_table'
    }

    config = {
        'destination_bucket': 'pcb-dev-staging-extract',
        'email': {
            'subject': "TSYS DRAFT5 Submission Request (C86 PCMC)",
            'approver': "Brendan,Omey",
            'operator_id': "SDDATA",
            'description': "Draft5 - C86 Refund",
            'batch_type': "Draft5 - C86 Refund",
            'purpose': "For PCMC - Refund",
            'recipients': {
                'dev': "daniel.zhao@pcbank.ca, Himanshu.Guliani@pcbank.ca",
                'uat': "radhika.solanki@pcbank.ca, Madhu.Chatterjee@pcbank.ca",
                'prod': "Pcf-Techops@pcbank.ca"
            }
        }
    }
    draft5_gen.create_email(config, 'dev',
                            'pcb_tsys_pcmc_monsbm/pcb_ops_pcmc_monsbm_inc12345678_20250621122611.uatv',
                            'pcmc',
                            **dag_context_fixture)

    mock_ti = dag_context_fixture['ti']
    subject_call = call.xcom_push(key=consts.SUBJECT,
                                  value='TSYS DRAFT5 Submission Request (C86 PCMC)')
    recipients_call = call.xcom_push(key=consts.RECIPIENTS,
                                     value='daniel.zhao@pcbank.ca, Himanshu.Guliani@pcbank.ca')
    mock_ti.assert_has_calls([subject_call, recipients_call])
    mock_get_trailer_data.assert_called()
    mock_get_transaction_type.assert_called()


@patch("pcb_ops_processing.fee_reversal.draft5_file_generator.gcs_file_exists", return_value=True)
@patch("pcb_ops_processing.fee_reversal.draft5_file_generator.create_external_table")
@patch("pcb_ops_processing.fee_reversal.draft5_file_generator.apply_column_transformation")
@patch("google.cloud.bigquery.Client")
def test_prepare_input_data(
    mock_bq_client,
    mock_apply_column_transformation,
    mock_create_external_table,
    mock_gcs_exists
):
    draft5_gen = Draft5FileGenerator(
        module_name="pcb_ops_processing",
        config_path="fee_reversal/config",
        config_filename="pcb_ops_fee_reversal_dags_config.yaml",
        dag_default_args={}
    )

    draft5_gen.transformation_config = {
        'context_parameter': {
            'fee.reversal.staging.input.external.table.id': 'test_project.test_dataset.test_table'
        }
    }

    config = {
        consts.STAGING_BUCKET: 'pcb-dev-staging-extract',
        'pcmc': {
            'bank_number': '7637',
            'esid': '7637AD'
        }
    }

    draft5_gen.prepare_input_data(
        config=config,
        source_path="pcb_tsys_pcmc_monsbm/pcb_ops_pcmc_monsbm_inc12345678_20250621122611.uatv",
        output_file_name="pcb_tsys_pcmc_monsbm_inc12345678_20250621122611.uatv",
        product="pcmc"
    )

    mock_gcs_exists.assert_called_once()
    mock_create_external_table.assert_called_once()
    mock_apply_column_transformation.assert_called_once()


@patch("pcb_ops_processing.fee_reversal.draft5_file_generator.Draft5FileGenerator.write_query_results")
@patch("google.cloud.storage.Client")
def test_write_draft5_file(mock_storage_client, mock_write_query):
    mock_blob = MagicMock()
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_blob_writer = MagicMock()
    mock_blob.open.return_value.__enter__.return_value = mock_blob_writer
    mock_storage_client.return_value.bucket.return_value = mock_bucket

    draft5_gen = Draft5FileGenerator(
        module_name="pcb_ops_processing",
        config_path="fee_reversal/config",
        config_filename="pcb_ops_fee_reversal_dags_config.yaml",
        dag_default_args={}
    )

    config = {
        consts.DESTINATION_BUCKET: "pcb-dev-staging-extract",
        consts.CHARACTER_SET: "cp037",
        'pcmc': {
            'bank_number': "7637",
            'esid': "7637AD"
        }
    }

    output_file_path = "pcb_tsys_pcmc_monsbm/pcb_ops_pcmc_monsbm_inc12345678_20250621122611.uatv"

    draft5_gen.write_draft5_file(config, output_file_path, 'pcmc')

    mock_storage_client.assert_called_once()
    mock_bucket.blob.assert_called_once_with(output_file_path)
    mock_blob.open.assert_called_once_with("wb")
    assert mock_write_query.call_count == 3
