import pytest
from unittest.mock import MagicMock, patch, call, ANY
from tsys_processing.ts2_pcdownload_file_loader import PCDownloadFileLoader
from datetime import datetime
import pytz
import util.constants as consts
from airflow import DAG


@pytest.fixture
def mock_loader():
    with patch("tsys_processing.ts2_pcdownload_file_loader.PCDownloadFileLoader.__init__", return_value=None), \
         patch("tsys_processing.ts2_pcdownload_file_loader.get_cluster_name_for_dag", return_value="test-cluster"):
        loader = PCDownloadFileLoader("fake_config.yaml")
        loader.gcp_config = {
            consts.DEPLOY_ENV_STORAGE_SUFFIX: "-dev",
            consts.DEPLOYMENT_ENVIRONMENT_NAME: "dev",
            consts.PROCESSING_ZONE_CONNECTION_ID: "fake-conn-id",
            consts.LANDING_ZONE_PROJECT_ID: 'test-processing-project',
            consts.PROCESSING_ZONE_PROJECT_ID: 'test-processing-project'
        }
        loader.dataproc_config = {
            consts.PROJECT_ID: "fake-project",
            consts.LOCATION: "us-central1",
        }
        loader.default_args = {}
        loader.deploy_env = loader.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        loader.env_storage_suffix = loader.gcp_config[consts.DEPLOY_ENV_STORAGE_SUFFIX]
        loader.get_output_dir_path = MagicMock(return_value="mocked/output/dir")
        loader.audit_util = MagicMock()
        loader.extract_tables_info = MagicMock(return_value={})

        return loader


@patch("tsys_processing.ts2_pcdownload_file_loader.get_bucket_project_id", return_value="mocked-project-id")
@patch("tsys_processing.ts2_pcdownload_file_loader.create_data_transfer_audit_table_if_not_exists")
@patch("tsys_processing.ts2_pcdownload_file_loader.build_spark_logging_info", side_effect=lambda dag_id, default_args, arg_list: arg_list)
def test_prepare_decoding_config(
    mock_build_logging,
    mock_create_audit_table,
    mock_get_bucket_project_id,
    mock_loader,
):
    context = {
        "ti": MagicMock(),
        "dag_run": MagicMock(conf={"bucket": "source-bucket", "name": "tsys_pcb_pcdownload_20250813185549.dat"}, run_type="manual", run_id="manual__runid123"),
        "dag": MagicMock(dag_id="test_dag"),
        "run_id": "manual__runid123",
    }
    context["ti"].xcom_pull.return_value = "2025-08-13"
    dag_config = {
        consts.SPARK: {
            consts.DECODING_JOB_ARGS: {"key1": "value1"},
            consts.JAR_FILE_URIS: ["gs://fake-bucket/jar1.jar"],
            consts.MAIN_CLASS: "com.test.MainClass",
        },
        consts.OUTPUTFILE_FILE_PREFIX: "test_prefix",
        consts.PROCESSING_BUCKET: "source-bucket",
        consts.PROCESSING_BUCKET_EXTRACT: "extract-bucket",
        consts.DESTINATION_BUCKET: "dest-bucket"
    }

    job_config = mock_loader.prepare_decoding_config(dag_id="test_dag", file_name="tsys_pcb_pcdownload_20250813185549.dat", dag_config=dag_config, cluster_name="test-cluster", **context)

    # Verify expected job config keys
    assert consts.SPARK_JOB in job_config
    assert job_config[consts.SPARK_JOB][consts.MAIN_CLASS] == "com.test.MainClass"
    assert any("key1=value1" in arg for arg in job_config[consts.SPARK_JOB][consts.ARGS])

    # Verify xcom_push calls
    expected_keys = [
        "decoding_job_config", "source_path", "processing_extract_path",
        "destination_bucket", "processing_extract_bucket", "destination_path"
    ]
    for key in expected_keys:
        context["ti"].xcom_push.assert_any_call(key=key, value=ANY)

    mock_create_audit_table.assert_called_once()
    mock_loader.audit_util.record_request_received.assert_called_once()

    spark_args = job_config[consts.SPARK_JOB][consts.ARGS]
    output_arg = next((arg for arg in spark_args if "pcb.tsys.processor.output.path=" in arg), None)

    assert output_arg is not None, "Expected pcb.tsys.processor.output.path argument not found"

    expected_output_path = "gs://extract-bucket/test_prefix/test_prefix_20250813185549.uatv"
    expected_output_arg = f"pcb.tsys.processor.output.path={expected_output_path}"

    assert output_arg == expected_output_arg, (
        f"Output path mismatch.\nExpected: {expected_output_arg}\nActual:   {output_arg}"
    )


@patch("tsys_processing.ts2_pcdownload_file_loader.save_job_to_control_table")
def test_build_control_record_saving_job(mock_save_job, mock_loader):
    context = {"ti": MagicMock()}
    context["ti"].xcom_pull.side_effect = lambda task_ids, key: f"mocked_{key}"

    mock_loader.build_control_record_saving_job("testfile.dat", "mocked/output/dir", **context)

    # Verify save_job_to_control_table call with correct JSON
    mock_save_job.assert_called_once()
    call_args = mock_save_job.call_args[0][0]
    assert "testfile.dat" in call_args
    assert "mocked_processing_extract_bucket" in call_args


@patch("tsys_processing.ts2_pcdownload_file_loader.PythonOperator")
@patch("tsys_processing.ts2_pcdownload_file_loader.DataprocSubmitJobOperator")
@patch("tsys_processing.ts2_pcdownload_file_loader.GCSToGCSOperator")
def test_build_postprocessing_task_group(mock_gcs, mock_dataproc, mock_python, mock_loader):
    from util import constants as consts

    dag_config = {
        consts.SPARK: {
            consts.DECODING_JOB_ARGS: {},
            consts.JAR_FILE_URIS: ["jar"],
            consts.MAIN_CLASS: "MainClass",
        },
        consts.OUTPUTFILE_FILE_PREFIX: "prefix",
        consts.PROCESSING_BUCKET: "bucket",
        consts.PROCESSING_BUCKET_EXTRACT: "extract-bucket",
        consts.DESTINATION_BUCKET: "destination",
    }

    mock_job_operator = MagicMock()
    mock_dataproc.return_value = mock_job_operator

    with DAG(dag_id="test_dag", start_date=datetime(2023, 1, 1)) as dag:
        tg = mock_loader.build_postprocessing_task_group("test_dag", dag_config)
        assert tg.group_id == "post_processing"
        assert dag.dag_id == "test_dag"

    assert mock_python.call_count == 3
    assert mock_dataproc.call_count == 1
    assert mock_gcs.call_count == 1
