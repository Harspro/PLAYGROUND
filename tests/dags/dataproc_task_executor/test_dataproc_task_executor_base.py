import os
from unittest.mock import patch, MagicMock
import pytest

from airflow import DAG
from airflow.exceptions import AirflowFailException
from datetime import datetime

import util.constants as consts
from dataproc_task_executor.dataproc_task_executor_base import DataprocTaskExecutor


class TestDataprocTaskExecutor:

    def _mock_read_variable_or_file(self, var_str, deploy_env=None):
        if var_str == consts.GCP_CONFIG:
            return {
                consts.DEPLOYMENT_ENVIRONMENT_NAME: "dev",
                consts.DEPLOY_ENV_STORAGE_SUFFIX: "-dev",
                consts.NETWORK_TAG: "pcb-network",
                consts.PROCESSING_ZONE_CONNECTION_ID: "gcp_default",
            }
        if var_str == consts.DATAPROC_CONFIG:
            return {
                consts.LOCATION: "northamerica-northeast1",
                consts.PROJECT_ID: "pcb-dev-processing",
            }
        return {}

    def _mock_get_cluster_config_dev(self):
        """Helper to mock dev environment cluster config"""
        return (
            "https://www.googleapis.com/compute/v1/projects/pcb-nonprod-svpc/regions/northamerica-northeast1/subnetworks/snet-pcbnp-app01-dev",
            ["pcb-network", "processing"],
            "dataproc@pcb-dev-processing.iam.gserviceaccount.com"
        )

    def _mock_get_cluster_config_prod(self):
        """Helper to mock prod environment cluster config"""
        return (
            "https://www.googleapis.com/compute/v1/projects/pcb-prod-svpc/regions/northamerica-northeast1/subnetworks/snet-pcbpr-app01-nane1",
            ["pcb-network", "processing"],
            "dataproc@pcb-prod-processing.iam.gserviceaccount.com"
        )

    @patch("dataproc_task_executor.dataproc_task_executor_base.DataprocCreateBatchOperator")
    @patch("dataproc_task_executor.dataproc_task_executor_base.get_serverless_cluster_config")
    @patch("dataproc_task_executor.dataproc_task_executor_base.read_yamlfile_env")
    @patch("dataproc_task_executor.dataproc_task_executor_base.read_variable_or_file")
    def test_create_dag_minimal_creates_task(self, mock_read_var, mock_read_yaml, mock_get_cluster_config, mock_batch_op):
        mock_read_var.side_effect = self._mock_read_variable_or_file
        mock_read_yaml.return_value = {}
        mock_get_cluster_config.return_value = self._mock_get_cluster_config_dev()

        executor = DataprocTaskExecutor("unused.yaml")

        config = {
            consts.DEFAULT_ARGS: {},
            consts.DAG: {
                consts.DAGRUN_TIMEOUT: 10,
                consts.DESCRIPTION: "test",
                consts.TAGS: ["test"],
                consts.SCHEDULE_INTERVAL: None,
            },
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: ["gs://pcb-dev-staging-artifacts/jarfiles/app.jar"],
                consts.MAIN_CLASS: "com.example.Main"
            }
        }

        dag = executor.create_dag("test_dataproc_min", config)
        assert isinstance(dag, DAG)

        # Ensure operator is constructed once (mocked)
        mock_batch_op.assert_called_once()

        # Ensure start and end tasks exist on DAG
        assert consts.START_TASK_ID in dag.task_ids
        assert consts.END_TASK_ID in dag.task_ids

        # Validate args default to empty list when omitted
        called_kwargs = mock_batch_op.call_args.kwargs
        assert called_kwargs["batch"]["spark_batch"]["args"] == []

        # Validate serverless cluster config is used
        mock_get_cluster_config.assert_called_once_with("dev", "pcb-network")
        execution_config = called_kwargs["batch"]["environment_config"]["execution_config"]
        assert execution_config["subnetwork_uri"] == "https://www.googleapis.com/compute/v1/projects/pcb-nonprod-svpc/regions/northamerica-northeast1/subnetworks/snet-pcbnp-app01-dev"
        assert execution_config["network_tags"] == ["pcb-network", "processing"]
        assert execution_config["service_account"] == "dataproc@pcb-dev-processing.iam.gserviceaccount.com"

    @patch("dataproc_task_executor.dataproc_task_executor_base.DataprocCreateBatchOperator")
    @patch("dataproc_task_executor.dataproc_task_executor_base.get_serverless_cluster_config")
    @patch("dataproc_task_executor.dataproc_task_executor_base.read_yamlfile_env")
    @patch("dataproc_task_executor.dataproc_task_executor_base.read_variable_or_file")
    def test_args_dict_converted_to_list(self, mock_read_var, mock_read_yaml, mock_get_cluster_config, mock_batch_op):
        mock_read_var.side_effect = self._mock_read_variable_or_file
        mock_read_yaml.return_value = {}
        mock_get_cluster_config.return_value = self._mock_get_cluster_config_dev()

        executor = DataprocTaskExecutor("unused.yaml")

        config = {
            consts.DEFAULT_ARGS: {},
            consts.DAG: {
                consts.DAGRUN_TIMEOUT: 10,
                consts.DESCRIPTION: "test",
                consts.TAGS: ["test"],
                consts.SCHEDULE_INTERVAL: None,
            },
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: ["gs://pcb-dev-staging-artifacts/jarfiles/app.jar"],
                consts.MAIN_CLASS: "com.example.Main",
                consts.ARGS: {
                    "k1": "v1",
                    "k2": "v2"
                }
            }
        }

        executor.create_dag("test_dataproc_args", config)

        called_kwargs = mock_batch_op.call_args.kwargs
        args_list = called_kwargs["batch"]["spark_batch"]["args"]
        assert "k1=v1" in args_list and "k2=v2" in args_list

    @patch("dataproc_task_executor.dataproc_task_executor_base.DataprocCreateBatchOperator")
    @patch("dataproc_task_executor.dataproc_task_executor_base.get_serverless_cluster_config")
    @patch("dataproc_task_executor.dataproc_task_executor_base.read_yamlfile_env")
    @patch("dataproc_task_executor.dataproc_task_executor_base.read_variable_or_file")
    def test_args_list_passthrough(self, mock_read_var, mock_read_yaml, mock_get_cluster_config, mock_batch_op):
        mock_read_var.side_effect = self._mock_read_variable_or_file
        mock_read_yaml.return_value = {}
        mock_get_cluster_config.return_value = self._mock_get_cluster_config_dev()

        executor = DataprocTaskExecutor("unused.yaml")

        config = {
            consts.DEFAULT_ARGS: {},
            consts.DAG: {
                consts.DAGRUN_TIMEOUT: 10,
                consts.DESCRIPTION: "test",
                consts.TAGS: ["test"],
                consts.SCHEDULE_INTERVAL: None,
            },
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: ["gs://pcb-dev-staging-artifacts/jarfiles/app.jar"],
                consts.MAIN_CLASS: "com.example.Main",
                consts.ARGS: ["a=1", "b=2"]
            }
        }

        executor.create_dag("test_dataproc_args_list", config)

        called_kwargs = mock_batch_op.call_args.kwargs
        args_list = called_kwargs["batch"]["spark_batch"]["args"]
        assert args_list == ["a=1", "b=2"]

    @patch("dataproc_task_executor.dataproc_task_executor_base.get_serverless_cluster_config")
    @patch("dataproc_task_executor.dataproc_task_executor_base.read_yamlfile_env")
    @patch("dataproc_task_executor.dataproc_task_executor_base.read_variable_or_file")
    def test_missing_spark_job_raises(self, mock_read_var, mock_read_yaml, mock_get_cluster_config):
        mock_read_var.side_effect = self._mock_read_variable_or_file
        mock_read_yaml.return_value = {}
        mock_get_cluster_config.return_value = self._mock_get_cluster_config_dev()

        executor = DataprocTaskExecutor("unused.yaml")

        bad_config = {
            consts.DEFAULT_ARGS: {},
            consts.DAG: {
                consts.DAGRUN_TIMEOUT: 10,
                consts.DESCRIPTION: "test",
                consts.TAGS: ["test"],
                consts.SCHEDULE_INTERVAL: None,
            }
        }

        with pytest.raises(AirflowFailException):
            executor.create_dag("test_missing_spark_job", bad_config)

    @patch("dataproc_task_executor.dataproc_task_executor_base.get_serverless_cluster_config")
    @patch("dataproc_task_executor.dataproc_task_executor_base.read_yamlfile_env")
    @patch("dataproc_task_executor.dataproc_task_executor_base.read_variable_or_file")
    def test_missing_required_keys_raises(self, mock_read_var, mock_read_yaml, mock_get_cluster_config):
        mock_read_var.side_effect = self._mock_read_variable_or_file
        mock_read_yaml.return_value = {}
        mock_get_cluster_config.return_value = self._mock_get_cluster_config_dev()

        executor = DataprocTaskExecutor("unused.yaml")

        bad_config = {
            consts.DEFAULT_ARGS: {},
            consts.DAG: {
                consts.DAGRUN_TIMEOUT: 10,
                consts.DESCRIPTION: "test",
                consts.TAGS: ["test"],
                consts.SCHEDULE_INTERVAL: None,
            },
            consts.SPARK_JOB: {
                # Missing MAIN_CLASS
                consts.JAR_FILE_URIS: ["gs://pcb-dev-staging-artifacts/jarfiles/app.jar"],
            }
        }

        with pytest.raises(AirflowFailException):
            executor.create_dag("test_missing_required_keys", bad_config)

    @patch("dataproc_task_executor.dataproc_task_executor_base.DataprocCreateBatchOperator")
    @patch("dataproc_task_executor.dataproc_task_executor_base.get_serverless_cluster_config")
    @patch("dataproc_task_executor.dataproc_task_executor_base.read_yamlfile_env")
    @patch("dataproc_task_executor.dataproc_task_executor_base.read_variable_or_file")
    def test_prod_environment_uses_prod_subnetwork(self, mock_read_var, mock_read_yaml, mock_get_cluster_config, mock_batch_op):
        """Test that prod environment uses the dedicated prod subnetwork URI"""
        # Mock prod environment
        def mock_read_var_prod(var_str, deploy_env=None):
            if var_str == consts.GCP_CONFIG:
                return {
                    consts.DEPLOYMENT_ENVIRONMENT_NAME: "prod",
                    consts.DEPLOY_ENV_STORAGE_SUFFIX: "",
                    consts.NETWORK_TAG: "pcb-network",
                    consts.PROCESSING_ZONE_CONNECTION_ID: "gcp_default",
                }
            if var_str == consts.DATAPROC_CONFIG:
                return {
                    consts.LOCATION: "northamerica-northeast1",
                    consts.PROJECT_ID: "pcb-prod-processing",
                }
            return {}

        mock_read_var.side_effect = mock_read_var_prod
        mock_read_yaml.return_value = {}
        mock_get_cluster_config.return_value = self._mock_get_cluster_config_prod()

        executor = DataprocTaskExecutor("unused.yaml")

        config = {
            consts.DEFAULT_ARGS: {},
            consts.DAG: {
                consts.DAGRUN_TIMEOUT: 10,
                consts.DESCRIPTION: "test",
                consts.TAGS: ["test"],
                consts.SCHEDULE_INTERVAL: None,
            },
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: ["gs://pcb-prod-staging-artifacts/jarfiles/app.jar"],
                consts.MAIN_CLASS: "com.example.Main"
            }
        }

        executor.create_dag("test_dataproc_prod", config)

        # Verify prod subnetwork URI is used
        mock_get_cluster_config.assert_called_once_with("prod", "pcb-network")
        called_kwargs = mock_batch_op.call_args.kwargs
        execution_config = called_kwargs["batch"]["environment_config"]["execution_config"]
        assert execution_config["subnetwork_uri"] == "https://www.googleapis.com/compute/v1/projects/pcb-prod-svpc/regions/northamerica-northeast1/subnetworks/snet-pcbpr-app01-nane1"
        assert execution_config["service_account"] == "dataproc@pcb-prod-processing.iam.gserviceaccount.com"
