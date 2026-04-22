import os
from unittest.mock import patch, call, ANY
import pytest
from airflow import DAG
from airflow.exceptions import AirflowFailException
from python_task_executor.python_task_executor_base import PythonTaskExecutor
from datetime import datetime
from util.miscutils import read_yamlfile_env
from importlib import import_module

current_script_directory = os.path.dirname(os.path.abspath(__file__))
task_executor_config_directory = os.path.join(
    current_script_directory,
    "..",
    "config",
    "python_task_executor_configs",
)


class TestPythonTaskExecutor:
    def setup_method(self):
        os.makedirs(task_executor_config_directory, exist_ok=True)

    @patch("python_task_executor.python_task_executor_base.read_variable_or_file")
    def test_create_dags_with_empty_config(self, mock_read_variable):
        mock_read_variable.return_value = {
            "deployment_environment_name": "test-deployment-environment",
            "deploy_env_storage_suffix": "-test",
        }
        executor = PythonTaskExecutor("empty_config.yaml", task_executor_config_directory)
        executor.job_config = {}
        dags = executor.create_dags()
        assert dags == {}

    @patch("python_task_executor.python_task_executor_base.read_variable_or_file")
    def test_create_dags_with_invalid_yaml(self, mock_read_variable):
        mock_read_variable.return_value = {
            "deployment_environment_name": "test-deployment-environment",
            "deploy_env_storage_suffix": "-test",
        }
        with pytest.raises(AirflowFailException) as ex_info:
            PythonTaskExecutor("invalid_config.yaml", task_executor_config_directory)
        assert "Error while creating config for python task executor" in str(ex_info.value)

    @patch("python_task_executor.python_task_executor_base.PythonOperator")
    @patch("python_task_executor.python_task_executor_base.EmptyOperator")
    @patch("python_task_executor.python_task_executor_base.read_variable_or_file")
    def test_create_tasks(self, mock_read_variable, mock_empty_operator, mock_python_operator):
        mock_read_variable.return_value = {
            "deployment_environment_name": "test-deployment-environment",
            "deploy_env_storage_suffix": "-test",
        }

        config = read_yamlfile_env(
            f"{task_executor_config_directory}/tasks_config.yaml",
            "dev",
        )

        # ✅ Ensure config contains a "dag" section because implementation reads config["dag"]
        config.setdefault("dag", {"dagrun_timeout": 120})

        executor = PythonTaskExecutor("tasks_config.yaml")
        executor.dag = DAG(dag_id="dummy_dag", schedule=None, start_date=datetime(2023, 1, 1))
        executor._create_tasks(config)

        assert mock_empty_operator.call_count == 2
        assert mock_empty_operator.call_args_list[0][1]["task_id"] == "start"
        assert mock_empty_operator.call_args_list[1][1]["task_id"] == "end"

        mock_python_operator.assert_called_once()
        call_args = mock_python_operator.call_args[1]
        deploy_env = call_args["op_kwargs"]["deploy_env"]
        assert deploy_env == "dev"

    @patch("python_task_executor.python_task_executor_base.read_variable_or_file")
    def test_create_tasks_no_module_fail(self, mock_read_variable):
        mock_read_variable.return_value = {
            "deployment_environment_name": "test-deployment-environment",
            "deploy_env_storage_suffix": "-test",
        }
        config = read_yamlfile_env(
            f"{task_executor_config_directory}/no_module.yaml",
            "dev",
        )

        # ✅ Ensure config contains a "dag" section because implementation reads config["dag"]
        config.setdefault("dag", {"dagrun_timeout": 120})

        executor = PythonTaskExecutor("no_module.yaml")
        executor.dag = DAG(dag_id="dummy_dag", schedule=None, start_date=datetime(2023, 1, 1))
        with pytest.raises(AirflowFailException) as ex_info:
            executor._create_tasks(config)
        assert "util.test1234.test_method is not defined, hence cannot create tasks" in str(ex_info.value)

    @patch("python_task_executor.python_task_executor_base.read_variable_or_file")
    def test_create_tasks_no_python_callable(self, mock_read_variable):
        mock_read_variable.return_value = {
            "deployment_environment_name": "test-deployment-environment",
            "deploy_env_storage_suffix": "-test",
        }
        config = read_yamlfile_env(
            f"{task_executor_config_directory}/no_python_callable.yaml",
            "dev",
        )

        # ✅ Ensure config contains a "dag" section because implementation reads config["dag"]
        config.setdefault("dag", {"dagrun_timeout": 120})

        executor = PythonTaskExecutor("no_python_callable.yaml")
        executor.dag = DAG(dag_id="dummy_dag", schedule=None, start_date=datetime(2023, 1, 1))
        with pytest.raises(AirflowFailException) as ex_info:
            executor._create_tasks(config)
        assert "Please provide python callable, none found for task" in str(ex_info.value)

    @patch("python_task_executor.python_task_executor_base.read_variable_or_file")
    def test_create_multiple_dags_creation(self, mock_read_variable):
        mock_read_variable.return_value = {
            "deployment_environment_name": "test-deployment-environment",
            "deploy_env_storage_suffix": "-test",
        }
        executor = PythonTaskExecutor("multiple_dags.yaml", task_executor_config_directory)
        dags = executor.create_dags()
        assert len(dags) == 2

    @patch("python_task_executor.python_task_executor_base.PythonOperator")
    @patch("python_task_executor.python_task_executor_base.EmptyOperator")
    @patch("python_task_executor.python_task_executor_base.read_variable_or_file")
    def test_tasks_callable_calls(self, mock_read_variable, mock_empty_operator, mock_python_operator):
        mock_read_variable.return_value = {
            "deployment_environment_name": None,
            "deploy_env_storage_suffix": "-test",
        }
        executor = PythonTaskExecutor("multiple_tasks.yaml", task_executor_config_directory)
        executor.dag = DAG(dag_id="dummy_dag", schedule=None, start_date=datetime(2023, 1, 1))
        executor.create_dags()

        module = import_module("util.miscutils")
        method = getattr(module, "replace_all_env_in_dict")
        method_pattern = getattr(module, "read_env_filepattern")

        expected_calls = [
            call(
                task_id="test_module_task_1",
                python_callable=method,
                op_kwargs={
                    "config": {"env": "pcb-{env}-curated"},
                    "deploy_env": "test-environment",
                },
                execution_timeout=ANY,
                dag=executor.dag,
            ),
            call(
                task_id="test_module_task_2",
                python_callable=method_pattern,
                op_kwargs={
                    "pattern": "filename_yyyymmdd.{file_env}",
                    "deploy_env": "test-environment",
                },
                execution_timeout=ANY,  # ✅ add this (implementation sets it for all tasks)
                dag=executor.dag,
            ),
        ]

        assert mock_empty_operator.call_count == 2
        mock_python_operator.assert_has_calls(expected_calls)
