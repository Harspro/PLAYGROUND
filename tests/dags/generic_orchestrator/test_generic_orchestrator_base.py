# Future imports
from __future__ import annotations

# Standard library imports
from datetime import datetime, timedelta
import os
from unittest.mock import patch

# Third-party imports
from airflow.utils.trigger_rule import TriggerRule
import pendulum
import pytest

# Local application imports
from generic_orchestrator.generic_orchestrator_base import GenericOrchestrator, INITIAL_DEFAULT_ARGS
import util.constants as consts
from util.miscutils import read_yamlfile_env

current_script_directory = os.path.dirname(os.path.abspath(__file__))
generic_orchestrator_config_directory = os.path.join(
    current_script_directory,
    "..",
    "config",
    "generic_orchestrator_configs"
)


class TestGenericOrchestrator:
    """
    This test suite covers the functionality of the `GenericOrchestrator` class.
    """

    @pytest.mark.parametrize(
        "config_file, valid_config, invalid_trigger_rule, invalid_default_args, sequential_multi_tasks",
        [
            ("test_config.yaml", True, False, False, False),
            ("test_empty_config.yaml", False, False, False, False),
            ("test_invalid_trigger_config.yaml", True, True, False, False),
            ("test_invalid_default_args.yaml", True, False, True, False),
            ("test_sequential_multi_tasks.yaml", True, False, False, True),
        ],
    )
    @patch("generic_orchestrator.generic_orchestrator_base.DAG")
    @patch("generic_orchestrator.generic_orchestrator_base.EmptyOperator")
    @patch("generic_orchestrator.generic_orchestrator_base.TaskGroup")
    @patch("generic_orchestrator.generic_orchestrator_base.TriggerDagRunOperator")
    @patch("generic_orchestrator.generic_orchestrator_base.read_yamlfile_env")
    @patch("generic_orchestrator.generic_orchestrator_base.read_variable_or_file")
    @patch("generic_orchestrator.generic_orchestrator_base.GenericOrchestrator._get_pause_setting")
    @patch("generic_orchestrator.generic_orchestrator_base.pause_unpause_dag")
    def test_generic_orchestrator_dag_creation(
        self,
        mock_pause_unpause_dag,
        mock_get_pause_setting,
        mock_read_variable_or_file,
        mock_read_yamlfile_env,
        mock_trigger,
        mock_task_group,
        mock_empty,
        mock_dag,
        config_file: str,
        valid_config: bool,
        invalid_trigger_rule: bool,
        invalid_default_args: bool,
        sequential_multi_tasks: bool,
    ):
        """Test DAG creation for the `GenericOrchestrator` class."""
        mock_get_pause_setting.return_value = False
        mock_pause_unpause_dag.return_value = None

        mock_read_variable_or_file.return_value = {
            consts.DEPLOYMENT_ENVIRONMENT_NAME: "test_env"
        }

        local_tz = pendulum.timezone("America/Toronto")
        mock_dag_instance = mock_dag.return_value
        mock_dag_instance.start_date = datetime(2025, 1, 1, tzinfo=local_tz)

        mock_read_yamlfile_env.return_value = read_yamlfile_env(
            f"{generic_orchestrator_config_directory}/{config_file}",
            "test_env",
        )

        if not valid_config:
            assert GenericOrchestrator(
                config_file,
                generic_orchestrator_config_directory,
            ).create_dags() == {}

        elif invalid_default_args:
            with pytest.raises(ValueError, match=r"Default args must be a dictionary"):
                GenericOrchestrator(
                    config_file,
                    generic_orchestrator_config_directory,
                ).create_dags()
        else:
            globals().update(
                GenericOrchestrator(
                    config_file,
                    generic_orchestrator_config_directory,
                ).create_dags()
            )

            mock_read_variable_or_file.assert_called_once_with(consts.GCP_CONFIG)
            mock_read_yamlfile_env.assert_called_once_with(
                f"{generic_orchestrator_config_directory}/{config_file}",
                "test_env",
            )

            assert mock_dag.call_count == 1
            dag_call_kwargs = mock_dag.call_args.kwargs
            assert dag_call_kwargs["dag_id"] == "dag1"
            assert dag_call_kwargs["start_date"] == datetime(2025, 1, 1, tzinfo=local_tz)
            assert dag_call_kwargs["catchup"] is False
            assert dag_call_kwargs["dagrun_timeout"] == timedelta(minutes=60)
            assert dag_call_kwargs["is_paused_upon_creation"] is True
            assert isinstance(dag_call_kwargs["default_args"], dict)

            # EmptyOperators
            assert mock_empty.call_count == 2
            mock_empty.assert_any_call(task_id=consts.START_TASK_ID, dag=mock_dag_instance)
            mock_empty.assert_any_call(task_id=consts.END_TASK_ID, dag=mock_dag_instance)

            # TaskGroups
            expected_task_group_count = 2 if not sequential_multi_tasks else 1
            assert mock_task_group.call_count == expected_task_group_count
            for call_args in mock_task_group.call_args_list:
                assert call_args.kwargs["dag"] == mock_dag_instance
                assert isinstance(call_args.kwargs["group_id"], str)

            # TriggerDagRunOperator calls
            expected_trigger_count = 3
            assert mock_trigger.call_count == expected_trigger_count
            trigger_task_ids = {call_args.kwargs["task_id"] for call_args in mock_trigger.call_args_list}
            expected_task_ids = {"trigger_sub_dag1", "trigger_sub_dag2", "trigger_sub_dag3"}
            assert trigger_task_ids == expected_task_ids

            for call_args in mock_trigger.call_args_list:
                assert call_args.kwargs["wait_for_completion"] is True
                assert call_args.kwargs["poke_interval"] == 60

                # ✅ Updated: implementation now sets execution_timeout to 1 hour (3600s)
                assert call_args.kwargs["execution_timeout"] == timedelta(seconds=3600)

                assert call_args.kwargs["trigger_rule"] == TriggerRule.ALL_SUCCESS

            if invalid_trigger_rule:
                for call_args in mock_trigger.call_args_list:
                    assert call_args.kwargs["trigger_rule"] == TriggerRule.ALL_SUCCESS

            if sequential_multi_tasks:
                assert mock_trigger.call_count == 3

    @patch("generic_orchestrator.generic_orchestrator_base.read_yamlfile_env")
    @patch("generic_orchestrator.generic_orchestrator_base.read_variable_or_file")
    def test_load_deploy_settings_caching(self, mock_read_variable_or_file, mock_read_yamlfile_env):
        """Test that _load_deploy_settings() only reads the deploy_config.yaml file once and caches."""
        mock_read_variable_or_file.return_value = {
            consts.DEPLOYMENT_ENVIRONMENT_NAME: "test_env"
        }

        deploy_config = {
            "dag1": {"test_env": False},
            "dag2": {"test_env": True},
            "default": {"test_env": True},
        }
        mock_read_yamlfile_env.return_value = deploy_config

        orchestrator = GenericOrchestrator(
            "test_config.yaml", generic_orchestrator_config_directory
        )

        initial_call_count = mock_read_yamlfile_env.call_count

        result1 = orchestrator._load_deploy_settings()
        assert result1 == deploy_config
        deploy_config_call_count = mock_read_yamlfile_env.call_count - initial_call_count
        assert deploy_config_call_count == 1

        result2 = orchestrator._load_deploy_settings()
        assert result2 == deploy_config
        deploy_config_call_count = mock_read_yamlfile_env.call_count - initial_call_count
        assert deploy_config_call_count == 1

        result3 = orchestrator._load_deploy_settings()
        assert result3 == deploy_config
        deploy_config_call_count = mock_read_yamlfile_env.call_count - initial_call_count
        assert deploy_config_call_count == 1

    @patch("generic_orchestrator.generic_orchestrator_base.read_yamlfile_env")
    @patch("generic_orchestrator.generic_orchestrator_base.read_variable_or_file")
    def test_get_pause_setting_with_dag_specific_config(self, mock_read_variable_or_file, mock_read_yamlfile_env):
        """Test that _get_pause_setting() correctly retrieves pause settings for a specific DAG."""
        mock_read_variable_or_file.return_value = {
            consts.DEPLOYMENT_ENVIRONMENT_NAME: "test_env"
        }

        deploy_config = {
            "dag1": {"test_env": False, "prod": True},
            "dag2": {"test_env": True, "prod": False},
            "default": {"test_env": True, "prod": True},
        }
        mock_read_yamlfile_env.return_value = deploy_config

        orchestrator = GenericOrchestrator(
            "test_config.yaml", generic_orchestrator_config_directory
        )

        initial_call_count = mock_read_yamlfile_env.call_count

        assert orchestrator._get_pause_setting("dag1") is False
        assert orchestrator._get_pause_setting("dag2") is True

        deploy_config_call_count = mock_read_yamlfile_env.call_count - initial_call_count
        assert deploy_config_call_count == 1

    @patch("generic_orchestrator.generic_orchestrator_base.read_yamlfile_env")
    @patch("generic_orchestrator.generic_orchestrator_base.read_variable_or_file")
    def test_get_pause_setting_fallback_to_default(self, mock_read_variable_or_file, mock_read_yamlfile_env):
        """Test that _get_pause_setting() falls back to 'default' when DAG name is not found."""
        mock_read_variable_or_file.return_value = {
            consts.DEPLOYMENT_ENVIRONMENT_NAME: "test_env"
        }

        deploy_config = {
            "dag1": {"test_env": False},
            "default": {"test_env": True, "prod": False},
        }
        mock_read_yamlfile_env.return_value = deploy_config

        orchestrator = GenericOrchestrator(
            "test_config.yaml", generic_orchestrator_config_directory
        )

        assert orchestrator._get_pause_setting("unknown_dag") is True

    @patch("generic_orchestrator.generic_orchestrator_base.DAG")
    @patch("generic_orchestrator.generic_orchestrator_base.EmptyOperator")
    @patch("generic_orchestrator.generic_orchestrator_base.TaskGroup")
    @patch("generic_orchestrator.generic_orchestrator_base.TriggerDagRunOperator")
    @patch("generic_orchestrator.generic_orchestrator_base.read_yamlfile_env")
    @patch("generic_orchestrator.generic_orchestrator_base.read_variable_or_file")
    @patch("generic_orchestrator.generic_orchestrator_base.pause_unpause_dag")
    @patch("generic_orchestrator.generic_orchestrator_base.add_tags")
    def test_create_dag_uses_cached_deploy_settings(
        self,
        mock_add_tags,
        mock_pause_unpause_dag,
        mock_read_variable_or_file,
        mock_read_yamlfile_env,
        mock_trigger,
        mock_task_group,
        mock_empty,
        mock_dag,
    ):
        """Test that create_dag() uses cached deploy settings and doesn't reread deploy_config.yaml."""
        mock_read_variable_or_file.return_value = {
            consts.DEPLOYMENT_ENVIRONMENT_NAME: "test_env"
        }

        deploy_config = {
            "dag1": {"test_env": False},
            "sub_dag1": {"test_env": True},
            "sub_dag2": {"test_env": False},
            "default": {"test_env": True},
        }

        job_config = {
            "dag1": {
                "dag": {
                    "read_pause_deploy_config": True,
                    "schedule_interval": None,
                    "description": "Test DAG",
                    "tags": ["test"],
                },
                "task_groups": [
                    {"group_id": "group1", "dags": ["sub_dag1", "sub_dag2"]}
                ],
            }
        }

        def read_yamlfile_env_side_effect(file_path, env):
            if "deploy_config.yaml" in str(file_path):
                return deploy_config
            return job_config

        mock_read_yamlfile_env.side_effect = read_yamlfile_env_side_effect

        local_tz = pendulum.timezone("America/Toronto")
        mock_dag_instance = mock_dag.return_value
        mock_dag_instance.start_date = datetime(2025, 1, 1, tzinfo=local_tz)
        mock_add_tags.return_value = mock_dag_instance

        orchestrator = GenericOrchestrator(
            "test_config.yaml", generic_orchestrator_config_directory
        )
        orchestrator.job_config = job_config

        dag = orchestrator.create_dag("dag1", job_config["dag1"])
        assert dag is mock_dag_instance

        pause_calls = mock_pause_unpause_dag.call_args_list
        assert len(pause_calls) == 3

        deploy_config_calls = [
            call_args
            for call_args in mock_read_yamlfile_env.call_args_list
            if call_args.args and "deploy_config.yaml" in str(call_args.args[0])
        ]
        assert len(deploy_config_calls) == 1

        job_config2 = {
            "dag2": {
                "dag": {
                    "read_pause_deploy_config": True,
                    "schedule_interval": None,
                    "description": "Second DAG",
                    "tags": ["test"],
                },
                "task_groups": [
                    {"group_id": "group1", "dags": ["sub_dag1", "sub_dag2"]}
                ],
            }
        }

        deploy_config_reads_before_second = len(
            [
                call_args
                for call_args in mock_read_yamlfile_env.call_args_list
                if call_args.args and "deploy_config.yaml" in str(call_args.args[0])
            ]
        )

        orchestrator.job_config = job_config2
        orchestrator.create_dag("dag2", job_config2["dag2"])

        deploy_config_reads_after_second = len(
            [
                call_args
                for call_args in mock_read_yamlfile_env.call_args_list
                if call_args.args and "deploy_config.yaml" in str(call_args.args[0])
            ]
        )

        assert deploy_config_reads_after_second == deploy_config_reads_before_second

    @patch("generic_orchestrator.generic_orchestrator_base.DAG")
    @patch("generic_orchestrator.generic_orchestrator_base.EmptyOperator")
    @patch("generic_orchestrator.generic_orchestrator_base.TaskGroup")
    @patch("generic_orchestrator.generic_orchestrator_base.TriggerDagRunOperator")
    @patch("generic_orchestrator.generic_orchestrator_base.read_yamlfile_env")
    @patch("generic_orchestrator.generic_orchestrator_base.read_variable_or_file")
    @patch("generic_orchestrator.generic_orchestrator_base.pause_unpause_dag")
    @patch("generic_orchestrator.generic_orchestrator_base.add_tags")
    def test_mocked_stress_test_large_configuration(
        self,
        mock_add_tags,
        mock_pause_unpause_dag,
        mock_read_variable_or_file,
        mock_read_yamlfile_env,
        mock_trigger,
        mock_task_group,
        mock_empty,
        mock_dag,
    ):
        """Mocked stress test: Tests the orchestrator with a large, complex configuration."""
        mock_read_variable_or_file.return_value = {
            consts.DEPLOYMENT_ENVIRONMENT_NAME: "test_env"
        }

        deploy_config = {"default": {"test_env": True}}
        for i in range(1, 101):
            deploy_config[f"sub_dag_{i}"] = {"test_env": i % 2 == 0}

        task_groups = []
        for group_num in range(1, 11):
            sub_dags = [
                f"sub_dag_{i}"
                for i in range((group_num - 1) * 10 + 1, group_num * 10 + 1)
            ]
            task_groups.append(
                {
                    "group_id": f"group_{group_num}",
                    "execution_mode": "parallel",
                    "dags": sub_dags,
                }
            )

        job_config = {
            "stress_test_dag": {
                "dag": {
                    "read_pause_deploy_config": True,
                    "schedule_interval": None,
                    "description": "Stress Test DAG with 50 sub-DAGs",
                    "tags": ["test", "stress"],
                    "dagrun_timeout": 60,
                },
                "task_groups": task_groups,
            }
        }

        def read_yamlfile_env_side_effect(file_path, env):
            if "deploy_config.yaml" in str(file_path):
                return deploy_config
            return job_config

        mock_read_yamlfile_env.side_effect = read_yamlfile_env_side_effect

        local_tz = pendulum.timezone("America/Toronto")
        mock_dag_instance = mock_dag.return_value
        mock_dag_instance.start_date = datetime(2025, 1, 1, tzinfo=local_tz)
        mock_add_tags.return_value = mock_dag_instance
        mock_pause_unpause_dag.return_value = None

        orchestrator = GenericOrchestrator(
            "test_config.yaml", generic_orchestrator_config_directory
        )
        orchestrator.job_config = job_config

        initial_deploy_config_calls = len(
            [
                call_args
                for call_args in mock_read_yamlfile_env.call_args_list
                if call_args.args and "deploy_config.yaml" in str(call_args.args[0])
            ]
        )

        dag = orchestrator.create_dag("stress_test_dag", job_config["stress_test_dag"])

        deploy_config_calls = [
            call_args
            for call_args in mock_read_yamlfile_env.call_args_list
            if call_args.args and "deploy_config.yaml" in str(call_args.args[0])
        ]
        deploy_config_reads = len(deploy_config_calls) - initial_deploy_config_calls

        assert deploy_config_reads == 1
        assert mock_pause_unpause_dag.call_count == 101
        assert mock_trigger.call_count == 100
        assert mock_task_group.call_count == 10
        assert dag is mock_dag_instance
