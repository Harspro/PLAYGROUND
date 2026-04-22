import pytest
from airflow.exceptions import AirflowFailException
from unittest.mock import patch, MagicMock

from pcb_ops_processing.nonmon.account_closure_batch_file_generator import (
    Rt159AccountClosureFileGenerator,
)
import util.constants as consts


@pytest.fixture
def mock_gcp_config():
    return {
        consts.DEPLOYMENT_ENVIRONMENT_NAME: "dev",
        consts.LANDING_ZONE_PROJECT_ID: "test-landing-project",
        consts.PROCESSING_ZONE_PROJECT_ID: "test-processing-project",
    }


@pytest.fixture
def mock_config():
    return {
        "dag_id": "test_account_closure_dag",
        "default_args": {"owner": "test_owner", "retries": 1},
    }


class TestAccountClosureFileGenerator:
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file")
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env")
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings")
    def test_generate_nonmon_record_valid_input(
        self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config
    ):
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = "/test/dags"

        gen = Rt159AccountClosureFileGenerator("test_config.yaml")

        account_id = 12345
        original_row = [str(account_id), "AB", "159"]

        rec = gen.generate_nonmon_record(account_id, original_row)

        assert len(rec) == 596
        assert rec.startswith("IIIIII00000012345")
        assert "159" in rec
        assert "020702" in rec
        assert "AB" in rec

        # Static fields now used by the generator
        assert "272107" in rec     # FIELD_IND2 (positions 037–042)
        assert "2025164" in rec    # JULDATE   (positions 043–049)
        assert "272207" in rec     # FIELD_IND3 (positions 050–055)
        assert "2099001" in rec    # STOP_DATE  (positions 056–062)

    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file")
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env")
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings")
    def test_generate_nonmon_record_missing_reason_raises(
        self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config
    ):
        """If reason_code is blank/missing we must fail the DAG (per review)."""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = "/test/dags"

        gen = Rt159AccountClosureFileGenerator("test_config.yaml")

        account_id = 99999
        original_row = [str(account_id), "", "159"]  # blank reason_code

        with pytest.raises(AirflowFailException) as exc:
            gen.generate_nonmon_record(account_id, original_row)

        msg = str(exc.value)
        assert "Missing reason_code" in msg
        assert "Account ID: 99999" in msg

    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file")
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env")
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings")
    def test_generate_nonmon_record_missing_record_type_raises(
        self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config
    ):
        """If record_type is blank/missing we must fail the DAG."""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = "/test/dags"

        gen = Rt159AccountClosureFileGenerator("test_config.yaml")

        account_id = 88888
        original_row = [str(account_id), "AB", ""]  # blank record_type

        with pytest.raises(AirflowFailException) as exc:
            gen.generate_nonmon_record(account_id, original_row)

        msg = str(exc.value)
        assert "Missing record_type" in msg
        assert "Account ID: 88888" in msg

    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file")
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env")
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings")
    def test_build_nonmon_table(self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config):
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = "/test/dags"

        gen = Rt159AccountClosureFileGenerator("test_config.yaml")

        table_id = "test_project.test_dataset.account_closure"
        ddl = gen.build_nonmon_table(table_id)

        assert f"CREATE TABLE IF NOT EXISTS `{table_id}`" in ddl
        assert "FILLER_1 STRING" in ddl
        assert "MAST_ACCOUNT_ID STRING" in ddl
        assert "FILLER_2 STRING" in ddl
        assert "TSYSID STRING" in ddl
        assert "RECORD_TYPE STRING" in ddl
        assert "STATIC_CODE STRING" in ddl
        assert "REASON_CODE STRING" in ddl
        assert "FIELD_IND2 STRING" in ddl
        assert "JULDATE STRING" in ddl
        assert "FIELD_IND3 STRING" in ddl
        assert "STOP_DATE STRING" in ddl
        assert "REC_LOAD_TIMESTAMP DATETIME" in ddl
        assert "FILE_NAME STRING" in ddl

    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file")
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env")
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings")
    def test_build_nonmon_query_single_view(
        self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config
    ):
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = "/test/dags"

        gen = Rt159AccountClosureFileGenerator("test_config.yaml")

        transformed_views = [
            {"id": "test_project.test_dataset.account_closure_view", "columns": "record_data, file_name"}
        ]
        table_id = "test_project.test_dataset.account_closure"

        q = gen.build_nonmon_query(transformed_views, table_id)

        assert f"INSERT INTO {table_id}" in q
        assert "SUBSTR(record_data, 1, 6)" in q
        assert "SUBSTR(record_data, 35, 2)" in q   # REASON_CODE
        assert "SUBSTR(record_data, 37, 6)" in q   # FIELD_IND2
        assert "SUBSTR(record_data, 43, 7)" in q   # JULDATE
        assert "SUBSTR(record_data, 50, 6)" in q   # FIELD_IND3
        assert "SUBSTR(record_data, 56, 7)" in q   # STOP_DATE
        assert "file_name" in q
        assert "`test_project.test_dataset.account_closure_view`" in q

    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file")
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env")
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings")
    @patch("pcb_ops_processing.nonmon.account_closure_batch_file_generator.logging.getLogger")
    def test_build_nonmon_query_logs_debug_info(
        self,
        mock_get_logger,
        mock_settings,
        mock_read_yaml,
        mock_read_var,
        mock_gcp_config,
        mock_config,
    ):
        """Multi-line logs: one block for view info, one for final query."""
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = "/test/dags"
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        gen = Rt159AccountClosureFileGenerator("test_config.yaml")

        transformed_views = [
            {"id": "test_project.test_dataset.account_closure_view", "columns": "record_data, file_name"}
        ]
        table_id = "test_project.test_dataset.account_closure"

        gen.build_nonmon_query(transformed_views, table_id)

        # We log in two multi-line blocks (view details + final query)
        assert mock_logger.info.call_count >= 2

        msgs = [c.args[0] for c in mock_logger.info.call_args_list]
        assert any("Building query for view: test_project.test_dataset.account_closure_view" in m for m in msgs)
        assert any("record   :: record_data" in m for m in msgs)
        assert any("filename :: file_name" in m for m in msgs)
        assert any("Final combined query for account closure" in m for m in msgs)


class TestAccountClosureFileGeneratorEdgeCases:
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_variable_or_file")
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.read_yamlfile_env")
    @patch("pcb_ops_processing.nonmon.abc.base_nonmon_file_generator.settings")
    def test_build_nonmon_query_empty_views(
        self, mock_settings, mock_read_yaml, mock_read_var, mock_gcp_config, mock_config
    ):
        mock_read_var.return_value = mock_gcp_config
        mock_read_yaml.return_value = mock_config
        mock_settings.DAGS_FOLDER = "/test/dags"

        gen = Rt159AccountClosureFileGenerator("test_config.yaml")

        q = gen.build_nonmon_query([], "test_project.test_dataset.account_closure")

        assert "INSERT INTO test_project.test_dataset.account_closure" in q
        assert "UNION ALL" not in q
