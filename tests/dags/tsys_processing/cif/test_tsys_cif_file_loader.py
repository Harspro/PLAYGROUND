import os
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

import pytest
from airflow.exceptions import AirflowException

import util.constants as consts
from tsys_processing.cif.constants import CIF_CONFIG_FILE_PATH, CIF_CONFIG_FILE_NAME, ACCOUNT_SEGMENT, CARD_SEGMENT, \
    CIF_SEGMENT_PREV_SQL_PATH
from tsys_processing.cif.tsys_cif_file_loader import TsysCIFFileLoader
from util.miscutils import sanitize_string, read_file_env, read_yamlfile_env


@pytest.fixture
def airflow_env_fixture() -> dict:
    """
    Pytest fixture providing test data for Airflow environment variables
    :returns: A dictionary containing test data for Airflow environment variables
    :rtype: dict
    """
    return {
        'deployment_environment_name': 'test-deployment-environment',
        'network_tag': 'test-network-tag',
        'processing_zone_connection_id': 'google_cloud_default',
        'location': 'northamerica-northeast1',
        'project_id': 'test-project'
    }


@pytest.fixture
def cif_account_transformation_config_fixture() -> dict:
    """
    Pytest fixture providing test data for CIF account transformation config
    :returns: A dictionary containing test data for CIF account transformation config
    :rtype: dict
    """
    return {
        consts.EXTERNAL_TABLE_ID: 'test-processing-project.test_domain.CIF_ACCOUNT_EXT',
        consts.DATA_FILE_LOCATION: 'gs://test-bucket/cif_account_parquets/',
        consts.ADD_COLUMNS: ["CURRENT_DATETIME('America/Toronto')  AS REC_LOAD_TIMESTAMP"],
        consts.DROP_COLUMNS: ['CIFP_RECORD_ID', 'CIFP_PROD_CHNG_IN_PROG'],
        consts.RANK_PARTITION: 'CIFP_ACCOUNT_ID5',
        consts.RANK_ORDER_BY: 'REC_LOAD_TIMESTAMP DESC, CIFP_ACTION_CODE DESC',
        consts.JOIN_SPECIFICATION: [],
        consts.DESTINATION_TABLE: {
            consts.ID: 'test-curated-project.test_domain.CIF_ACCOUNT_20250312',
            consts.PARTITION: 'DATE_TRUNC(FILE_CREATE_DT, MONTH)',
            consts.CLUSTERING: ['CIFP_ACCOUNT_ID5', 'CIFP_CLIENT_PRODUCT_CODE']
        }
    }


@pytest.fixture
def cif_card_transformation_config_fixture() -> dict:
    """
    Pytest fixture providing test data for CIF card transformation config
    :returns: A dictionary containing test data for CIF card transformation config
    :rtype: dict
    """
    return {
        consts.EXTERNAL_TABLE_ID: 'test-processing-project.test_domain.CIF_CARD_EXT',
        consts.DATA_FILE_LOCATION: 'gs://test-bucket/cif_card_parquets/',
        consts.ADD_COLUMNS: ["CURRENT_DATETIME('America/Toronto')  AS REC_LOAD_TIMESTAMP"],
        consts.DROP_COLUMNS: ['CIFP_RECORD_ID'],
        consts.RANK_PARTITION: 'CIFP_ACCOUNT_NUM, CIFP_ACCOUNT_ID6, CIFP_CUSTOMER_ID_2',
        consts.RANK_ORDER_BY: 'REC_LOAD_TIMESTAMP DESC',
        consts.JOIN_SPECIFICATION: [],
        consts.DESTINATION_TABLE: {
            consts.ID: 'test-curated-project.test_domain.CIF_CARD_20250312',
            consts.PARTITION: 'DATE_TRUNC(FILE_CREATE_DT, MONTH)',
            consts.CLUSTERING: ['CIFP_ACCOUNT_ID6', 'CIFP_RELATIONSHIP_STAT']
        }
    }


@pytest.fixture
def cif_trlr_transformation_config_fixture() -> dict:
    """
    Pytest fixture providing test data for CIF card transformation config
    :returns: A dictionary containing test data for CIF card transformation config
    :rtype: dict
    """
    return {
        consts.EXTERNAL_TABLE_ID: 'test-processing-project.test_domain.CIF_ACCOUNT_TRAIL_EXT',
        consts.DATA_FILE_LOCATION: 'gs://test-bucket/cif_card_parquets/',
        consts.ADD_COLUMNS: ["CURRENT_DATETIME('America/Toronto')  AS REC_LOAD_TIMESTAMP"],
        consts.DROP_COLUMNS: [],
        consts.JOIN_SPECIFICATION: [],
        consts.DESTINATION_TABLE: {
            consts.ID: 'test-landing-project.test_domain.CIF_ACCOUNT_TRAIL'
        }
    }


def setup_transformation_mocks(mock_create_external_table, mock_apply_column_transformation, mock_apply_timestamp_transformation,
                               mock_table_exists,
                               mock_submit_transformation, mock_apply_schema_sync_transformation, segment_name):

    mock_create_external_table.return_value = {
        consts.ID: f'test-processing-project.test_domain.CIF_{segment_name}_EXT',
        consts.COLUMNS: f'CIF_{segment_name}_COL1,CIF_{segment_name}_COL2,CIF_{segment_name}_COL3'
    }

    mock_apply_column_transformation.return_value = {
        consts.ID: f'test-processing-project.test_domain.CIF_{segment_name}_COLUMN_TF',
        consts.COLUMNS: f'CIF_{segment_name}_COL1,CIF_{segment_name}_COL2,CIF_{segment_name}_COL3'
    }
    mock_apply_timestamp_transformation.return_value = {
        consts.ID: f'test-processing-project.test_domain.CIF_{segment_name}_COLUMN_TF_TIMESTAMP_TF',
        consts.COLUMNS: f'CIF_{segment_name}_COL1,CIF_{segment_name}_COL2,CIF_{segment_name}_COL3'
    }
    mock_table_exists.return_value = True
    mock_submit_transformation.return_value = {
        consts.ID: f'test-processing-project.test_domain.CIF_{segment_name}_DUMMY',
        consts.COLUMNS: f'CIF_{segment_name}_COL1,CIF_{segment_name}_COL2,CIF_{segment_name}_COL3'
    }
    mock_apply_schema_sync_transformation.return_value = {
        consts.ID: f'test-processing-project.test_domain.CIF_{segment_name}_COLUMN_TF_TIMESTAMP_TF_SYNC_TF',
        consts.COLUMNS: f'CIF_{segment_name}_COL1,CIF_{segment_name}_COL2,CIF_{segment_name}_COL3'
    }


@pytest.mark.parametrize("segment_name",
                         [
                             "CARD",
                             "ACCOUNT",
                             "TRLR"
                         ])
@patch("tsys_processing.cif.tsys_cif_file_loader.create_external_table")
@patch("tsys_processing.cif.tsys_cif_file_loader.apply_column_transformation")
@patch("tsys_processing.cif.tsys_cif_file_loader.apply_timestamp_transformation")
@patch("tsys_processing.cif.tsys_cif_file_loader.submit_transformation")
@patch("tsys_processing.cif.tsys_cif_file_loader.apply_schema_sync_transformation")
@patch("tsys_processing.cif.tsys_cif_file_loader.table_exists")
def test_apply_transformations(mock_table_exists,
                               mock_apply_schema_sync_transformation,
                               mock_submit_transformation,
                               mock_apply_timestamp_transformation,
                               mock_apply_column_transformation,
                               mock_create_external_table,
                               segment_name: str,
                               cif_account_transformation_config_fixture,
                               cif_card_transformation_config_fixture,
                               cif_trlr_transformation_config_fixture):
    setup_transformation_mocks(mock_create_external_table, mock_apply_column_transformation, mock_apply_timestamp_transformation,
                               mock_table_exists,
                               mock_submit_transformation, mock_apply_schema_sync_transformation, segment_name)

    cif_file_loader = TsysCIFFileLoader('dummy.yaml')
    bigquery_client = MagicMock()
    mock_bq_table = MagicMock()
    mock_bq_cards_cnt = MagicMock()
    mock_bq_cards_cnt.name = 'AM00_BLOCKED_CARDS_CNT'
    mock_bq_bkrpt_type = MagicMock()
    mock_bq_bkrpt_type.name = 'AM00_BANKRUPTCY_TYPE'

    bigquery_client.get_table.return_value = mock_bq_table
    mock_bq_table.schema = [mock_bq_cards_cnt, mock_bq_bkrpt_type]

    if segment_name == ACCOUNT_SEGMENT:
        cif_file_loader.apply_transformations(bigquery_client, cif_account_transformation_config_fixture, segment_name)
        mock_apply_column_transformation.assert_called_once()
        mock_apply_timestamp_transformation.assert_called_once()
        mock_submit_transformation.assert_called()
        mock_table_exists.assert_called()

        assert mock_table_exists.call_count == 2
        assert mock_submit_transformation.call_count == 1
    elif segment_name == CARD_SEGMENT:
        cif_file_loader.apply_transformations(bigquery_client, cif_card_transformation_config_fixture, segment_name)
        mock_apply_column_transformation.assert_called_once()
        mock_apply_timestamp_transformation.assert_called_once()
        mock_submit_transformation.assert_called()
        mock_table_exists.assert_called()

        assert mock_table_exists.call_count == 2
        assert mock_submit_transformation.call_count == 3
    elif segment_name == 'TRLR':
        cif_file_loader.apply_transformations(bigquery_client, cif_trlr_transformation_config_fixture, segment_name)
        mock_apply_column_transformation.assert_called_once()
        mock_apply_timestamp_transformation.assert_called_once()
        mock_submit_transformation.assert_not_called()

        assert mock_table_exists.call_count == 1
    else:
        raise Exception(f'Unknown segment_name: {segment_name}')


@pytest.mark.parametrize("segment_name",
                         [
                             "CARD",
                             "ACCOUNT"
                         ])
@patch("tsys_processing.tsys_file_loader_base.read_variable_or_file")
@patch("tsys_processing.tsys_file_loader_base.read_yamlfile_env")
@patch("tsys_processing.cif.tsys_cif_file_loader.submit_transformation")
def test_apply_merge_transformation(mock_submit_transformation,
                                    mock_read_yamlfile_env,
                                    mock_read_variable_or_file,
                                    segment_name: str,
                                    airflow_env_fixture: dict):

    mock_read_variable_or_file.return_value = airflow_env_fixture

    cif_file_loader = TsysCIFFileLoader('dummy.yaml')
    source_table = {'id': 'test_table_id', 'columns': 'test_column'}
    rank_partition = 'test_rank_partition'
    rank_order_by = 'test_order_by'

    cif_file_loader.apply_merge_transformation(bigquery_client=None,
                                               source_table=source_table,
                                               target_table_id='test_target_table_id',
                                               rank_partition=rank_partition,
                                               rank_order_by=rank_order_by,
                                               segment_name=segment_name)

    args, kwargs = mock_submit_transformation.call_args

    transform_view_ddl = sanitize_string(args[2])

    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_directory = os.path.join(current_dir, 'sql')
    expected_cif_account_sql = read_file_env(os.path.join(sql_directory, 'expected_cif_account_sql.sql'))
    expected_cif_card_sql = read_file_env(os.path.join(sql_directory, 'expected_cif_card_sql.sql'))

    if CARD_SEGMENT in segment_name:
        expected = sanitize_string(expected_cif_card_sql)
    elif ACCOUNT_SEGMENT in segment_name:
        expected = sanitize_string(expected_cif_account_sql)
    else:
        raise Exception(f'Unknown segment_name: {segment_name}')

    assert transform_view_ddl == expected


@pytest.mark.parametrize("segment_name",
                         [
                             "CARD",
                             "ACCOUNT",
                             "TRLR"
                         ])
@patch("google.cloud.bigquery.Client")
@patch("tsys_processing.cif.tsys_cif_file_loader.create_external_table")
@patch("tsys_processing.cif.tsys_cif_file_loader.apply_column_transformation")
@patch("tsys_processing.cif.tsys_cif_file_loader.apply_timestamp_transformation")
@patch("tsys_processing.cif.tsys_cif_file_loader.submit_transformation")
@patch("tsys_processing.cif.tsys_cif_file_loader.apply_schema_sync_transformation")
@patch("tsys_processing.cif.tsys_cif_file_loader.table_exists")
@patch("tsys_processing.cif.tsys_cif_file_loader.TsysCIFFileLoader.apply_transformations")
@patch("tsys_processing.cif.tsys_cif_file_loader.TsysCIFFileLoader.build_file_validation")
def test_build_segment_loading_job(mock_build_file_validation,
                                   mock_apply_transformations,
                                   mock_table_exists,
                                   mock_apply_schema_sync_transformation,
                                   mock_submit_transformation,
                                   mock_apply_timestamp_transformation,
                                   mock_apply_column_transformation,
                                   mock_create_external_table,
                                   mock_bigquery_client,
                                   segment_name: str,
                                   cif_account_transformation_config_fixture,
                                   cif_card_transformation_config_fixture,
                                   cif_trlr_transformation_config_fixture):
    setup_transformation_mocks(mock_create_external_table, mock_apply_column_transformation, mock_apply_timestamp_transformation,
                               mock_table_exists,
                               mock_submit_transformation, mock_apply_schema_sync_transformation, segment_name)

    # Mock the apply_transformations method to return the expected data structure
    mock_apply_transformations.return_value = {
        consts.ID: f'test-processing-project.test_domain.CIF_{segment_name}_TRANSFORMED',
        consts.COLUMNS: f'CIF_{segment_name}_COL1,CIF_{segment_name}_COL2,CIF_{segment_name}_COL3'
    }

    # Mock the build_file_validation method to return True
    mock_build_file_validation.return_value = True

    # Mock table_exists to return True for PREV table checks (for CARD/ACCOUNT segments)
    # and for destination table checks (for TRLR segment)
    def table_exists_side_effect(client, table_id):
        if 'PREV' in table_id or segment_name == 'TRLR':
            return True
        return False

    mock_table_exists.side_effect = table_exists_side_effect

    cif_file_loader = TsysCIFFileLoader('dummy.yaml')

    bigquery_client = MagicMock()
    mock_bigquery_client.return_value = bigquery_client

    # Mock the context parameter that build_segment_loading_job expects
    mock_dag_run = MagicMock()
    mock_dag_run.conf = MagicMock()
    mock_dag_run.conf.get.side_effect = lambda key, default=None: {
        'file_name': 'test_file.txt',
        'force_reload': False
    }.get(key, default)

    mock_context = {
        'dag_run': mock_dag_run
    }

    if segment_name == CARD_SEGMENT:
        cif_file_loader.build_segment_loading_job(cif_card_transformation_config_fixture, segment_name, **mock_context)
        # For ACCOUNT/CARD segments, query_and_wait is called twice: once for CREATE TABLE LIKE, once for INSERT
        assert bigquery_client.query_and_wait.call_count == 2

        # First call: CREATE TABLE IF NOT EXISTS ... LIKE ...
        first_call_sql = bigquery_client.query_and_wait.call_args_list[0].args[0]
        assert 'CREATE TABLE IF NOT EXISTS' in first_call_sql
        assert 'LIKE' in first_call_sql

        # Second call: INSERT INTO ...
        second_call_sql = bigquery_client.query_and_wait.call_args_list[1].args[0]
        assert 'INSERT INTO' in second_call_sql
    elif segment_name == ACCOUNT_SEGMENT:
        cif_file_loader.build_segment_loading_job(cif_account_transformation_config_fixture, segment_name, **mock_context)
        # For ACCOUNT/CARD segments, query_and_wait is called twice: once for CREATE TABLE LIKE, once for INSERT
        assert bigquery_client.query_and_wait.call_count == 2

        # First call: CREATE TABLE IF NOT EXISTS ... LIKE ...
        first_call_sql = bigquery_client.query_and_wait.call_args_list[0].args[0]
        assert 'CREATE TABLE IF NOT EXISTS' in first_call_sql
        assert 'LIKE' in first_call_sql

        # Second call: INSERT INTO ...
        second_call_sql = bigquery_client.query_and_wait.call_args_list[1].args[0]
        assert 'INSERT INTO' in second_call_sql

    elif segment_name == 'TRLR':
        cif_file_loader.build_segment_loading_job(cif_trlr_transformation_config_fixture, segment_name, **mock_context)
        bigquery_client.query_and_wait.assert_called_once()

        loading_sql = bigquery_client.query_and_wait.call_args.args[0]
        assert 'CREATE TABLE IF NOT EXISTS' in loading_sql
        assert 'INSERT INTO' in loading_sql
    else:
        raise Exception(f'Unknown segment_name: {segment_name}')


def assert_common_transform_config(transformation_config: dict):
    assert transformation_config.get(consts.EXTERNAL_TABLE_ID)
    assert 'gs://test-bucket/test-folder/' in transformation_config.get(consts.DATA_FILE_LOCATION)
    assert "CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP" in transformation_config.get(consts.ADD_COLUMNS)
    assert transformation_config.get(consts.DESTINATION_TABLE)


@pytest.fixture
def cif_config_fixture():
    return read_yamlfile_env(f'{CIF_CONFIG_FILE_PATH}/{CIF_CONFIG_FILE_NAME}')


@pytest.mark.parametrize("dag_id",
                         [
                             "tsys_pcb_mc_cif_file_loading",
                             "tsys_pcb_mc_cif_weekly_file_loading"
                         ])
def test_build_transformation_config(cif_config_fixture: dict, dag_id: str):
    assert dag_id in cif_config_fixture

    cif_file_loader = TsysCIFFileLoader('dummy.yaml')
    segments = [ACCOUNT_SEGMENT, CARD_SEGMENT, "TRLR"]
    output_dir = 'gs://test-bucket/test-folder/'

    dag_config = cif_config_fixture.get(dag_id)

    assert dag_config

    bigquery_config = dag_config.get(consts.BIGQUERY)
    for segment_name in segments:
        transformation_config = cif_file_loader.build_transformation_config(bigquery_config, output_dir, segment_name)

        assert_common_transform_config(transformation_config)

        if segment_name == ACCOUNT_SEGMENT:
            assert transformation_config.get(consts.DROP_COLUMNS)
            assert 'CIFP_ACCOUNT_ID5, CIFP_SUFFIX_NUMBER' == transformation_config.get(consts.RANK_PARTITION)
            if 'weekly' in dag_id:
                assert ('REC_LOAD_TIMESTAMP DESC, CIFP_ACTION_CODE_SORTING_KEY ASC'
                        == transformation_config.get(consts.RANK_ORDER_BY))
            else:
                assert ('REC_LOAD_TIMESTAMP DESC, CIFP_ACTION_CODE_SORTING_KEY ASC'
                        == transformation_config.get(consts.RANK_ORDER_BY))
        elif segment_name == CARD_SEGMENT:
            assert transformation_config.get(consts.DROP_COLUMNS)
            assert ('CIFP_ACCOUNT_ID6, CIFP_APPLICATION_SUF, CIFP_ACCOUNT_NUM, CIFP_CUSTOMER_ID_2'
                    == transformation_config.get(consts.RANK_PARTITION))
            if 'weekly' in dag_id:
                assert 'REC_LOAD_TIMESTAMP DESC' == transformation_config.get(consts.RANK_ORDER_BY)
            else:
                assert 'REC_LOAD_TIMESTAMP DESC' == transformation_config.get(consts.RANK_ORDER_BY)
            assert transformation_config.get(consts.DESTINATION_TABLE)
        elif segment_name == 'TRLR':
            assert 'landing' in transformation_config.get(consts.DESTINATION_TABLE).get(consts.ID)
        else:
            raise Exception(f'Unknown segment_name: {segment_name}')


@patch("google.cloud.bigquery.Client")
def test_back_up_table(mock_bigquery_client):
    bigquery_client = MagicMock()
    bigquery_job = MagicMock()
    mock_bigquery_client.return_value = bigquery_client
    bigquery_client.query.return_value = bigquery_job
    bigquery_job.result.return_value = None

    cif_file_loader = TsysCIFFileLoader('dummy.yaml')

    cif_file_loader.back_up_table('test_table_id', 'teset_backup_table_id')
    cif_file_loader.back_up_table('test_table_id', 'teset_backup_table_id', 'col1, col2')

    query_call_args = bigquery_client.query.call_args_list

    assert len(query_call_args) == 2
    assert 'test_table_id' in query_call_args[0].args[0]
    assert 'test_table_id' in query_call_args[1].args[0]
    assert '*' in query_call_args[0].args[0]
    assert '*' in query_call_args[1].args[0]
    assert 'col1, col2' in query_call_args[1].args[0]


@patch("google.cloud.bigquery.Client")
def test_build_drop_segment_prev_table(mock_bigquery_client, cif_account_transformation_config_fixture):
    bigquery_client = MagicMock()
    mock_bigquery_client.return_value = bigquery_client
    bigquery_client.delete_table.return_value = None

    cif_file_loader = TsysCIFFileLoader('dummy.yaml')
    prev_table_qualified_name = f'test-curated-project.test_domain.CIF_{ACCOUNT_SEGMENT}_PREV'
    cif_file_loader.drop_segment_prev_table(ACCOUNT_SEGMENT, cif_account_transformation_config_fixture)

    bigquery_client.delete_table.assert_called_with(prev_table_qualified_name, not_found_ok=True)


def test_build_segment_prev_to_history(cif_account_transformation_config_fixture):
    cif_file_loader = TsysCIFFileLoader('dummy.yaml')
    with patch.object(cif_file_loader, 'back_up_table') as mock_back_up_table:
        cif_file_loader.append_segment_prev_to_history(ACCOUNT_SEGMENT, cif_account_transformation_config_fixture)
        mock_back_up_table.assert_called_once_with(
            f'test-curated-project.test_domain.CIF_{ACCOUNT_SEGMENT}_PREV',
            f'test-curated-project.test_domain.CIF_{ACCOUNT_SEGMENT}_HISTORY',
            'CURRENT_DATETIME("America/Toronto") AS insert_time',
            partitioning_field='DATETIME_TRUNC(insert_time, DAY)'
        )


@pytest.fixture
def transformation_config_fixture() -> dict:
    return {
        consts.EXTERNAL_TABLE_ID: 'test-processing-project.test_domain.test_table',
        consts.DATA_FILE_LOCATION: 'gs://test-bucket/test-folder/cif_weekly_data/',
    }


@pytest.fixture
def gcp_config_fixture() -> dict:
    return {
        consts.LANDING_ZONE_PROJECT_ID: 'test-landing-project',
        consts.CURATED_ZONE_PROJECT_ID: 'test-curated-project',
    }


@pytest.fixture
def dag_config_fixture() -> dict:
    return {
        consts.SPARK: {
            consts.PARSING_JOB_ARGS: {},
            consts.SEGMENT_ARGS: {
                ACCOUNT_SEGMENT: {},
                CARD_SEGMENT: {},
            }
        },
        consts.BIGQUERY: {},
        consts.SCHEDULE_INTERVAL: '@daily',
        consts.DATAPROC_JOB_SIZE: 'large',
    }


def test_back_up_raw_data(transformation_config_fixture, gcp_config_fixture):
    cif_file_loader = TsysCIFFileLoader('dummy.yaml')
    cif_file_loader.gcp_config = gcp_config_fixture

    with patch.object(cif_file_loader, 'back_up_table') as mock_back_up_table:
        cif_file_loader.back_up_raw_data(ACCOUNT_SEGMENT, transformation_config_fixture)

        mock_back_up_table.assert_called_once_with(
            'test-processing-project.test_domain.test_table_COLUMN_TF',
            f'test-landing-project.test_domain.CIF_{ACCOUNT_SEGMENT}_RAW_DATA',
            "'weekly' AS FILE_TYPE"
        )


@patch("google.cloud.bigquery.Client")
def test_build_segment_curr_to_prev(
        mock_bigquery_client,
        cif_account_transformation_config_fixture,
):
    bigquery_client = MagicMock()
    bigquery_job = MagicMock()
    mock_bigquery_client.return_value = bigquery_client
    bigquery_client.query.return_value = bigquery_job
    bigquery_job.result.return_value = None

    cif_file_loader = TsysCIFFileLoader('dummy.yaml')
    cif_file_loader.rename_segment_curr_to_prev(ACCOUNT_SEGMENT, cif_account_transformation_config_fixture)

    query_call_args = bigquery_client.query.call_args_list

    assert len(query_call_args) == 1
    test_sql_str = read_file_env(CIF_SEGMENT_PREV_SQL_PATH).format(
        from_table_ref='test-curated-project.test_domain.CIF_ACCOUNT_20250312',
        to_table_id='CIF_ACCOUNT_PREV'
    )
    assert test_sql_str == query_call_args[0].args[0]


@patch("tsys_processing.cif.tsys_cif_file_loader.DAG")
@pytest.mark.parametrize("dag_id",
                         [
                             "tsys_pcb_mc_cif_file_loading",
                             "tsys_pcb_mc_cif_weekly_file_loading"
                         ])
def test_create_dag(mock_dag_class, cif_config_fixture: dict, dag_id: str):
    assert dag_id in cif_config_fixture

    dag_config = cif_config_fixture.get(dag_id)
    cif_file_loader = TsysCIFFileLoader('dummy.yaml')
    cif_file_loader.default_args = {'owner': 'test_owner'}

    mock_dag_instance = MagicMock()
    mock_dag_class.return_value = mock_dag_instance

    with patch.object(cif_file_loader, 'build_segment_task_group') as mock_build_segment_task_group:
        try:
            cif_file_loader.create_dag(dag_id, dag_config)
        except AirflowException:
            mock_dag_class.assert_called_once_with(
                dag_id=dag_id,
                default_args={'owner': 'test_owner'},
                schedule=dag_config.get(consts.SCHEDULE_INTERVAL),
                start_date=datetime(2023, 1, 1, tzinfo=cif_file_loader.local_tz),
                catchup=False,
                dagrun_timeout=timedelta(hours=5),
                max_active_runs=1,
                is_paused_upon_creation=True,
                render_template_as_native_obj=True
            )
            assert mock_build_segment_task_group.call_count == 3
        else:
            raise Exception("[ERROR] Unexpected Exception encountered! Please check test case.")


@patch("google.cloud.bigquery.Client")
def test_build_file_validation_success_newer_file(mock_bigquery_client):
    """Test successful file validation when incoming file is newer"""
    # Arrange
    cif_file_loader = TsysCIFFileLoader('dummy.yaml')
    file_name = "tsys_pcb_cif_weekly_20250531020750.uatv"  # Weekly file
    transformation_config = {
        consts.DESTINATION_TABLE: {
            consts.ID: "pcb-dev-landing.domain_account_management.CIF_ACCOUNT_TRAIL"
        }
    }
    transformed_view = {
        consts.ID: "pcb-dev-processing.domain_account_management.CIF_ACCOUNT_TRAIL_DBEXT_COLUMN_TF_TIMESTAMP_TF_SCHEMA_SYNC_TF"
    }

    current_file_dt = datetime(2025, 5, 31, 2, 7, 50)  # Newer than max
    max_file_dt = datetime(2025, 5, 20, 10, 0, 0)

    # Mock pandas DataFrame-like objects
    mock_current_series = MagicMock()
    mock_current_series.values = [current_file_dt]

    mock_max_series = MagicMock()
    mock_max_series.values = [max_file_dt]

    mock_query_result = MagicMock()
    mock_query_result.to_dataframe.return_value = {'FILE_CREATE_DT': mock_current_series}

    mock_max_result = MagicMock()
    mock_max_result.to_dataframe.return_value = {'max_file_create_dt': mock_max_series}

    mock_query_job = MagicMock()
    mock_query_job.result.side_effect = [mock_query_result, mock_max_result]

    bigquery_client = MagicMock()
    bigquery_client.query.return_value = mock_query_job
    mock_bigquery_client.return_value = bigquery_client

    # Act
    result = cif_file_loader.build_file_validation(
        file_name, transformation_config, transformed_view
    )

    # Assert
    assert result is True
    assert bigquery_client.query.call_count == 2

    # Verify the queries were called with correct SQL
    first_call_args = bigquery_client.query.call_args_list[0][0][0]
    assert "SELECT FILE_CREATE_DT FROM `pcb-dev-processing.domain_account_management.CIF_ACCOUNT_TRAIL_DBEXT_COLUMN_TF_TIMESTAMP_TF_SCHEMA_SYNC_TF`" in first_call_args

    second_call_args = bigquery_client.query.call_args_list[1][0][0]
    assert "SELECT MAX(FILE_CREATE_DT) as max_file_create_dt" in second_call_args
    assert "FROM `pcb-dev-landing.domain_account_management.CIF_ACCOUNT_TRAIL`" in second_call_args


@patch("google.cloud.bigquery.Client")
def test_build_file_validation_failure_older_file(mock_bigquery_client):
    """Test file validation failure when incoming file is older"""
    # Arrange
    cif_file_loader = TsysCIFFileLoader('dummy.yaml')
    file_name = "tsys_pcb_mc_cif_daily_20250603003101.uatv"  # Daily file
    transformation_config = {
        consts.DESTINATION_TABLE: {
            consts.ID: "pcb-dev-landing.domain_account_management.CIF_ACCOUNT_TRAIL"
        }
    }
    transformed_view = {
        consts.ID: "pcb-dev-processing.domain_account_management.CIF_ACCOUNT_TRAIL_DBEXT_COLUMN_TF_TIMESTAMP_TF_SCHEMA_SYNC_TF"
    }

    current_file_dt = datetime(2025, 6, 3, 0, 31, 1)  # Older than max
    max_file_dt = datetime(2025, 6, 10, 10, 0, 0)

    # Mock pandas DataFrame-like objects
    mock_current_series = MagicMock()
    mock_current_series.values = [current_file_dt]

    mock_max_series = MagicMock()
    mock_max_series.values = [max_file_dt]

    mock_query_result = MagicMock()
    mock_query_result.to_dataframe.return_value = {'FILE_CREATE_DT': mock_current_series}

    mock_max_result = MagicMock()
    mock_max_result.to_dataframe.return_value = {'max_file_create_dt': mock_max_series}

    mock_query_job = MagicMock()
    mock_query_job.result.side_effect = [mock_query_result, mock_max_result]

    bigquery_client = MagicMock()
    bigquery_client.query.return_value = mock_query_job
    mock_bigquery_client.return_value = bigquery_client

    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        cif_file_loader.build_file_validation(
            file_name, transformation_config, transformed_view
        )

    # Verify the error message contains the expected information
    error_message = str(exc_info.value)
    assert "DAG FAILED" in error_message
    assert str(current_file_dt) in error_message
    assert str(max_file_dt) in error_message
    assert file_name in error_message
    assert "set \"force_reload\": true" in error_message


@patch("google.cloud.bigquery.Client")
def test_build_file_validation_success_same_date(mock_bigquery_client):
    """Test successful file validation when incoming file has same date as max"""
    # Arrange
    cif_file_loader = TsysCIFFileLoader('dummy.yaml')
    file_name = "tsys_pcb_cif_weekly_20250531020750.uatv"  # Weekly file
    transformation_config = {
        consts.DESTINATION_TABLE: {
            consts.ID: "pcb-dev-landing.domain_account_management.CIF_ACCOUNT_TRAIL"
        }
    }
    transformed_view = {
        consts.ID: "pcb-dev-processing.domain_account_management.CIF_ACCOUNT_TRAIL_DBEXT_COLUMN_TF_TIMESTAMP_TF_SCHEMA_SYNC_TF"
    }

    same_date = datetime(2025, 5, 31, 2, 7, 50)

    # Mock pandas DataFrame-like objects
    mock_current_series = MagicMock()
    mock_current_series.values = [same_date]

    mock_max_series = MagicMock()
    mock_max_series.values = [same_date]

    mock_query_result = MagicMock()
    mock_query_result.to_dataframe.return_value = {'FILE_CREATE_DT': mock_current_series}

    mock_max_result = MagicMock()
    mock_max_result.to_dataframe.return_value = {'max_file_create_dt': mock_max_series}

    mock_query_job = MagicMock()
    mock_query_job.result.side_effect = [mock_query_result, mock_max_result]

    bigquery_client = MagicMock()
    bigquery_client.query.return_value = mock_query_job
    mock_bigquery_client.return_value = bigquery_client

    # Act
    result = cif_file_loader.build_file_validation(
        file_name, transformation_config, transformed_view
    )

    # Assert
    assert result is True
