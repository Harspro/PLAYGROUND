"""
Tests for the miscutils functions utilized throughout the data foundation repository.

Author: Sharif Mansour
"""
import pytest
import util.constants as consts

from airflow.exceptions import AirflowNotFoundException, AirflowFailException
from airflow.hooks.base import BaseHook
from unittest.mock import patch, MagicMock
from util.miscutils import (
    airflow_connection_exists,
    create_airflow_connection,
    get_ephemeral_cluster_config,
    compose_infinite_files_into_one,
    get_smb_server_config,
    resolve_smb_server_config,
    get_dagruns_after,
    save_job_to_control_table,
    MACHINE_ALLOCATION,
    generate_bq_insert_from_filename,
    track_data_freshness,
    generate_and_upload_png,
    read_file_env,
    get_pnp_env,
    get_serverless_cluster_config
)
from io import BytesIO
from google.cloud.storage import Client, Bucket, Blob
from google.cloud.exceptions import NotFound
from unittest import mock
import os


@pytest.fixture
def mock_get_connection(mocker) -> MagicMock:
    """
    Pytest fixture to mock the BaseHook.get_connection method.

    This fixture creates a mock object for the `util.miscutils.BaseHook.get_connection`
    method using the `mocker.patch.object` method.

    :param mocker: The pytest mocker fixture used to create the mock object.
    :type mocker: :class:`pytest_mock.plugin.MockerFixture`

    :returns:
        A mock object for the `util.miscutils.BaseHook.get_connection` method.
    :rtype: :class:`unittest.mock.MagicMock`
    """
    return mocker.patch.object(BaseHook, 'get_connection')


@pytest.fixture
def test_connection_data() -> dict:
    """
    Pytest fixture providing test data for Airflow connection testing.

    It returns a dictionary that includes sample values for connection ID,
    type, description, host, login, vault password secret path,
    vault password secret key, password, schema, and port.

    :returns: A dictionary containing test data for Airflow connection creation.
    :rtype: dict
    """
    return {
        'conn_id': 'test-connection',
        'conn_type': 'test-type',
        'description': 'test-description',
        'host': 'test-host',
        'login': 'test-login',
        'vault_password_secret_path': 'test-vault-password-secret-path',
        'vault_password_secret_key': 'test-vault-password-secret-key',
        'password': 'test-password',
        'schema': 'test-schema',
        'port': 1234
    }


@pytest.mark.parametrize("conn_id, expected_result", [
    ("existing_conn_id", True),  # Test case for existing connection
    ("non_existing_conn_id", False)  # Test case for non-existing connection
])
def test_airflow_connection_exists(mock_get_connection, conn_id: str, expected_result: bool):
    """
    Test if `util.miscutils.airflow_connection_exists` function correctly checks
    if a connection exists or not.

    This test function is parameterized to run with different connection IDs and their expected results.
    It mocks the `util.miscutils.BaseHook.get_connection` method to simulate both existing and
    non-existing connections.

    :param mock_get_connection: Fixture providing a mock object for the `util.miscutils.BaseHook.get_connection` method.
    :type mock_get_connection: :class:`unittest.mock.MagicMock`

    :param conn_id: The unique identifier for the connection being tested.
    :type conn_id: str

    :param expected_result: The expected boolean result of the `util.miscutils.airflow_connection_exists` function
        for the given `conn_id`.
    :type expected_result: bool
    """
    # Mocking the BaseHook.get_connection method.
    if not expected_result:
        mock_get_connection.side_effect = AirflowNotFoundException
    else:
        mock_get_connection.return_value = "mocked_connection"

    # Assert that the method returns the expected result for the given connection ID.
    assert airflow_connection_exists(conn_id) == expected_result


@pytest.mark.parametrize("airflow_connection_result", [
    True,  # Test case for existing connection
    False  # Test case for non-existing connection
])
@patch('util.miscutils.VaultUtilBuilder')
@patch('util.miscutils.airflow_connection_exists')
@patch('util.miscutils.settings.Session')
@patch('util.miscutils.Connection')
def test_create_connection(mock_connection, mock_session, mock_airflow_connection_exists,
                           mock_vault_util_builder, airflow_connection_result: bool,
                           test_connection_data: dict):
    """
    Test case for the `util.miscutils.create_airflow_connection` function.

    This test case verifies that the `util.miscutils.create_airflow_connection` function
    correctly creates a new Airflow connection and adds/commits it to the session.

    :param mock_connection: A MagicMock representing the Connection class.
        This is patched to track calls to the Connection constructor.
    :type mock_connection: :class:`unittest.mock.MagicMock`

    :param mock_session: A MagicMock representing the settings.Session class.
        This is patched to allow tracking of calls to the session's add/commit method.
    :type mock_session: :class:`unittest.mock.MagicMock`

    :param mock_airflow_connection_exists: A MagicMock representing the airflow_connection_exists function.
        This is patched to control the behavior of checking for existing Airflow connections.
    :type mock_airflow_connection_exists: :class:`unittest.mock.MagicMock`

    :param airflow_connection_result: The boolean result of the airflow_connection_exists function.
        This parameterizes the test to simulate both existing and non-existing Airflow connections.
    :type airflow_connection_result: bool

    :param test_connection_data: Fixture providing a dictionary containing test data for creating
        the Airflow connection.
    :type test_connection_data: dict
    """

    # Setting return value for checking Airflow Connection.
    mock_airflow_connection_exists.return_value = airflow_connection_result

    # Create Airflow Connection only if it doesn't exist.
    if not airflow_connection_result:
        # Create MagicMock instances to represent the session's add and commit method.
        mock_add = MagicMock()
        mock_commit = MagicMock()
        mock_session.return_value.add = mock_add
        mock_session.return_value.commit = mock_commit
        mock_vault_util_builder.build().get_secret.return_value = test_connection_data['password']

        # Connection data.
        conn_data = test_connection_data

        # Call the function with test data.
        create_airflow_connection(
            conn_data['conn_id'],
            conn_data['conn_type'],
            conn_data['description'],
            conn_data['host'],
            conn_data['login'],
            conn_data['vault_password_secret_path'],
            conn_data['vault_password_secret_key'],
            conn_data['schema'],
            conn_data['port']
        )

        # Assert that the Connection constructor was called with the correct arguments.
        mock_connection.assert_called_once_with(
            conn_id=conn_data['conn_id'],
            conn_type=conn_data['conn_type'],
            description=conn_data['description'],
            host=conn_data['host'],
            login=conn_data['login'],
            password=conn_data['password'],
            schema=conn_data['schema'],
            port=conn_data['port']
        )

        # Assert that the add and commit method of the session mock was called once.
        mock_add.assert_called_once()
        mock_commit.assert_called_once()


def test_get_ephemeral_cluster_config():
    subnetwork_uri_dev = "https://www.googleapis.com/compute/v1/projects/pcb-nonprod-svpc/regions/northamerica-northeast1/subnetworks/snet-pcbnp-app01-dev"
    subnetwork_uri_prod = "https://www.googleapis.com/compute/v1/projects/pcb-prod-svpc/regions/northamerica-northeast1/subnetworks/snet-pcbpr-app01-nane1"
    default_machine_type = MACHINE_ALLOCATION["dev"][consts.PRIMARY_MACHINE_TYPE]

    cluster_config = get_ephemeral_cluster_config('dev', 'test_network_tag')

    assert 'test_network_tag' in cluster_config.get('gce_cluster_config').get('tags')
    assert subnetwork_uri_dev == cluster_config.get('gce_cluster_config').get('subnetwork_uri')
    assert default_machine_type == cluster_config.get('master_config').get('machine_type_uri')
    assert default_machine_type == cluster_config.get('worker_config').get('machine_type_uri')
    assert 'true' == cluster_config.get('software_config').get('properties').get('dataproc:dataproc.conscrypt.provider.enable')

    primary_machine_type = MACHINE_ALLOCATION["prod"][consts.PRIMARY_MACHINE_TYPE]
    cluster_config = get_ephemeral_cluster_config('prod', 'test_network_tag', consts.DATAPROC_CLUSTER_SIZES["medium"], True)
    assert subnetwork_uri_prod == cluster_config.get('gce_cluster_config').get('subnetwork_uri')
    assert primary_machine_type == cluster_config.get('master_config').get('machine_type_uri')
    assert primary_machine_type == cluster_config.get('worker_config').get('machine_type_uri')
    assert 'false' == cluster_config.get('software_config').get('properties').get('dataproc:dataproc.conscrypt.provider.enable')

    secondary_machine_type = MACHINE_ALLOCATION["prod"][consts.SECONDARY_MACHINE_TYPE]
    cluster_config = get_ephemeral_cluster_config('prod', 'test_network_tag', consts.DATAPROC_CLUSTER_SIZES["small"], True)
    assert subnetwork_uri_prod == cluster_config.get('gce_cluster_config').get('subnetwork_uri')
    assert secondary_machine_type == cluster_config.get('master_config').get('machine_type_uri')
    assert secondary_machine_type == cluster_config.get('worker_config').get('machine_type_uri')
    assert 'false' == cluster_config.get('software_config').get('properties').get('dataproc:dataproc.conscrypt.provider.enable')


def test_empty_config_compose_files():
    """
        this will test compose_infinite_files_into_one when no source or dest config is provided.
        It raised AirflowFailException as source and dest config is mandatory

    """

    with pytest.raises(AirflowFailException) as ex_info:
        compose_infinite_files_into_one(None, None, False, False)
    assert "please provide source and dest config" in str(ex_info.value)


@patch('util.miscutils.storage.Client')
def test_no_blobs_source_dir_while_compose_files(storage_client_mock):

    """
        this will test compose_infinite_files_into_one when  source and dest config is provided.
        However, It raised AirflowFailException as No objects are found in the source directory
        to compose.

        :param storage_client_mock: Mocked variable to return the google storage client
        :type storage_client_mock: :class:`google.cloud.storage.Client`
    """

    source_config = {'bucket': 'test_source_bucket', 'folder_prefix': 'cdic/pcma/test_extract',
                     'file_prefix': 'cdic-0110-'}
    dest_config = {'bucket': 'cdic-outbound-pcb-dev', 'folder_prefix': 'pcma', 'file_name': 'test.txt'}
    storage_client_mock.return_value.list_blobs.return_value = None
    with pytest.raises(AirflowFailException) as ex_info:
        compose_infinite_files_into_one(source_config, dest_config, False, False)
    assert "No objects to compose in test_source_bucket" in str(ex_info.value)


@patch('util.miscutils.storage.Client')
def test_no_filtered_blobs_source_dir_while_compose_files(storage_client_mock):

    """
        this will test compose_infinite_files_into_one when  source and dest config is provided.
        However, It raised AirflowFailException as No objects are found in the source directory
        to compose.

        :param storage_client_mock: Mocked variable to return the google storage client
        :type storage_client_mock: :class:`google.cloud.storage.Client`
    """

    source_config = {'bucket': 'test_source_bucket', 'folder_prefix': 'cdic/pcma/test_extract',
                     'file_prefix': 'cdic-0110-'}
    dest_config = {'bucket': 'cdic-outbound-pcb-dev', 'folder_prefix': 'pcma', 'file_name': 'test.txt'}
    mock_blob1 = MagicMock(spec=Blob)
    mock_blob1.name = "cdic-0100.txt"
    storage_client_mock.return_value.list_blobs.return_value = [mock_blob1]
    with pytest.raises(AirflowFailException) as ex_info:
        compose_infinite_files_into_one(source_config, dest_config, False, False)
    assert "No filtered source objects to compose in" in str(ex_info.value)


@patch('util.miscutils.storage.Client')
def test_write_open_throw_exception(storage_client_mock):

    """
        this will test compose_infinite_files_into_one when  source and dest config is provided.
        However, It raised AirflowFailException as failed to write to dest bucket

        :param storage_client_mock: Mocked variable to return the google storage client
        :type storage_client_mock: :class:`google.cloud.storage.Client`
    """

    source_config = {'bucket': 'test_source_bucket', 'folder_prefix': 'cdic/pcma/test_extract',
                     'file_prefix': 'cdic-0110-'}
    dest_config = {'bucket': 'cdic-outbound-pcb-dev', 'folder_prefix': 'pcma', 'file_name': 'test.txt'}
    mock_blob1 = MagicMock(spec=Blob)
    mock_blob1.name = "cdic-0110-0000001.txt"
    storage_client_mock.return_value.list_blobs.return_value = [mock_blob1]
    mock_blob1.open.side_effect = IOError("Failed to write blob in write-binary mode")

    with pytest.raises(AirflowFailException) as ex_info:
        compose_infinite_files_into_one(source_config, dest_config, False, False)
    assert "Failed to write to destination blob" in str(ex_info.value)


@pytest.mark.parametrize(
    "windows_compatible, replace_empty_with_blank, source_data_1, source_data_2, source_data_3, result",
    [
        (False, False, "header\nline1\nline2\n", "", "header\nline3\nline4\n", "header\nline1\nline2\nline3\nline4\n"),
        (True, False, "header\nline1\nline2\n", "header\n", "header\nline3\nline4\n",
         "header\r\nline1\r\nline2\r\nline3\r\nline4\r\n"),
        (True, True, "header\ncol1,'col2'\nline2\n", "header\n", "header\nline3\nline4\n",
         "header\r\ncol1,'col2'\r\nline2\r\nline3\r\nline4\r\n"),
        (True, True, 'header\ncol1,"col2"\nline2\n', "header\n", "header\nline3\nline4\n",
         "header\r\ncol1,\"col2\"\r\nline2\r\nline3\r\nline4\r\n"),
        (True, False, 'header\ncol1,"col2"\nline2\n', "header\n", "header\nline3\nline4\n",
         'header\r\ncol1,"col2"\r\nline2\r\nline3\r\nline4\r\n'),
        (True, False, "header\ncol1,'col2'\nline2\n", "header\n", "header\nline3\nline4\n",
         "header\r\ncol1,'col2'\r\nline2\r\nline3\r\nline4\r\n"),
    ]
)
@patch('util.miscutils.storage.Client')
def test_file_compose_no_windows_compatible(mock_client, windows_compatible, replace_empty_with_blank, source_data_1,
                                            source_data_2, source_data_3, result):

    """
        this will test compose_infinite_files_into_one when  source and dest config is provided.
        It tests whether lists of blobs returned are composed into a single file. It uses Mocked Blobs and
        capture the open value as file and assert the output in the destination file
        This tests windows compatibility and assert that LF is replaced with CR LF characters.

        :param mock_client: Mocked variable to return the google storage client
        :type mock_client: :class:`google.cloud.storage.Client`

        :param windows_compatible: Configurable boolean flag for windows compatibility
        :type windows_compatible: :class:`bool`

        param replace_empty_with_blank: Configurable boolean flag for replacing empty string with blanks
        :type replace_empty_with_blank: :class:`bool`

        :param source_data_1: Configurable Source data from Blob
        :type source_data_1: :class:`str`

        :param source_data_2: Configurable Source data from Blob
        :type source_data_2: :class:`str`

        :param source_data_3: Configurable Source data from Blob
        :type source_data_3 :class:`str`

        :param result: Configurable result to be written to destination Blob
        :type result: :class:`str`
    """
    # Mock setup
    mock_source_file1 = BytesIO(source_data_1.encode("utf-8"))
    mock_source_file2 = BytesIO(source_data_2.encode("utf-8"))
    mock_source_file3 = BytesIO(source_data_3.encode("utf-8"))
    mock_dest_file = BytesIO()

    mock_blob1 = MagicMock(spec=Blob)
    mock_blob1.name = "file1.txt"
    mock_blob2 = MagicMock(spec=Blob)
    mock_blob2.name = "file2.txt"
    mock_blob3 = MagicMock(spec=Blob)
    mock_blob3.name = "file3.txt"

    mock_dest_bucket = MagicMock()
    mock_dest_blob = MagicMock(spec=Blob)
    mock_dest_bucket.blob.return_value = mock_dest_blob

    # mock storage client
    mock_client.return_value.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]
    mock_client.return_value.bucket.return_value = mock_dest_bucket

    mock_blob1.open.return_value.__enter__.return_value = mock_source_file1
    mock_blob2.open.return_value.__enter__.return_value = mock_source_file2
    mock_blob3.open.return_value.__enter__.return_value = mock_source_file3
    mock_dest_blob.open.return_value.__enter__.return_value = mock_dest_file

    source_config = {"bucket": "source_bucket", "folder_prefix": "prefix", "file_prefix": "file"}
    dest_config = {"bucket": "dest_bucket", "folder_prefix": "output", "file_name": "combined.txt"}

    compose_infinite_files_into_one(source_config, dest_config, win_compatible=windows_compatible,
                                    replace_empty_with_blank=replace_empty_with_blank)

    assert mock_dest_file.getvalue() == result.encode("utf-8")


@pytest.fixture
def test_ctera_config_data() -> dict:
    return {
        "i_test": {
            "nonprod": {
                "ctera_shared_folder": "my_folder",
                "hostname": "my_hostname",
                "username": "my_username",
                "secret_path": "my_secret_path",
                "secret_key": "my_secret_key"
            },
            "prod": {
                "ctera_shared_folder": "my_folder",
                "hostname": "my_hostname",
                "username": "my_username",
                "secret_path": "my_secret_path",
                "secret_key": "my_secret_key"
            },
        }
    }


@pytest.mark.parametrize("deploy_env", ["nonprod", "prod"])
@patch('util.miscutils.VaultUtilBuilder')
@patch('util.miscutils.read_yamlfile_env')
def test_get_smb_server_config(mock_read_yamlfile_env, mock_vault_util_builder,
                               deploy_env: str, test_ctera_config_data: dict):
    mock_read_yamlfile_env.return_value = test_ctera_config_data
    mock_vault_util_builder.build().get_secret.return_value = 'test_passwd'

    hostname, username, password = get_smb_server_config('dummy.yaml', 'i_test', deploy_env)

    mock_vault_util_builder.build().get_secret.assert_called_with('my_secret_path', 'my_secret_key', consts.ALM_VAULT_NAME)

    assert hostname == 'my_hostname'
    assert username == 'my_username'
    assert password == 'test_passwd'


@pytest.mark.parametrize("deploy_env", ["nonprod", "prod"])
@patch('util.miscutils.VaultUtilBuilder')
def test_resolve_smb_server_config(mock_vault_util_builder,
                                   deploy_env: str, test_ctera_config_data: dict):
    mock_vault_util_builder.build().get_secret.return_value = 'test_passwd'

    hostname, username, password = resolve_smb_server_config(test_ctera_config_data.get('i_test').get(deploy_env))

    mock_vault_util_builder.build().get_secret.assert_called_with('my_secret_path', 'my_secret_key', None)

    assert hostname == 'my_hostname'
    assert username == 'my_username'
    assert password == 'test_passwd'


@pytest.mark.parametrize("deploy_env", ["nonprod", "prod"])
@patch('util.miscutils.VaultUtilBuilder')
def test_resolve_smb_server_config_almvault(mock_vault_util_builder,
                                            deploy_env: str, test_ctera_config_data: dict):
    mock_vault_util_builder.build().get_secret.return_value = 'test_passwd'

    hostname, username, password = resolve_smb_server_config(test_ctera_config_data.get('i_test').get(deploy_env),
                                                             consts.ALM_VAULT_NAME)

    mock_vault_util_builder.build().get_secret.assert_called_with('my_secret_path', 'my_secret_key',
                                                                  consts.ALM_VAULT_NAME)

    assert hostname == 'my_hostname'
    assert username == 'my_username'
    assert password == 'test_passwd'


@patch("google.cloud.bigquery.Client")
@patch("util.miscutils.read_variable_or_file")
def test_get_dagruns_after(mock_read_variable_or_file, mock_bigquery_client):
    # Mock gcp_config
    mock_read_variable_or_file.return_value = {"curated_zone_project_id": "pcb-dev-curated"}

    # Set up the BigQuery client mock
    bigquery_client = MagicMock()
    mock_bigquery_client.return_value = bigquery_client

    query_job = MagicMock()
    bigquery_client.query.return_value = query_job
    query_job.result.return_value = 'result'

    # Call the function
    get_dagruns_after('test_dag_id', '2025-05-23 00:00:00')

    # Verify that query was called correctly
    bigquery_client.query.assert_called_once()
    table_id = "pcb-dev-curated.JobControlDataset.DAGS_HISTORY"

    args, kwargs = bigquery_client.query.call_args
    assert table_id in args[0]
    assert "test_dag_id" in args[0]
    assert "2025-05-23 00:00:00" in args[0]


@pytest.fixture
def test_context_data() -> dict:
    dag = MagicMock()
    dag.dag_id = 'test_dag_id'

    return {
        'dag': dag,
        'run_id': 'test_run_id'
    }


@patch("util.miscutils.read_variable_or_file")  # Mock read_variable_or_file
@patch("google.cloud.bigquery.Client")
def test_save_job_to_control_table_default_tms(mock_bigquery_client, mock_read_variable_or_file, test_context_data):
    # Mock gcp_config to return the expected project ID
    mock_read_variable_or_file.return_value = {"curated_zone_project_id": "pcb-dev-curated"}

    bigquery_client = MagicMock()
    mock_bigquery_client.return_value = bigquery_client
    mock_bigquery_client.get_table.return_value = True
    query_job = MagicMock()
    bigquery_client.query.return_value = query_job
    query_job.result.return_value = 'result'

    save_job_to_control_table('{"param":"value"}', **test_context_data)

    bigquery_client.query.assert_called_once()
    table_id = "pcb-dev-curated.JobControlDataset.DAGS_HISTORY"
    args, kwargs = bigquery_client.query.call_args
    assert table_id in args[0]
    assert 'CURRENT_TIMESTAMP()' in args[0]


@patch("util.miscutils.read_variable_or_file")  # Mock read_variable_or_file
@patch("google.cloud.bigquery.Client")
def test_save_job_to_control_table_provided_tms(mock_bigquery_client, mock_read_variable_or_file, test_context_data):
    # Mock gcp_config to return the expected project ID
    mock_read_variable_or_file.return_value = {"curated_zone_project_id": "pcb-dev-curated"}

    bigquery_client = MagicMock()
    mock_bigquery_client.return_value = bigquery_client
    mock_bigquery_client.get_table.return_value = True
    query_job = MagicMock()
    bigquery_client.query.return_value = query_job
    query_job.result.return_value = 'result'

    save_job_to_control_table('{"param":"value"}', '2025-05-26 12:00:00', **test_context_data)

    bigquery_client.query.assert_called_once()
    table_id = "pcb-dev-curated.JobControlDataset.DAGS_HISTORY"
    args, kwargs = bigquery_client.query.call_args
    assert table_id in args[0]
    assert '2025-05-26 12:00:00' in args[0]
    assert 'test_dag_id' in args[0]
    assert 'test_run_id' in args[0]


def test_empty_config_read_filename():
    """
        this will test generate_bq_insert_from_filename when no source or dest config is provided.
        It raised AirflowFailException as source and dest config is mandatory
    """

    with pytest.raises(AirflowFailException) as ex_info:
        generate_bq_insert_from_filename(None, None)
    assert "please provide source and dest config" in str(ex_info.value)


@patch('util.bq_utils.run_bq_query', autospec=True)
@patch('util.miscutils.storage.Client', autospec=True)
def test_no_source_dir_to_iterate_files(mock_storage_client, mock_bq_query):
    """
    This will test generate_bq_insert_from_filename when source and dest config is provided.
    However, it raises AirflowFailException as no files are found in the specified folder.

    :param mock_storage_client: Mocked Google storage client
    :type mock_storage_client: :class:`google.cloud.storage.Client`
    :param mock_bq_query: Mocked BigQuery query function
    :type mock_bq_query: :class:`unittest.mock.MagicMock`
    """
    source_config = {
        'project': 'test_gcp_project_001',
        'bucket': 'test_bucket_001',
        'folder_prefix': 'test_folder_001',
        'file_extension': '.pdf',
        'str_split_at_pattern': '_',
        'execution_datetime': '2025-01-01T00:00:00'
    }
    dest_config = {'table_name': 'test_table_name_001', 'sql_file_path': 'test_sql_file.sql'}
    mock_storage_client.return_value.list_blobs.return_value = []
    with pytest.raises(AirflowFailException) as ex_info:
        generate_bq_insert_from_filename(source_config, dest_config)
    assert "No source object:- test_bucket_001/test_folder_001" == str(ex_info.value)
    mock_storage_client.assert_called()  # Verify storage client was called
    mock_bq_query.assert_not_called()  # Verify no BigQuery query was called


@patch('util.bq_utils.run_bq_query', autospec=True)
@patch('util.miscutils.storage.Client', autospec=True)
def test_no_matched_files_to_iterate(mock_storage_client, mock_bq_query):
    """
    This will test generate_bq_insert_from_filename when source and dest config is provided.
    However, it raises AirflowFailException as no matching files are found.

    :param mock_storage_client: Mocked Google storage client
    :type mock_storage_client: :class:`google.cloud.storage.Client`
    :param mock_bq_query: Mocked BigQuery query function
    :type mock_bq_query: :class:`unittest.mock.MagicMock`
    """
    source_config = {
        'project': 'test_gcp_project_001',
        'bucket': 'test_bucket_001',
        'folder_prefix': 'test_folder_001',
        'str_split_at_pattern': '_',
        'execution_datetime': '2025-01-01T00:00:00'
    }
    dest_config = {'table_name': 'test_table_name_001', 'sql_file_path': 'test_sql_file.sql'}
    # Setup: The storage client returns no blobs, simulating "no matching files"x
    mock_storage_client.return_value.list_blobs.return_value = []

    with pytest.raises(AirflowFailException) as ex_info:
        generate_bq_insert_from_filename(source_config, dest_config)

    # Assert the correct error message; update this string if your code's error has changed
    assert "No source object:- test_bucket_001/test_folder_001" in str(ex_info.value)
    mock_storage_client.assert_called()
    mock_bq_query.assert_not_called()


# ============================================================================
# Tests for track_data_freshness function (simplified architecture)
# ============================================================================

@pytest.fixture
def freshness_context_data() -> dict:
    """
    Pytest fixture providing test context data for track_data_freshness function testing.

    :returns: A dictionary containing test data for Airflow context.
    :rtype: dict
    """
    dag = MagicMock()
    dag.dag_id = 'test_freshness_dag'

    return {
        'dag': dag,
        'run_id': 'test_run_id_123'
    }


@pytest.fixture
def single_table_info() -> list:
    """
    Pytest fixture providing single-table info for testing.

    :returns: A list containing single-table info.
    :rtype: list
    """
    return [
        {
            'project_id': 'dummy_project',
            'dataset_id': 'dummy_dataset',
            'table_name': 'dummy_table'
        }
    ]


@pytest.fixture
def multi_table_info() -> list:
    """
    Pytest fixture providing multi-table info for testing.

    :returns: A list containing multi-table info.
    :rtype: list
    """
    return [
        {
            'project_id': 'dummy_project',
            'dataset_id': 'dummy_dataset',
            'table_name': 'dummy_table_1'
        },
        {
            'project_id': 'dummy_project',
            'dataset_id': 'dummy_dataset',
            'table_name': 'dummy_table_2'
        }
    ]


@pytest.mark.parametrize("tables_info,should_call", [
    ([], True),    # Empty list - should call track_data_freshness
    (None, False)  # Missing/None - should NOT call track_data_freshness
])
@patch("util.miscutils.track_data_freshness")
@patch("util.miscutils.read_variable_or_file")
@patch("google.cloud.bigquery.Client")
def test_save_job_to_control_table_tables_info_scenarios(mock_bigquery_client, mock_read_variable_or_file, mock_track_data_freshness, test_context_data, tables_info, should_call):
    """Test save_job_to_control_table behavior with different tables_info scenarios"""
    # Mock setup
    mock_read_variable_or_file.return_value = {"curated_zone_project_id": "pcb-dev-curated"}
    mock_bigquery_client.return_value.get_table.return_value = True
    mock_bigquery_client.return_value.query.return_value.result.return_value = 'result'

    # Setup context based on test parameter
    context = test_context_data.copy()
    if tables_info is not None:
        context['tables_info'] = tables_info

    save_job_to_control_table('{"param":"value"}', **context)

    # Verify behavior based on expectation
    if should_call:
        mock_track_data_freshness.assert_called_once_with('test_dag_id', tables_info)
    else:
        mock_track_data_freshness.assert_not_called()


@patch("util.miscutils.read_variable_or_file")
@patch("google.cloud.bigquery.Client")
def test_track_data_freshness_single_table_success(mock_bigquery_client, mock_read_variable_or_file, single_table_info):
    """
    Test track_data_freshness function with single-table info - successful execution.

    This test verifies that the function correctly handles single-table info,
    creates the freshness table if needed, and upserts freshness data.
    """
    # Mock GCP config
    mock_gcp_config = {
        'curated_zone_project_id': 'pcb-dev-curated',
        'landing_zone_project_id': 'pcb-dev-landing'
    }
    mock_read_variable_or_file.return_value = mock_gcp_config

    # Mock BigQuery client
    bigquery_client = MagicMock()
    mock_bigquery_client.return_value = bigquery_client

    # Mock table exists (no NotFound exception)
    bigquery_client.get_table.return_value = True

    # Mock query execution
    query_job = MagicMock()
    bigquery_client.query.return_value = query_job
    query_job.result.return_value = 'success'

    # Call the function with simplified parameters
    track_data_freshness('test_freshness_dag', single_table_info)

    # Verify BigQuery client was created once
    mock_bigquery_client.assert_called_once()

    # Verify table existence check
    bigquery_client.get_table.assert_called_once()
    table_id_arg = bigquery_client.get_table.call_args[0][0]
    assert "pcb-dev-landing.domain_data_logs.DATA_FRESHNESS" == table_id_arg

    # Verify upsert query was executed
    bigquery_client.query.assert_called_once()
    upsert_query = bigquery_client.query.call_args[0][0]
    assert "MERGE" in upsert_query
    assert "dummy_project" in upsert_query
    assert "dummy_dataset" in upsert_query
    assert "dummy_table" in upsert_query
    assert "test_freshness_dag" in upsert_query


@patch("util.miscutils.read_variable_or_file")
@patch("google.cloud.bigquery.Client")
def test_track_data_freshness_multi_table_success(mock_bigquery_client, mock_read_variable_or_file, multi_table_info):
    """
    Test track_data_freshness function with multi-table info - successful execution.

    This test verifies that the function correctly handles multi-table info
    and creates freshness records for all tables.
    """
    # Mock GCP config
    mock_gcp_config = {
        'curated_zone_project_id': 'pcb-dev-curated',
        'landing_zone_project_id': 'pcb-dev-landing'
    }
    mock_read_variable_or_file.return_value = mock_gcp_config

    # Mock BigQuery client
    bigquery_client = MagicMock()
    mock_bigquery_client.return_value = bigquery_client

    # Mock table exists
    bigquery_client.get_table.return_value = True

    # Mock query execution
    query_job = MagicMock()
    bigquery_client.query.return_value = query_job
    query_job.result.return_value = 'success'

    # Call the function with simplified parameters
    track_data_freshness('test_freshness_dag', multi_table_info)

    # Verify BigQuery client was created once
    mock_bigquery_client.assert_called_once()

    # Verify table existence check
    bigquery_client.get_table.assert_called_once()

    # Verify upsert queries were executed (2 tables = 2 queries)
    assert bigquery_client.query.call_count == 2

    # Check both tables are referenced in queries
    all_queries = [call[0][0] for call in bigquery_client.query.call_args_list]
    assert any("dummy_table_1" in query for query in all_queries)
    assert any("dummy_table_2" in query for query in all_queries)


@patch("util.miscutils.read_variable_or_file")
@patch("google.cloud.bigquery.Client")
def test_track_data_freshness_table_creation(mock_bigquery_client, mock_read_variable_or_file, single_table_info):
    """
    Test track_data_freshness function creates freshness table when it doesn't exist.
    """
    # Mock GCP config
    mock_gcp_config = {
        'curated_zone_project_id': 'pcb-dev-curated',
        'landing_zone_project_id': 'pcb-dev-landing'
    }
    mock_read_variable_or_file.return_value = mock_gcp_config

    # Mock BigQuery client
    bigquery_client = MagicMock()
    mock_bigquery_client.return_value = bigquery_client

    # Mock table doesn't exist (raise NotFound)
    bigquery_client.get_table.side_effect = NotFound("Table not found")

    # Mock query execution for both CREATE TABLE and UPSERT
    query_job = MagicMock()
    bigquery_client.query.return_value = query_job
    query_job.result.return_value = 'success'

    # Call the function
    track_data_freshness('test_freshness_dag', single_table_info)

    # Verify table creation and upsert queries were executed
    assert bigquery_client.query.call_count == 2

    # Check CREATE TABLE query
    create_query = bigquery_client.query.call_args_list[0][0][0]
    assert "CREATE TABLE IF NOT EXISTS" in create_query
    assert "pcb-dev-landing.domain_data_logs.DATA_FRESHNESS" in create_query


@pytest.mark.parametrize("tables_info,gcp_config,description", [
    ([], {'curated_zone_project_id': 'pcb-dev-curated', 'landing_zone_project_id': 'pcb-dev-landing'}, "empty tables_info"),
    (None, {'curated_zone_project_id': 'pcb-dev-curated', 'landing_zone_project_id': 'pcb-dev-landing'}, "None tables_info"),
    ([{'dataset_id': 'dummy_dataset', 'table_name': 'dummy_table'}], None, "missing GCP config")
])
@patch("util.miscutils.read_variable_or_file")
def test_track_data_freshness_validation_scenarios(mock_read_variable_or_file, tables_info, gcp_config, description):
    """Test track_data_freshness validation with different failure scenarios"""
    # Mock GCP config based on test parameter
    mock_read_variable_or_file.return_value = gcp_config

    # Call the function (should return early with warning for all scenarios)
    track_data_freshness('test_freshness_dag', tables_info)

    # Should return early, no BigQuery operations


@patch("util.miscutils.read_variable_or_file")
@patch("google.cloud.bigquery.Client")
def test_track_data_freshness_bigquery_client_failure(mock_bigquery_client, mock_read_variable_or_file, single_table_info):
    """
    Test track_data_freshness function when BigQuery client initialization fails.
    """
    # Mock GCP config
    mock_gcp_config = {
        'curated_zone_project_id': 'pcb-dev-curated',
        'landing_zone_project_id': 'pcb-dev-landing'
    }
    mock_read_variable_or_file.return_value = mock_gcp_config

    # Mock BigQuery client initialization failure
    mock_bigquery_client.side_effect = Exception("BigQuery client failed")

    # Call the function (should handle exception gracefully)
    track_data_freshness('test_freshness_dag', single_table_info)

    # Verify BigQuery client creation was attempted once
    mock_bigquery_client.assert_called_once()


@patch("util.miscutils.read_variable_or_file")
@patch("google.cloud.bigquery.Client")
def test_track_data_freshness_invalid_table_info(mock_bigquery_client, mock_read_variable_or_file):
    """
    Test track_data_freshness function with invalid table info format.
    """
    # Mock GCP config
    mock_gcp_config = {
        'curated_zone_project_id': 'pcb-dev-curated',
        'landing_zone_project_id': 'pcb-dev-landing'
    }
    mock_read_variable_or_file.return_value = mock_gcp_config

    # Mock BigQuery client
    bigquery_client = MagicMock()
    mock_bigquery_client.return_value = bigquery_client
    bigquery_client.get_table.return_value = True

    # Invalid table info (missing required fields)
    invalid_table_info = [
        {'dataset_id': 'dummy_dataset'},  # Missing table_name
        {'table_name': 'dummy_table'},    # Missing dataset_id
        'invalid_format'                  # Not a dict
    ]

    # Call the function (should skip invalid entries)
    track_data_freshness('test_freshness_dag', invalid_table_info)

    # Verify BigQuery client was created once
    mock_bigquery_client.assert_called_once()
    # No valid tables, so no queries should be executed
    bigquery_client.query.assert_not_called()


@pytest.fixture
def valid_source_config():
    return {
        "png_file_name": "sample.png",
        "png_content": "hello world",
        "width": 50,
        "height": 20,
        "colour": [200, 200, 200],
    }


@pytest.fixture
def valid_dest_config():
    return {
        "target_gcp_bucket": "my-bucket",
        "target_gcp_path": "folder/sample.png"
    }


def test_missing_source_field(valid_dest_config):
    bad_source = {"png_file_name": "sample.png"}  # missing png_content
    with pytest.raises(AirflowFailException, match="Missing required source fields"):
        generate_and_upload_png(bad_source, valid_dest_config)


def test_missing_dest_field(valid_source_config):
    bad_dest = {"target_gcp_bucket": "bucket"}  # missing target_gcp_path
    with pytest.raises(AirflowFailException, match="Missing required dest fields"):
        generate_and_upload_png(valid_source_config, bad_dest)


def test_invalid_dimensions(valid_source_config, valid_dest_config):
    bad_source = {**valid_source_config, "width": 0}
    with pytest.raises(AirflowFailException, match="Invalid PNG dimensions"):
        generate_and_upload_png(bad_source, valid_dest_config)


def test_invalid_colour(valid_source_config, valid_dest_config):
    bad_source = {**valid_source_config, "colour": [999, 0, 0]}  # invalid RGB
    with pytest.raises(AirflowFailException, match="Invalid RGB color"):
        generate_and_upload_png(bad_source, valid_dest_config)


@patch("util.miscutils.storage.Client")
def test_successful_upload(mock_client, valid_source_config, valid_dest_config):
    # Arrange mocks
    mock_blob = MagicMock()
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client.return_value.bucket.return_value = mock_bucket

    # Act
    generate_and_upload_png(valid_source_config, valid_dest_config)

    # Assert blob upload & metadata
    mock_bucket.blob.assert_called_once_with(valid_dest_config["target_gcp_path"])
    mock_blob.upload_from_filename.assert_called_once()
    assert mock_blob.metadata["content"] == valid_source_config["png_content"]
    mock_blob.patch.assert_called_once()

    # Assert temp file cleanup (upload_from_filename arg should no longer exist)
    temp_file = mock_blob.upload_from_filename.call_args[0][0]
    assert not os.path.exists(temp_file)


@patch("util.miscutils.storage.Client")
def test_upload_failure_raises(mock_client, valid_source_config, valid_dest_config):
    # Arrange failing upload
    mock_blob = MagicMock()
    mock_blob.upload_from_filename.side_effect = Exception("GCS error")
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client.return_value.bucket.return_value = mock_bucket

    # Act + Assert
    with pytest.raises(AirflowFailException, match="PNG generation and upload failed"):
        generate_and_upload_png(valid_source_config, valid_dest_config)

    # Ensure cleanup occurred
    temp_file = mock_blob.upload_from_filename.call_args[0][0]
    assert not os.path.exists(temp_file)


def prepare_mock_file(mocker, mock_data):
    mocker.patch('builtins.open', mock.mock_open(read_data=mock_data))
    mocker.patch('os.path.exists', return_value=True)


def test_read_file_env_file_exists_no_deploy_env(mocker):
    """Test read_file_env when file exists and no deploy_env is provided."""
    prepare_mock_file(mocker, "Hello World\nThis is a test file.")
    result = read_file_env("temp_file_with_content")
    expected = "Hello World\nThis is a test file."
    assert result == expected


def test_read_file_env_file_not_exists():
    """Test read_file_env when file does not exist - should raise AirflowFailException."""
    non_existent_file = "/path/to/non/existent/file.txt"

    with pytest.raises(AirflowFailException) as exc_info:
        read_file_env(non_existent_file)

    assert f"The file {non_existent_file} doesnt exist" in str(exc_info.value)


def test_read_file_env_with_env_placeholder(mocker):
    """Test read_file_env with ENV_PLACEHOLDER replacement."""
    prepare_mock_file(mocker, "Environment: {env}\nPNP Environment: {pnp_env}\nOther content")
    result = read_file_env("temp_file_with_placeholders", deploy_env="dev")
    expected = "Environment: dev\nPNP Environment: nonprod\nOther content"
    assert result == expected


def test_read_file_env_with_pnp_env_placeholder(mocker):
    """Test read_file_env with PNP_ENV_PLACEHOLDER replacement."""
    prepare_mock_file(mocker, "Environment: {env}\nPNP Environment: {pnp_env}\nOther content")
    result = read_file_env("temp_file_with_placeholders", deploy_env="test")
    expected = "Environment: test\nPNP Environment: prod\nOther content"
    assert result == expected


def test_read_file_env_with_both_placeholders(mocker):
    """Test read_file_env with both ENV_PLACEHOLDER and PNP_ENV_PLACEHOLDER."""
    prepare_mock_file(mocker, "Environment: {env}\nPNP Environment: {pnp_env}\nOther content")
    result = read_file_env("temp_file_with_placeholders", deploy_env="prod")
    expected = "Environment: prod\nPNP Environment: prod\nOther content"
    assert result == expected


@pytest.mark.parametrize("deploy_env,expected_pnp_env", [
    ("prod", "prod"),
    ("nonprod", "nonprod"),
    ("dev", "nonprod"),
    ("test", "prod"),
    ("staging", "prod"),
    ("", "prod"),
    (None, "prod")
])
def test_read_file_env_pnp_env_mapping(mocker, deploy_env, expected_pnp_env):
    """Test PNP_ENV_PLACEHOLDER mapping for different deploy_env values."""
    prepare_mock_file(mocker, "Environment: {env}\nPNP Environment: {pnp_env}\nOther content")
    result = read_file_env("temp_file_with_placeholders", deploy_env=deploy_env)
    if deploy_env is not None:
        expected = f"Environment: {deploy_env}\nPNP Environment: {expected_pnp_env}\nOther content"
    else:
        expected = "Environment: {env}\nPNP Environment: {pnp_env}\nOther content"

    assert result == expected


def test_read_file_env_empty_file(mocker):
    """Test read_file_env with empty file content."""
    prepare_mock_file(mocker, '')
    result = read_file_env("temp_file_empty")
    assert result == ""


def test_read_file_env_empty_file_with_deploy_env(mocker):
    """Test read_file_env with empty file content and deploy_env provided."""
    prepare_mock_file(mocker, '')
    result = read_file_env("temp_file_empty", deploy_env="dev")
    assert result == ""


def test_read_file_env_unicode_content(mocker):
    """Test read_file_env with unicode/special characters in file content."""
    prepare_mock_file(mocker, "Hello 世界 🌍\nUnicode: café, naïve, résumé")
    result = read_file_env("temp_file_unicode")
    expected = "Hello 世界 🌍\nUnicode: café, naïve, résumé"
    assert result == expected


def test_read_file_env_unicode_with_placeholders(mocker):
    """Test read_file_env with unicode content and placeholders."""
    prepare_mock_file(mocker, "Hello 世界 {env} 🌍\nUnicode: café {pnp_env}, naïve")
    result = read_file_env("temp_file_unicode", deploy_env="dev")
    expected = "Hello 世界 dev 🌍\nUnicode: café nonprod, naïve"
    assert result == expected


def test_read_file_env_multiple_env_placeholders(mocker):
    """Test read_file_env with multiple ENV_PLACEHOLDER occurrences."""
    prepare_mock_file(mocker, "First {env}, Second {env}, Third {env}")
    result = read_file_env("temp_file", deploy_env="prod")
    expected = "First prod, Second prod, Third prod"
    assert result == expected


def test_read_file_env_multiple_pnp_placeholders(mocker):
    """Test read_file_env with multiple PNP_ENV_PLACEHOLDER occurrences."""
    prepare_mock_file(mocker, "First {pnp_env}, Second {pnp_env}, Third {pnp_env}")

    result = read_file_env("temp_file", deploy_env="test")
    expected = "First prod, Second prod, Third prod"
    assert result == expected


def test_read_file_env_mixed_placeholders(mocker):
    """Test read_file_env with mixed placeholders and regular text."""
    prepare_mock_file(mocker, "Start {env} middle {pnp_env} end {env} final {pnp_env}")

    result = read_file_env("temp_file", deploy_env="staging")
    expected = "Start staging middle prod end staging final prod"
    assert result == expected


def test_read_file_env_no_placeholders_in_content(mocker):
    """Test read_file_env with deploy_env but no placeholders in content."""
    prepare_mock_file(mocker, "Hello World\nThis is a test file.")
    result = read_file_env("temp_file_with_content", deploy_env="dev")
    expected = "Hello World\nThis is a test file."
    assert result == expected


def test_read_file_env_deploy_env_none(mocker):
    """Test read_file_env with deploy_env explicitly set to None."""
    prepare_mock_file(mocker, "Environment: {env}\nPNP Environment: {pnp_env}\nOther content")
    result = read_file_env("temp_file_with_placeholders", deploy_env=None)
    expected = "Environment: {env}\nPNP Environment: {pnp_env}\nOther content"
    assert result == expected


def test_read_file_env_deploy_env_empty_string(mocker):
    """Test read_file_env with deploy_env as empty string."""
    prepare_mock_file(mocker, "Environment: {env}\nPNP Environment: {pnp_env}\nOther content")
    result = read_file_env("temp_file_with_placeholders", deploy_env="")
    expected = "Environment: \nPNP Environment: prod\nOther content"
    assert result == expected


@patch('util.miscutils.logger')
def test_read_file_env_file_not_exists_logging(mock_logger):
    """Test that read_file_env logs appropriate message when file doesn't exist."""
    non_existent_file = "/path/to/non/existent/file.txt"

    with pytest.raises(AirflowFailException):
        read_file_env(non_existent_file)

    mock_logger.info.assert_called_once_with(f'The file {non_existent_file} doesnt exist')


def test_get_pnp_env():
    assert 'nonprod' == get_pnp_env('dev')
    assert 'nonprod' == get_pnp_env('uat')
    assert 'prod' == get_pnp_env('prod')


# ============================================================================
# Tests for get_serverless_cluster_config function
# ============================================================================

@pytest.mark.parametrize("deploy_env,expected_subnetwork_uri", [
    ("dev", "https://www.googleapis.com/compute/v1/projects/pcb-nonprod-svpc/regions/northamerica-northeast1/subnetworks/snet-pcbnp-app01-dev"),
    ("uat", "https://www.googleapis.com/compute/v1/projects/pcb-nonprod-svpc/regions/northamerica-northeast1/subnetworks/snet-pcbnp-app01-uat"),
    ("nonprod", "https://www.googleapis.com/compute/v1/projects/pcb-nonprod-svpc/regions/northamerica-northeast1/subnetworks/snet-pcbnp-app01-nonprod"),
])
def test_get_serverless_cluster_config_nonprod_environments(deploy_env, expected_subnetwork_uri):
    """
    Test get_serverless_cluster_config for non-prod environments.

    Verifies that non-prod environments use the default subnetwork URI with {env} replaced.
    """
    network_tag = "test-network-tag"
    subnetwork_uri, network_tags, service_account = get_serverless_cluster_config(deploy_env, network_tag)

    assert subnetwork_uri == expected_subnetwork_uri
    assert network_tags == [network_tag, "processing"]
    assert service_account == f"dataproc@pcb-{deploy_env}-processing.iam.gserviceaccount.com"


def test_get_serverless_cluster_config_prod():
    """
    Test get_serverless_cluster_config for prod environment.

    Verifies that prod uses the dedicated prod subnetwork URI.
    """
    deploy_env = "prod"
    network_tag = "prod-network-tag"
    subnetwork_uri, network_tags, service_account = get_serverless_cluster_config(deploy_env, network_tag)

    expected_subnetwork_uri = "https://www.googleapis.com/compute/v1/projects/pcb-prod-svpc/regions/northamerica-northeast1/subnetworks/snet-pcbpr-app01-nane1"
    assert subnetwork_uri == expected_subnetwork_uri
    assert network_tags == [network_tag, "processing"]
    assert service_account == "dataproc@pcb-prod-processing.iam.gserviceaccount.com"


@pytest.mark.parametrize("network_tag", [
    "pcb-network",
    "custom-tag",
    "test-tag-123",
])
def test_get_serverless_cluster_config_network_tags(network_tag):
    """
    Test get_serverless_cluster_config with different network tags.

    Verifies that network_tags always includes the provided tag and "processing".
    """
    deploy_env = "dev"
    subnetwork_uri, network_tags, service_account = get_serverless_cluster_config(deploy_env, network_tag)

    assert len(network_tags) == 2
    assert network_tags[0] == network_tag
    assert network_tags[1] == "processing"
    assert service_account == f"dataproc@pcb-{deploy_env}-processing.iam.gserviceaccount.com"


def test_get_serverless_cluster_config_service_account_format():
    """
    Test get_serverless_cluster_config service account format.

    Verifies that service account follows the expected format: dataproc@pcb-{env}-processing.iam.gserviceaccount.com
    """
    test_cases = [
        ("dev", "dataproc@pcb-dev-processing.iam.gserviceaccount.com"),
        ("uat", "dataproc@pcb-uat-processing.iam.gserviceaccount.com"),
        ("prod", "dataproc@pcb-prod-processing.iam.gserviceaccount.com"),
    ]

    for deploy_env, expected_service_account in test_cases:
        _, _, service_account = get_serverless_cluster_config(deploy_env, "test-tag")
        assert service_account == expected_service_account


def test_get_serverless_cluster_config_return_type():
    """
    Test get_serverless_cluster_config return type.

    Verifies that the function returns a tuple with correct types.
    """
    subnetwork_uri, network_tags, service_account = get_serverless_cluster_config("dev", "test-tag")

    assert isinstance(subnetwork_uri, str)
    assert isinstance(network_tags, list)
    assert isinstance(service_account, str)
    assert len(network_tags) == 2
    assert all(isinstance(tag, str) for tag in network_tags)
