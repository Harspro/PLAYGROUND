import json
import logging
import os
import re
from copy import deepcopy
from datetime import timedelta, datetime
import traceback
from typing import Optional

try:
    import yaml
except Exception:
    yaml = None

try:
    from airflow import settings as airflow_settings
    from airflow.exceptions import AirflowFailException, AirflowNotFoundException, AirflowException
    from airflow.hooks.base import BaseHook
    from airflow.models import Connection, Variable
except Exception:
    airflow_settings = None
    settings = None

    class AirflowFailException(Exception):
        pass

    class AirflowNotFoundException(Exception):
        pass

    class AirflowException(Exception):
        pass

    BaseHook = None
    Connection = None
    Variable = None
else:
    settings = airflow_settings

try:
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound
    from google.cloud import storage
    from google.protobuf.duration_pb2 import Duration
except Exception:
    bigquery = None
    storage = None

    class NotFound(Exception):
        pass

    class Duration:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise RuntimeError("google-cloud dependencies are required for Duration usage.")

import struct
import tempfile
import zlib

import util.constants as consts
from util.constants import (
    DEFAULT_DLAAS_SPARK_JOB,
    CLUSTER_SUFFIX,
    MAX_LENGTH,
    ALM_VAULT_NAME,
    PROD_ENV,
    NONPROD_ENV,
    NONPROD_ENV_LIST,
    ENV_PLACEHOLDER,
    PNP_ENV_PLACEHOLDER,
)
from util.vault_util import VaultUtilBuilder

DAGS_FOLDER = (
    airflow_settings.DAGS_FOLDER
    if airflow_settings is not None
    else os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

ENV_FILENAME_PLACEHOLDER = '{file_env}'
JOB_HISTORY_DATASET = "JobControlDataset"
JOB_HISTORY_TABLE = "DAGS_HISTORY"
ENV_SUFFIX_PLACEHOLDER = "{env_suffix}"

# Data Freshness Constants
DATA_FRESHNESS_PREFIX = "[DATA_FRESHNESS]"
DATA_FRESHNESS_DATASET = consts.DOMAIN_DATA_LOGS_DATASET_ID
DATA_FRESHNESS_TABLE = "DATA_FRESHNESS"

logger = logging.getLogger(__name__)

MACHINE_ALLOCATION = {
    "dev": {
        "primary-machine-type": "e2-highmem-16",
        "secondary-machine-type": "e2-highmem-16"
    },
    "uat": {
        "primary-machine-type": "n1-highmem-16",
        "secondary-machine-type": "n1-standard-16"
    },
    "prod": {
        "primary-machine-type": "n2-highmem-16",
        "secondary-machine-type": "n2-standard-16"
    }
}


def create_airflow_connection(conn_id: str, conn_type: str, description: str, host: str, login: str,
                              vault_password_secret_path: str, vault_password_secret_key: str,
                              schema: str, port: str) -> None:
    """
    Function to create an Airflow connection.

    :param conn_id: The connection ID.
    :type conn_id: str

    :param conn_type: The connection type.
    :type conn_type: str

    :param description: The connection description.
    :type description: str

    :param host: The host.
    :type host: str

    :param login: The login.
    :type login: str

    :param vault_password_secret_path: Vault password secret path.
    :type vault_password_secret_path: str

    :param vault_password_secret_key: Vault password secret key.
    :type vault_password_secret_key: str

    :param schema: The DB schema.
    :type schema: str

    :param port: The port number.
    :type port: str

    :returns: None
    :rtype: None

    """
    if airflow_settings is None or Connection is None:
        raise AirflowFailException("Airflow is required to create connections.")
    if not airflow_connection_exists(conn_id):
        password = VaultUtilBuilder.build().get_secret(
            vault_password_secret_path,
            vault_password_secret_key
        )
        conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            description=description,
            host=host,
            login=login,
            password=password,
            schema=schema,
            port=port
        )

        session = airflow_settings.Session()
        session.add(conn)
        session.commit()


def get_smb_server_config(config_path, look_up: str, deploy_env: str):
    """
    Retrieves the SMB server configuration based on the environment and lookup key from a YAML configuration file.This function loads the SMB configuration from a specified YAML file, traverses the configuration using a `look_up` key, and accesses the environment-specific settings. It fetches the SMB server's password securely using a Vault utility and returns the connection details.
    :param config_path: The path to the YAML configuration file. If not provided, a default path is used.
    :type config_path: str
    :param look_up: The key used to navigate within the YAML configuration to locate the required settings.
    :type look_up: str
    :param deploy_env: deployment environment (nonprod, prod)
    :type deploy_env: str
    """
    smb_config = read_yamlfile_env(config_path)
    if look_up not in smb_config:
        raise AirflowFailException(f"The lookup key '{look_up}' is not found")
    smb_server_config = smb_config[look_up]
    if deploy_env not in smb_server_config:
        raise AirflowFailException(f"The environment '{deploy_env}' is not found")
    return resolve_smb_server_config(smb_server_config[deploy_env], ALM_VAULT_NAME)


def resolve_smb_server_config(smb_server_config: dict, vault_name=None):
    vault_util = VaultUtilBuilder.build()
    if not vault_util:
        logger.warning("failed to build vault util.")
        return None

    password = vault_util.get_secret(smb_server_config.get(consts.SECRET_PATH), smb_server_config.get(consts.SECRET_KEY), vault_name)
    return smb_server_config.get(consts.HOSTNAME), smb_server_config.get(consts.USERNAME), password


def airflow_connection_exists(conn_id: str) -> bool:
    """
    Checks if a connection with the given `conn_id` already exists.

    :param conn_id: The unique identifier for the connection.
    :type conn_id: str

    :return: True if the connection exists, False otherwise.
    :rtype: bool
    """
    if BaseHook is None:
        raise AirflowFailException("Airflow is required to check connections.")
    try:
        return BaseHook.get_connection(conn_id) is not None
    except AirflowNotFoundException:
        logger.info(f"Airflow Connection {conn_id} does not exist. Moving forward with creating connection.")
        return False


def read_env_filepattern(pattern, deploy_env):
    """
    Replaces the file name for different environments accordingly:
    Example: For uat "filename_yyyymmdd.{file_env}" becomes "filename_yyyymm_dd.uatv"

    :return - updated pattern
    """

    ENV_FILEPATTERN_MAPPING = {'dev': 'uatv',
                               'uat': 'uatv',
                               'prod': 'prod'}

    return pattern.replace("{file_env}", ENV_FILEPATTERN_MAPPING[deploy_env])


def replace_all_env_in_dict(config: dict, deploy_env) -> dict:
    json_string = json.dumps(config)
    json_string = json_string.replace('{env}', deploy_env)
    new_dict = json.loads(json_string)
    return new_dict


def read_variable_or_file(var_str: str, deploy_env=None) -> dict:
    if Variable is not None:
        var_dict = Variable.get(var_str, deserialize_json=True, default_var={})
    else:
        var_dict = {}

    if var_dict and len(var_dict) > 0:
        pass
    else:
        # logging.info(f'settings dagsf: {DAGS_FOLDER}')
        var_str_filename = f'{DAGS_FOLDER}/{var_str}.json'

        if os.path.exists(var_str_filename):
            with open(var_str_filename, 'r') as var_str_file:
                content = var_str_file.read()
                if deploy_env is not None and ENV_PLACEHOLDER in content:
                    content = content.replace(ENV_PLACEHOLDER, deploy_env)
                var_dict = json.loads(content)
        else:
            logger.info(f'{var_str_filename} doesnt exist')

    return var_dict


def get_pnp_env(deploy_env: str) -> str:
    return NONPROD_ENV if deploy_env in NONPROD_ENV_LIST else PROD_ENV


def read_file_env(fname: str, deploy_env=None) -> Optional[str]:
    if os.path.exists(fname):
        logger.info(f'reading file: file name={fname}, deploy env={deploy_env}')

        with open(fname, 'r') as fp:
            content = fp.read()
            # We need to check 'deploy_env is not None' here.
            # There are some mis-use where storage suffix is passed in as deploy_env.
            # Future clean up is needed.
            if deploy_env is not None:
                if ENV_PLACEHOLDER in content:
                    content = content.replace(ENV_PLACEHOLDER, deploy_env)
                if PNP_ENV_PLACEHOLDER in content:
                    content = content.replace(PNP_ENV_PLACEHOLDER, get_pnp_env(deploy_env))
            return content
    else:
        logger.info(f'The file {fname} doesnt exist')
        raise AirflowFailException(f'The file {fname} doesnt exist')


def read_yamlfile_env(fname: str, deploy_env=None):
    if yaml is None:
        raise AirflowFailException("pyyaml is required to read YAML files.")
    if os.path.exists(fname):
        content = read_file_env(fname, deploy_env)
        mappings = yaml.safe_load(content)
        return mappings
    else:
        logger.info(f'{fname} doesnt exist')
        return None


def read_yamlfile_env_suffix(fname: str, deploy_env=None, env_suffix=None):
    """
    This is to read a yaml file which has env name and env suffix both present:
    Example:
        - pcb-{env}-staging-artifacts : Use {env} [dev, uat, prod]
        - verafin-outbound-pcb-{env} : Use {env_suffix} [-dev, -uat, '']
    """
    if yaml is None:
        raise AirflowFailException("pyyaml is required to read YAML files.")
    if os.path.exists(fname):
        with open(fname, 'r') as yaml_fp:
            content = yaml_fp.read()
            if deploy_env is not None and ENV_PLACEHOLDER in content:
                content = content.replace(ENV_PLACEHOLDER, deploy_env)
            if env_suffix is not None and ENV_SUFFIX_PLACEHOLDER in content:
                content = content.replace(ENV_SUFFIX_PLACEHOLDER, env_suffix)
            mappings = yaml.safe_load(content)
            return mappings
    else:
        logger.info(f'{fname} doesnt exist')
        return None


def get_default_dlaas_spark_job(deploy_env=None):
    ret_dict = deepcopy(DEFAULT_DLAAS_SPARK_JOB)
    for k, v in ret_dict.items():
        type_of_val = type(v)
        if type_of_val == list:
            for idx, item in enumerate(v):
                v[idx] = item.replace(ENV_PLACEHOLDER, deploy_env)
        elif type_of_val == str:
            ret_dict[k] = v.replace(ENV_PLACEHOLDER, deploy_env)
    return ret_dict


def get_cluster_name(is_ephemeral: bool, dataproc_config: dict, dag_id: Optional[str] = None) -> str:
    if is_ephemeral:
        if dag_id:
            # Generate unique cluster name based on dag_id for ephemeral DAGs
            name = get_cluster_name_for_dag(dag_id)
        else:
            # Fallback to shared cluster name if dag_id is not provided (backward compatibility)
            name = "mdpc-ephemeral"
    else:
        name = dataproc_config.get('dataproc-cluster-name')
    return name


def get_cluster_name_for_dag(dag_id: str) -> str:
    name = dag_id.replace("_", "-") + CLUSTER_SUFFIX

    if len(name) > MAX_LENGTH:
        head = dag_id[:15]
        tail = dag_id[16:]
        shortened_tail = re.sub(r'[AEIOU]', '', tail, flags=re.IGNORECASE)
        name = f'{head}{shortened_tail}'.replace("_", "-")[:MAX_LENGTH - 2] + CLUSTER_SUFFIX

    return name.lower()


def _get_num_workers(job_size: str = "extra_small") -> int:
    if job_size is None:
        num_worker = consts.DATAPROC_CLUSTER_SIZES["extra_small"]
    elif job_size == "extra_small":
        num_worker = consts.DATAPROC_CLUSTER_SIZES["extra_small"]
    elif job_size == "small":
        num_worker = consts.DATAPROC_CLUSTER_SIZES["small"]
    elif job_size == "medium":
        num_worker = consts.DATAPROC_CLUSTER_SIZES["medium"]
    elif job_size == "large":
        num_worker = consts.DATAPROC_CLUSTER_SIZES["large"]
    else:
        num_worker = consts.DATAPROC_CLUSTER_SIZES["extra_small"]
    return num_worker


def get_cluster_config_by_job_size(deploy_env: str, network_tag: str, job_size: str = "extra_small", is_sqlserver=None):
    return get_ephemeral_cluster_config(deploy_env, network_tag, _get_num_workers(job_size), is_sqlserver)


def get_ephemeral_cluster_config(deploy_env: str, network_tag: str, num_worker=16, is_sqlserver=None) -> dict:
    subnetwork_uri_default = "https://www.googleapis.com/compute/v1/projects/pcb-nonprod-svpc/regions/northamerica-northeast1/subnetworks/snet-pcbnp-app01-{env}"
    subnetwork_uri_prod = "https://www.googleapis.com/compute/v1/projects/pcb-prod-svpc/regions/northamerica-northeast1/subnetworks/snet-pcbpr-app01-nane1"
    subnetwork_uri = subnetwork_uri_default
    if deploy_env == "prod":
        subnetwork_uri = subnetwork_uri_prod

    enable_conscrypt = "false" if is_sqlserver else "true"

    machine_types = MACHINE_ALLOCATION.get(deploy_env)
    if not machine_types:
        machine_types = MACHINE_ALLOCATION.get('dev')

    machine_type = machine_types.get(consts.PRIMARY_MACHINE_TYPE)

    # num_worker can be jinja template too
    if (type(num_worker) is int) and (num_worker < consts.DATAPROC_CLUSTER_SIZES["medium"]):   # small and extra small workload
        machine_type = machine_types.get(consts.SECONDARY_MACHINE_TYPE)

    cluster_config_tmpl = {
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": machine_type,
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024}
        },
        "worker_config": {
            "num_instances": num_worker,
            "machine_type_uri": machine_type,
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024}
        },
        "software_config": {
            "image_version": "2.2.21-debian12",
            "properties": {
                "dataproc:dataproc.logging.stackdriver.job.driver.enable": "true",
                "dataproc:dataproc.logging.stackdriver.enable": "true",
                "dataproc:jobs.file-backed-output.enable": "true",
                "dataproc:dataproc.logging.stackdriver.job.yarn.container.enable": "true",
                "dataproc:dataproc.monitoring.stackdriver.enable": "true",
                "dataproc:dataproc.conscrypt.provider.enable": enable_conscrypt,
                "spark:spark.dynamicAllocation.enabled": "true",
                "dataproc:dataproc.optional.components": " "
            }
        },
        "gce_cluster_config": {
            "service_account": "dataproc@pcb-{env}-processing.iam.gserviceaccount.com",
            "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
            "subnetwork_uri": subnetwork_uri,
            "internal_ip_only": True,
            "tags": [network_tag, "processing"]
        },
        "endpoint_config": {
            "enable_http_port_access": True
        },
    }

    cfg_str = json.dumps(cluster_config_tmpl)

    if deploy_env is not None and ENV_PLACEHOLDER in cfg_str:
        cfg_str = cfg_str.replace(ENV_PLACEHOLDER, deploy_env)

    cluster_config = json.loads(cfg_str)

    cluster_config["lifecycle_config"] = {
        "idle_delete_ttl": Duration(seconds=600)
    }

    return cluster_config


def save_job_to_control_table(job_params, load_timestamp=None, **context):
    dag_id = context['dag'].dag_id
    dag_run_id = context['run_id']
    gcp_config = read_variable_or_file(consts.GCP_CONFIG)

    dags_hist_table_id = (f"{gcp_config.get('curated_zone_project_id')}."
                          f"{JOB_HISTORY_DATASET}."
                          f"{JOB_HISTORY_TABLE}")

    logger.info(f"dag_id: {dag_id}, dag_run_id: {dag_run_id}, args: {job_params}")
    logger.info(f'{dags_hist_table_id}')

    client = bigquery.Client()
    try:
        client.get_table(dags_hist_table_id)
        logger.info(f"History table {dags_hist_table_id} already exists")
    except NotFound:
        logger.info("History table not found. Creating...")
        CREATE_TABLE_DDL = f"""
            CREATE TABLE IF NOT EXISTS
              `{dags_hist_table_id}`
            (
              load_timestamp TIMESTAMP NOT NULL OPTIONS(description='Timestamp of job history update'),
              dag_id STRING NOT NULL OPTIONS(description='ID (name) of the dag'),
              dag_run_id STRING OPTIONS(description='Run ID of the dag'),
              job_params STRING OPTIONS(description='Parameters from the dag run')
            )
            OPTIONS(
              description='Table for storing dags run history'
            );
        """
        logger.info(CREATE_TABLE_DDL)
        create_job = client.query(CREATE_TABLE_DDL)
        create_job.result()
    if load_timestamp:
        logger.info(f"dag_id: {dag_id}, dag_run_id: {dag_run_id}, args: {job_params}, load_timestamp: {load_timestamp}")
        APPEND_DML = f"""
            INSERT INTO `{dags_hist_table_id}`
            VALUES( "{load_timestamp}", "{dag_id}", "{dag_run_id}", '''{job_params}''');
        """
    else:
        logger.info(f"dag_id: {dag_id}, dag_run_id: {dag_run_id}, args: {job_params}")
        APPEND_DML = f"""
            INSERT INTO `{dags_hist_table_id}`
            VALUES( CURRENT_TIMESTAMP(), "{dag_id}", "{dag_run_id}", '''{job_params}''');
        """
    logger.info(APPEND_DML)
    append_job = client.query(APPEND_DML)
    append_job.result()

    # Track data freshness after successful control table update
    try:
        tables_info = context.get('tables_info')
        if tables_info is not None:
            track_data_freshness(dag_id, tables_info)
        else:
            logger.warning("Data freshness tracking skipped: tables_info not provided in context")
    except Exception as e:
        logger.warning(f"Data freshness tracking failed in save_job_to_control_table: {str(e)}")
        # Continue execution - data freshness tracking failure should not break the main job


def get_dagrun_last(dag_id: str, filter: str = ""):
    from util.bq_utils import run_bq_query  # Lazy import to avoid circular dependency
    gcp_config = read_variable_or_file(consts.GCP_CONFIG)
    dags_hist_table_id = (f"{gcp_config.get('curated_zone_project_id')}."
                          f"{JOB_HISTORY_DATASET}."
                          f"{JOB_HISTORY_TABLE}")
    logger.info(f'get_dagrun_last: {dags_hist_table_id}')

    QUERY_SQL = f"""
        SELECT * FROM
          `{dags_hist_table_id}`
        WHERE
            dag_id = '{dag_id}'
            {filter}
        ORDER BY load_timestamp DESC
        LIMIT 1;
    """
    logger.info(f'Fetching last logged dag run for {dag_id} using query : {QUERY_SQL}')
    query_job = run_bq_query(QUERY_SQL)
    return query_job.result()


def get_dagruns_after(dag_id: str, atimestamp):
    gcp_config = read_variable_or_file(consts.GCP_CONFIG)
    dags_hist_table_id = (f"{gcp_config.get('curated_zone_project_id')}."
                          f"{JOB_HISTORY_DATASET}."
                          f"{JOB_HISTORY_TABLE}")
    logger.info(f'get_dagruns_after: {dags_hist_table_id}')
    client = bigquery.Client()
    QUERY_SQL = f"""
        SELECT * FROM
          `{dags_hist_table_id}`
        WHERE load_timestamp > '{atimestamp}'
            AND dag_id = '{dag_id}'
        ORDER BY load_timestamp ASC
    """
    query_job = client.query(QUERY_SQL)
    return query_job.result()


def get_previous_month_details(prefix_date: str, dag_config: dict):
    prefix_date_obj = datetime.strptime(prefix_date, "%Y-%m-%d")
    previous_month_date = prefix_date_obj - timedelta(days=prefix_date_obj.day)
    previous_year = previous_month_date.strftime("%Y")
    previous_month = previous_month_date.strftime("%m")
    prefix = f"{dag_config.get(consts.FILE_PREFIX)}/{dag_config.get(consts.FILE_PREFIX)}_{previous_year}{previous_month:2}"
    return prefix


def sanitize_string(s: str) -> str:
    return re.sub("\r\n|\n|\r|\t| ", "", s)


def split_table_name(table_name_config: str, return_schema_only: bool = False):
    base_part = table_name_config.split(" ")[0]
    split_base = base_part.split(".", 2)

    if len(split_base) == 3:
        # For SQLSERVER, db.schema.table format
        source_schema, tablename = split_base[1], split_base[2]
    elif len(split_base) == 2:
        # For ORACLE, schema.table format
        source_schema, tablename = split_base
    else:
        raise ValueError("Invalid table name format.")

    tablename = re.sub('[^0-9a-zA-Z]+', '_', tablename)
    remaining_query = table_name_config[len(base_part):].strip()

    if remaining_query:
        remaining_query = re.sub('[^0-9a-zA-Z]+', '_', remaining_query)
        source_tablename = f"{tablename}_{remaining_query}"
    else:
        source_tablename = tablename

    if return_schema_only:
        return source_schema
    return source_schema, source_tablename


def compose_infinite_files_into_one(source_config: dict, dest_config: dict, win_compatible: bool,
                                    replace_empty_with_blank: bool):
    """
        This method compose multiple files from source folder, and compose into destination folder.
        While writing to destination, it ensures if windows_compatible flag is set to True then <LF> if replaced
        with <CR><LF> which is windows way of defining new lines
        It also ensures that if replace_empty_with_blank is set to True then empty string will be replaced with blanks
        It also ensures that the headers from multiple files are all not written, instead only from first file is
        written to dest file.

        :param source_config: Json Config which contains source [bucket, folder_prefix and matching glob]
        :type source_config: :class:`dict`

        :param dest_config: Json Config which contains source [bucket, folder_prefix and filename]
        :type dest_config: :class:`dict`

        :param win_compatible: Boolean flag for windows compatibility in terms of LF and CR
        :type win_compatible: :class:`Boolean`

        :param replace_empty_with_blank: Boolean flag for replacing empty string with blanks
        :type replace_empty_with_blank: :class:`Boolean`
    """
    logger.info(f"source config: {source_config}")
    logger.info(f"dest config: {dest_config}")
    if None in (source_config, dest_config):
        raise AirflowFailException("please provide source and dest config.")

    src_bucket = source_config["bucket"]
    src_folder_prefix = source_config["folder_prefix"]
    src_file_prefix = source_config["file_prefix"]

    # Get the blobs in a given bucket / folder
    storage_client = storage.Client()
    src_blobs = storage_client.list_blobs(src_bucket, prefix=src_folder_prefix)
    if not src_blobs:
        raise AirflowFailException(f"No objects to compose in {src_bucket}/{src_folder_prefix}")

    # Filtering the blobs with matching filename
    source_filtered_blobs = [src_blob for src_blob in src_blobs if src_file_prefix in src_blob.name]

    if not source_filtered_blobs:
        raise AirflowFailException(f"No filtered source objects to compose in {src_bucket}/{src_folder_prefix}/{src_file_prefix}")

    dest_bucket = storage_client.bucket(dest_config["bucket"])
    dest_blob = dest_bucket.blob(f"{dest_config['folder_prefix']}/{dest_config['file_name']}")

    try:
        with dest_blob.open(mode="wb") as outputFile:
            first_file_processed = False
            for blob in source_filtered_blobs:
                with blob.open(mode="rb") as inputFile:
                    # skipping header for all files but first. Any file could be empty so skipping those
                    first_line = next(inputFile, None)
                    if first_line is None:
                        logger.info(f"Blob {blob.name} is empty. Skipping this file....")
                        continue
                    if not first_file_processed:
                        outputFile.write(_format_string(first_line, win_compatible, replace_empty_with_blank))
                    for line in inputFile:
                        # making it windows compatible, replace empty string with blanks
                        line = _format_string(line, win_compatible, replace_empty_with_blank)
                        outputFile.write(line)
                    first_file_processed = True
    except Exception as ex:
        stack_trace = traceback.format_exc()
        logging.error(f"Error writing to destination blob {dest_blob.name}: {stack_trace}")
        raise AirflowFailException(f"Failed to write to destination blob {dest_blob.name}: {str(ex)}")


def _format_string(input_str: str, windows_compatible: bool, replace_empty_with_blank: bool):
    if windows_compatible:
        input_str = input_str.replace(b"\n", b"\r\n")
    if replace_empty_with_blank:
        input_str = input_str.replace(b'\"\"', b'').replace(b"\'\'", b'')
    return input_str


def _split_string(input_str: str, pattern: str):
    regex_pattern = "|".join(map(re.escape, pattern))
    splitted_strings = re.split(regex_pattern, input_str)
    return splitted_strings


def generate_bq_insert_from_filename(source_config: dict, dest_config: dict, **context):
    from util.bq_utils import run_bq_query_inline, read_sql_file
    """
        This method reads a file name, splits it based on a specified pattern, generates an SQL insert statement, and executes the statement.

        :param source_config: Json Config which contains source [project, bucket, folder_prefix and str_split_at_pattern]
        :type source_config: :class:`dict`

        :param dest_config: Json Config which contains source [table_name]
        :type dest_config: :class:`dict`
    """
    logger.info(f"source config: {source_config} and dest config: {dest_config}")

    if None in (source_config, dest_config):
        raise AirflowFailException("please provide source and dest config.")

    src_gcp_project = source_config["project"]
    src_gcp_bucket = source_config["bucket"]
    src_gcp_folder = source_config["folder_prefix"]
    pattern = source_config["str_split_at_pattern"]
    execution_datetime = source_config["execution_datetime"]

    # Get the blobs in a given bucket / folder
    try:
        storage_client = storage.Client(project=src_gcp_project)
        src_blobs = storage_client.list_blobs(src_gcp_bucket, prefix=src_gcp_folder)
    except Exception as e:
        logger.warning(f"Failed to access bucket with project {src_gcp_project}, trying without project specification: {e}")
        storage_client = storage.Client()
        src_blobs = storage_client.list_blobs(src_gcp_bucket, prefix=src_gcp_folder)
    if not src_blobs:
        raise AirflowFailException(f"No source object:- {src_gcp_bucket}/{src_gcp_folder}")

    # Filter out folder entries (entries ending with '/') and get only actual files
    source_filtered_blobs = [blob.name for blob in src_blobs if not blob.name.endswith('/') and "__MACOSX" not in blob.name]

    if not source_filtered_blobs:
        raise AirflowFailException(f"No file present:- {src_gcp_bucket}/{src_gcp_folder}")

    logger.info(f"Processing {len(source_filtered_blobs)} file(s)")

    dest_gcp_table = dest_config['table_name']
    sql_file_path = dest_config['sql_file_path']

    dag_id = context[consts.DAG].dag_id
    variables = {}

    # Read the sql file and put the content in a vatiable
    sql_file_contents = read_sql_file(sql_file_path)
    logger.info(f'Content of sql file:- {sql_file_contents}')

    sql_replacement_dict = {
        '{src_gcp_project}': src_gcp_project,
        '{src_gcp_bucket}': src_gcp_bucket,
        '{src_gcp_folder}': src_gcp_folder,
        '{dag_id}': dag_id,
        '{execution_datetime}': execution_datetime,
        '{dest_gcp_table}': dest_gcp_table
    }

    for blob in source_filtered_blobs:
        file_name = os.path.basename(blob)

        # Skip if file_name is empty (shouldn't happen with our filter, but safety check)
        if not file_name:
            logger.warning(f"Skipping blob with empty filename: {blob}")
            continue

        splitted_strings = _split_string(file_name, pattern)
        sql_file_contents_staging = sql_file_contents

        sql_replacement_dict['{file_name}'] = file_name

        for i, match in enumerate(splitted_strings, 0):
            variables[f'str_position_{i}'] = splitted_strings[i]
            sql_file_contents_staging = sql_file_contents_staging.replace(f"{{string_position_{i}}}", variables[f'str_position_{i}'])

        for k, v in sql_replacement_dict.items():
            sql_file_contents_staging = sql_file_contents_staging.replace(k, v)

        run_bq_query_inline(sql_file_contents_staging)


def get_file_date_parameter(dag_id: str, dag_config: dict, **context):
    bigquery_client = bigquery.Client()
    bigquery_config = dag_config.get(consts.BIGQUERY)
    bq_processing_project_name = bigquery_config.get(consts.PROJECT_ID)
    bq_dataset_name = bigquery_config.get(consts.DATASET_ID)
    file_date_column = dag_config.get("file_date_column")
    bq_table = bigquery_config.get("tables").get("TRLR").get("table_name")

    create_date_sql = f"""
        SELECT MAX({file_date_column}) AS {file_date_column}
        FROM {bq_processing_project_name}.{bq_dataset_name}.{bq_table}
    """
    logging.info(f"Extracting file_create_dt using SQL: {create_date_sql}")
    create_dt_result = bigquery_client.query(create_date_sql).result().to_dataframe()
    file_date_value = create_dt_result[file_date_column].values[0]

    if file_date_value:
        logging.info(f"File create date for this execution is {file_date_value}")
        context['ti'].xcom_push(key='file_create_dt_oracle', value=str(file_date_value))
    else:
        raise AirflowException(f"file_create_dt is not found for {dag_id}")


def create_data_freshness_table_if_not_exist(client, data_freshness_table_id: str):
    """Create data freshness table if it doesn't exist"""
    try:
        client.get_table(data_freshness_table_id)
        logger.info(f"{DATA_FRESHNESS_PREFIX} Freshness table {data_freshness_table_id} already exists")
    except NotFound:
        logger.info(f"{DATA_FRESHNESS_PREFIX} Freshness table not found. Creating...")
        create_table_ddl = f"""
            CREATE TABLE IF NOT EXISTS
              `{data_freshness_table_id}`
            (
              project_id STRING OPTIONS(description='Project ID of the table'),
              dataset_id STRING OPTIONS(description='Dataset ID of the table'),
              table_name STRING OPTIONS(description='Table name'),
              last_loaded_time DATETIME OPTIONS(description='Datetime when table was last loaded (Toronto timezone)'),
              loaded_by STRING OPTIONS(description='DAG ID that loaded the table')
            )
            CLUSTER BY project_id, dataset_id, table_name
            OPTIONS(
              description='Table for tracking data freshness in landing zone'
            );
        """
        logger.info(f"{DATA_FRESHNESS_PREFIX} {create_table_ddl}")
        client.query(create_table_ddl).result()


def track_data_freshness(dag_id: str, tables_info: list):
    """Track data freshness for curated zone tables"""
    gcp_config = read_variable_or_file(consts.GCP_CONFIG)

    if not tables_info or not gcp_config or not dag_id:
        missing = []
        if not tables_info:
            missing.append("table information")
        if not gcp_config:
            missing.append("GCP config")
        if not dag_id:
            missing.append("DAG ID")
        logger.warning(f"{DATA_FRESHNESS_PREFIX} Data freshness tracking skipped: Missing {', '.join(missing)}")
        return

    data_freshness_table_id = (f"{gcp_config.get('landing_zone_project_id')}."
                               f"{DATA_FRESHNESS_DATASET}."
                               f"{DATA_FRESHNESS_TABLE}")

    logger.info(f"{DATA_FRESHNESS_PREFIX} Tracking {len(tables_info)} table(s) freshness - DAG: {dag_id}")

    try:
        client = bigquery.Client()
        # Create freshness table if it doesn't exist
        create_data_freshness_table_if_not_exist(client, data_freshness_table_id)

        # Update freshness for each table
        for table_info in tables_info:
            if 'dataset_id' in table_info and 'table_name' in table_info:
                # Get project_id with fallback logic
                project_id = table_info.get('project_id')
                if not project_id:
                    # Fallback to landing_zone_project_id if not specified in YAML
                    project_id = gcp_config.get('landing_zone_project_id')
                    logger.info(f"{DATA_FRESHNESS_PREFIX} Using fallback project_id: {project_id} (from landing_zone_project_id)")
                else:
                    logger.info(f"{DATA_FRESHNESS_PREFIX} Using project_id from YAML: {project_id}")

                dataset_id = table_info['dataset_id']
                table_name = table_info['table_name']

                merge_dml = f"""
                    MERGE `{data_freshness_table_id}` AS target
                    USING (SELECT "{project_id}" as project_id, "{dataset_id}" as dataset_id, "{table_name}" as table_name,
                                  CURRENT_DATETIME('America/Toronto') as last_loaded_time, "{dag_id}" as loaded_by) AS source
                    ON target.project_id = source.project_id AND target.dataset_id = source.dataset_id AND target.table_name = source.table_name
                    WHEN MATCHED THEN UPDATE SET last_loaded_time = source.last_loaded_time, loaded_by = source.loaded_by
                    WHEN NOT MATCHED THEN INSERT (project_id, dataset_id, table_name, last_loaded_time, loaded_by)
                    VALUES (source.project_id, source.dataset_id, source.table_name, source.last_loaded_time, source.loaded_by)
                """
                logger.info(f"{DATA_FRESHNESS_PREFIX} {merge_dml}")
                client.query(merge_dml).result()
                logger.info(f"{DATA_FRESHNESS_PREFIX} Updated freshness: {project_id}.{dataset_id}.{table_name}")
            else:
                logger.warning(f"{DATA_FRESHNESS_PREFIX} Data freshness tracking skipped invalid table info: {table_info} (missing dataset_id or table_name)")

    except Exception as e:
        logger.warning(f"{DATA_FRESHNESS_PREFIX} Data freshness tracking failed: {str(e)}")


def generate_and_upload_png(source_config: dict, dest_config: dict):

    """
        Generates a png file and uploads it to the Google Cloud Storage.

        :param source_config: Configuration for PNG generation containing:
        - png_file_name: Name for the PNG file (required)
        - png_content: Content to store as GCS metadata (required)
        - width: PNG width in pixels (optional, default: 320)
        - height: PNG height in pixels (optional, default: 100)
        - colour: RGB color as list [R, G, B] (optional, default: [255, 255, 255])

        Example source_config:
        {
        "png_file_name": "sample.png",
        "png_content": "Hello, this is a sample png file.",
        "width": 400,
        "height": 200,
        "colour": [255, 255, 255]
        }

        :param dest_config: GCS upload configuration containing:
        - target_gcp_bucket: GCS bucket name (required)
        - target_gcp_path: GCS object path (required)

        Example dest_config:
        {
         "target_gcp_bucket": "my-env-staging-extract",
         "target_gcp_path": "upload/sample.png"
        }

    """

    logger.info(f"source_config: {source_config}")
    logger.info(f"dest_config: {dest_config}")

    # Validate required fields
    required_source_fields = ["png_file_name", "png_content"]
    missing_source_fields = [field for field in required_source_fields if field not in source_config or source_config[field] is None]

    if missing_source_fields:
        raise AirflowFailException(f"Missing required source fields: {missing_source_fields}")

    # Validate required fields
    required_dest_fields = ["target_gcp_bucket", "target_gcp_path"]
    missing_dest_fields = [field for field in required_dest_fields if field not in dest_config or dest_config[field] is None]

    if missing_dest_fields:
        raise AirflowFailException(f"Missing required dest fields: {missing_dest_fields}")

    png_file_name = source_config["png_file_name"]
    png_content = source_config["png_content"]
    width = source_config.get("width", 320)
    height = source_config.get("height", 100)
    colour = tuple(source_config.get("colour", [255, 255, 255]))

    target_gcp_bucket = dest_config["target_gcp_bucket"]
    target_gcp_path = dest_config["target_gcp_path"]

    if width <= 0 or height <= 0:
        raise AirflowFailException(f"Invalid PNG dimensions: {width}x{height}")

    if len(colour) != 3 or not all(0 <= c <= 255 for c in colour):
        raise AirflowFailException(f"Invalid RGB color: {colour}")

    def make_png_bytes(width, height, rgb):
        """Create PNG bytes for a solid color image."""
        row = bytes(rgb) * width
        raw = b"".join([b"\x00" + row for _ in range(height)])

        def chunk(tag, data):
            return (
                struct.pack('>I', len(data)) + tag + data + struct.pack('>I', zlib.crc32(tag + data) & 0xffffffff)
            )

        sig = b'\x89PNG\r\n\x1a\n'
        ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
        idat = zlib.compress(raw)
        return sig + chunk(b'IHDR', ihdr) + chunk(b'IDAT', idat) + chunk(b'IEND', b'')

    # Use temporary file to avoid leaving files on disk
    with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as temp_file:
        try:
            # Generate PNG bytes
            png_bytes = make_png_bytes(width, height, colour)

            # Write to temporary file
            temp_file.write(png_bytes)
            temp_file.flush()

            logger.info(f"PNG file created: {png_file_name} ({width}x{height}) at {temp_file.name}")

            # Upload to GCS
            client = storage.Client()
            bucket_obj = client.bucket(target_gcp_bucket)
            blob_obj = bucket_obj.blob(target_gcp_path)
            blob_obj.upload_from_filename(temp_file.name)

            # Attach content as metadata in GCS
            blob_obj.metadata = {'content': png_content}
            blob_obj.patch()

            logger.info(f"Successfully uploaded to GCS: gs://{target_gcp_bucket}/{target_gcp_path}")

        except Exception as e:
            logger.error(f"PNG generation and upload failed: {str(e)}")
            raise AirflowFailException(f"PNG generation and upload failed: {str(e)}")
        finally:
            # Cleanup temporary file
            try:
                os.unlink(temp_file.name)
                logger.debug(f"Cleaned up temporary file: {temp_file.name}")
            except OSError as cleanup_error:
                logger.warning(f"Failed to cleanup temporary file {temp_file.name}: {cleanup_error}")


def get_serverless_cluster_config(deploy_env: str, network_tag: str):
    """
    Build environment-specific network settings for Dataproc Serverless batches.

    Parameters:
        deploy_env (str): Deployment environment name (e.g., "dev", "uat", "prod").
        network_tag (str): Base VPC network tag to apply to serverless execution.

    Returns:
        tuple[str, list[str], str]:
            - subnetwork_uri: Fully qualified GCP subnetwork URI for the region and environment.
            - network_tags: List of network tags to attach to the execution (base tag plus "processing").
            - service_account: Service account email for Dataproc serverless execution in the given environment.

    Notes:
        - For non-prod environments, the default Shared VPC subnetwork (pcb-nonprod-svpc) is used and
          the placeholder {env} is replaced by the provided deploy_env.
        - For prod, a dedicated subnetwork (pcb-prod-svpc) is selected and placeholders are ignored for URI.
        - The returned service account follows the convention: dataproc@pcb-{env}-processing.iam.gserviceaccount.com.
    """

    subnetwork_uri_default = "https://www.googleapis.com/compute/v1/projects/pcb-nonprod-svpc/regions/northamerica-northeast1/subnetworks/snet-pcbnp-app01-{env}"
    subnetwork_uri_prod = "https://www.googleapis.com/compute/v1/projects/pcb-prod-svpc/regions/northamerica-northeast1/subnetworks/snet-pcbpr-app01-nane1"
    subnetwork_uri = subnetwork_uri_default.replace(ENV_PLACEHOLDER, deploy_env)
    if deploy_env == "prod":
        subnetwork_uri = subnetwork_uri_prod
    nw_tag = [network_tag, "processing"]
    service_account = "dataproc@pcb-{env}-processing.iam.gserviceaccount.com"

    return subnetwork_uri, nw_tag, service_account.replace(ENV_PLACEHOLDER, deploy_env)
