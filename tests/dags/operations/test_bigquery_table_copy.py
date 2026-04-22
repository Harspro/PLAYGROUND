import util.constants as consts

from airflow import settings
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env
)

from operations.bigquery_table_copy import SOURCE_TABLE_REF, TARGET_TABLE_REF


def test_bigquery_table_copy_config():
    config_dir = f'{settings.DAGS_FOLDER}/config'
    config_filename = 'bigquery_table_copy_config.yaml'
    gcp_config = read_variable_or_file(consts.GCP_CONFIG)
    deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
    table_copy_config = read_yamlfile_env(f'{config_dir}/{config_filename}', deploy_env)

    if not table_copy_config:  # empty config file
        return

    for job_id, dag_config in table_copy_config.items():
        assert dag_config.get(consts.DEFAULT_ARGS, {}).get(consts.DAG_OWNER)

        table_configs = dag_config.get(consts.TABLES)

        for table_config in table_configs:
            assert table_config.get(SOURCE_TABLE_REF)
            assert table_config.get(TARGET_TABLE_REF)
