import logging
import pprint
import re
from datetime import datetime, timedelta

from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from util.miscutils import read_variable_or_file, read_yamlfile_env, read_env_filepattern
from dag_factory.terminus_dag_factory import add_tags

DAG_DEFAULT_ARGS = {
    "owner": "team-centaurs",
    'capability': 'Terminus Data Platform',
    'severity': 'P2',
    'sub_capability': 'TBD',
    'business_impact': 'Processing is initiated by this DAG on file arrival; if the file trigger fails, that file’s processing is skipped',
    'customer_impact': 'TBD',
    "start_date": datetime(2021, 6, 26),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 5,
    "retries": 0,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "catchup": False
}

# Configs
bucket_config_fname = "bucket_trigger_config.yaml"

NO_BUCKET_MAPPING_TASK_ID = "no_bucket_mapping"
NO_FOLDER_MAPPING_TASK_ID = "no_folder_mapping"

# Code
bucket_mappings_filepath = f'{settings.DAGS_FOLDER}/{bucket_config_fname}'
gcp_config = read_variable_or_file("gcp_config")
deploy_env = gcp_config['deployment_environment_name']
deploy_env_suffix = gcp_config['deploy_env_storage_suffix']
bucket_mappings = read_yamlfile_env(bucket_mappings_filepath, deploy_env_suffix)


# pprint.pprint(bucket_mappings)

# logging.info(f"bucket mappings: {bucket_mappings}")

def make_safe_task_id(name: str) -> str:
    # Replace everything but alphanum, dashes, dots, and underscores
    return re.sub(r'[^\w\-\.]', '_', name)


def select_bucket_branch(*bucket_maps, **context):
    print('Context:')
    pprint.pprint(context)
    pprint.pprint(context['dag_run'].conf)

    bucket_name = context['dag_run'].conf.get('bucket')
    object_name = context['dag_run'].conf.get('name')

    parts = object_name.split('/')
    if len(parts) < 2:
        folder_name, file_name = object_name, ''
    else:
        folder_name = '/'.join(parts[:-1])
        file_name = parts[-1]

    context['ti'].xcom_push(key="bucket_name", value=bucket_name)
    context['ti'].xcom_push(key="folder_name", value=folder_name)
    context['ti'].xcom_push(key="file_name", value=file_name)

    bmap_filt = [b for b in bucket_maps if b['bucket'] == bucket_name]
    if len(bmap_filt) > 0:
        return bucket_name
    else:
        return NO_BUCKET_MAPPING_TASK_ID


def add_bucket_config(bmap: dict):
    config_filepath = (f"{settings.DAGS_FOLDER}/"
                       f"{bmap['config_file']}")
    # print(config_filepath)
    bmap['config'] = read_yamlfile_env(config_filepath)


def select_folder_branch(bucket_config, **context):
    folder_name = context['ti'].xcom_pull(task_ids="select_bucket_task", key="folder_name")
    file_name = context['ti'].xcom_pull(task_ids="select_bucket_task", key="file_name")
    bcfg = bucket_config['config']
    bcfg_filt = [b for b in bcfg if b['folder_name'] == folder_name]

    if len(bcfg_filt) == 0:
        logging.info('Uploaded folder is unknown')
        return NO_FOLDER_MAPPING_TASK_ID

    # Assumption: one folder per file-type
    folder_cfg = bcfg_filt[0]
    filename_pattern_with_env = read_env_filepattern(folder_cfg['filename_pattern'], deploy_env)

    if re.search(filename_pattern_with_env, file_name):
        return make_safe_task_id(folder_name)
    else:
        logging.info('Filename pattern did not match')
        return NO_FOLDER_MAPPING_TASK_ID


def add_dagrun_conf(**context):
    conf_payload = {
        "bucket": "{{ dag_run.conf['bucket'] }}",
        "name": "{{ dag_run.conf['name'] }}",
        "folder_name": "{{ ti.xcom_pull(task_ids='select_bucket_task', key='folder_name') }}",
        "file_name": "{{ ti.xcom_pull(task_ids='select_bucket_task', key='file_name') }}"
    }
    return conf_payload


with DAG(dag_id='bucket_trigger_mapping',
         schedule=None,
         is_paused_upon_creation=True,
         default_args=DAG_DEFAULT_ARGS) as dag:
    add_tags(dag)
    no_bucket_mapping_task = EmptyOperator(task_id=NO_BUCKET_MAPPING_TASK_ID)
    no_folder_mapping_task = EmptyOperator(
        task_id=NO_FOLDER_MAPPING_TASK_ID,
        trigger_rule='none_failed_min_one_success'
    )

    select_bucket_task = BranchPythonOperator(
        task_id="select_bucket_task",
        python_callable=select_bucket_branch,
        op_args=bucket_mappings
    )
    select_bucket_task >> no_bucket_mapping_task

    for bmap in bucket_mappings:
        add_bucket_config(bmap)

        select_folder_task = BranchPythonOperator(
            task_id=f"{bmap['bucket']}",
            python_callable=select_folder_branch,
            op_kwargs={"bucket_config": bmap}
        )
        select_folder_task >> no_folder_mapping_task
        select_bucket_task >> select_folder_task

        if bmap['config'] is None:
            continue

        for dag_cfg in bmap['config']:
            folder_name = dag_cfg['folder_name']
            safe_folder_name = make_safe_task_id(folder_name)
            folder_task = EmptyOperator(task_id=safe_folder_name)
            select_folder_task >> folder_task
            with TaskGroup(group_id=f'{safe_folder_name}_tasks') as folder_task_group:
                dags_to_trigger = dag_cfg['dags_to_trigger']
                for d in dags_to_trigger:
                    dag_task = TriggerDagRunOperator(
                        task_id=f"trigger__{d}",
                        trigger_dag_id=f"{d}",
                        conf=add_dagrun_conf()
                    )

            folder_task >> folder_task_group
