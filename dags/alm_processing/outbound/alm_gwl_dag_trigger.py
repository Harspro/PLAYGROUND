import os
from abc import ABC
from copy import deepcopy
from datetime import timedelta, datetime
import pendulum
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
import util.constants as consts
import re
import logging
from alm_processing.utils import constants as alm_constant

from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)

from util.miscutils import read_variable_or_file, read_yamlfile_env, read_env_filepattern
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class ALMGwPcbMnthlyTrigger(ABC):
    def __init__(self):
        self.default_args = deepcopy(alm_constant.DAG_DEFAULT_ARGS)
        self.dag_id = "alm_gwl_pcb_sftp_mnthly_load"
        self.local_tz = pendulum.timezone('America/Toronto')
        gcp_config = read_variable_or_file("gcp_config")
        self.deploy_env = gcp_config['deployment_environment_name']
        self.deploy_env_suffix = gcp_config['deploy_env_storage_suffix']

    def _decide_gwl_dag_trigger(self, **context):
        bucket_name = context['dag_run'].conf.get('bucket')
        object_name = context['dag_run'].conf.get('name')
        if not bucket_name or not object_name:
            raise ValueError("Missing required 'bucket' or 'name' in DAG run configuration.")
        folder_name, file_name, *_ = object_name.split('/') + ['']

        context['ti'].xcom_push(key="bucket_name", value=bucket_name)
        context['ti'].xcom_push(key="folder_name", value=folder_name)
        context['ti'].xcom_push(key="file_name", value=file_name)

        gwl_cash_pattern_with_env = read_env_filepattern(alm_constant.GWL_CASH_FILE_PATTERN, self.deploy_env)
        gwl_custody_pattern_with_env = read_env_filepattern(alm_constant.GWL_CUSTODY_FILE_PATTERN, self.deploy_env)

        if re.search(gwl_cash_pattern_with_env, file_name):
            return "trigger_alm_gwl_pcb_cash_monthly_load"
        elif re.search(gwl_custody_pattern_with_env, file_name):
            return "trigger_alm_gwl_pcb_investment_by_deal_monthly_load"
        else:
            return "skip_dag_trigger_task"

    def add_dagrun_conf(self, **context):
        conf_payload = {
            "bucket": "{{ dag_run.conf['bucket'] }}",
            "name": "{{ dag_run.conf['name'] }}",
            "folder_name": "{{ ti.xcom_pull(task_ids='evaluate_gwl_dag_trigger_task', key='folder_name') }}",
            "file_name": "{{ ti.xcom_pull(task_ids='evaluate_gwl_dag_trigger_task', key='file_name') }}"
        }
        return conf_payload

    def create_dag(self, dag_config={}) -> DAG:
        with DAG(
                dag_id=self.dag_id,
                default_args=self.default_args,
                schedule=None,
                start_date=datetime(2024, 1, 1, tzinfo=self.local_tz),
                dagrun_timeout=timedelta(hours=5),
                max_active_tasks=20,
                max_active_runs=1,
                catchup=False,
                is_paused_upon_creation=True,
                tags=alm_constant.TAGS
        )as dag:
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(
                    self.dag_id, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            pipeline_start = EmptyOperator(task_id='start')

            evaluate_gwl_dag_trigger_task = BranchPythonOperator(
                task_id="evaluate_gwl_dag_trigger_task",
                python_callable=self._decide_gwl_dag_trigger)

            trigger_alm_gwl_pcb_cash_task = TriggerDagRunOperator(
                task_id="trigger_alm_gwl_pcb_cash_monthly_load",
                trigger_dag_id="alm_gwl_pcb_cash_monthly_load",
                conf=self.add_dagrun_conf(),
                dag=dag)

            trigger_alm_gwl_pcb_custody_task = TriggerDagRunOperator(
                task_id="trigger_alm_gwl_pcb_investment_by_deal_monthly_load",
                trigger_dag_id="alm_gwl_pcb_investment_by_deal_monthly_load",
                conf=self.add_dagrun_conf(),
                dag=dag)

            skip_task = EmptyOperator(task_id='skip_dag_trigger_task')

            end_task = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

            pipeline_start >> evaluate_gwl_dag_trigger_task
            evaluate_gwl_dag_trigger_task >> trigger_alm_gwl_pcb_cash_task >> end_task
            evaluate_gwl_dag_trigger_task >> trigger_alm_gwl_pcb_custody_task >> end_task
            evaluate_gwl_dag_trigger_task >> skip_task >> end_task

        return add_tags(dag)


ALMGwPcbMnthlyTrigger().create_dag()
