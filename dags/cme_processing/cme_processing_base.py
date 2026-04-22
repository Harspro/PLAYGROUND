import json
import logging
from copy import deepcopy
from typing import Final, Union

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup

from cme_processing.utils.constants import DEPLOY_ENV, JOB_DATE_TIME
from util.logging_utils import build_spark_logging_info
import util.constants as consts
from etl_framework.etl_dag_base import ETLDagBase
from google.cloud import bigquery

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator)
from util.miscutils import (
    read_variable_or_file,
    save_job_to_control_table,
    get_cluster_config_by_job_size
)

logger = logging.getLogger(__name__)
UPDATE_JOB_ARGS: Final = 'updater_job_args'
UPDATE_JAR_URI: Final = 'update_jar_uri'
UPDATER_MAIN_CLASS: Final = 'updater_main_class'


class CmeBaseProcessor(ETLDagBase):
    def __init__(self, module_name, config_path, config_filename, dag_default_args):
        super().__init__(module_name, config_path, config_filename, dag_default_args)
        self.default_args = deepcopy(dag_default_args)
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)

    def preprocessing_job(self, config: dict, upstream_task: list):
        pass

    def build_control_record_saving_job(self, status: str, **context):
        job_params_str = json.dumps({'status': status})
        save_job_to_control_table(job_params_str, **context)

    def update_cme_batch_param(self, config: dict):
        bigquery_client = bigquery.Client()
        param_table = f"pcb-{DEPLOY_ENV}-landing.domain_account_management.CME_BATCH_PARAM"
        interface_id = config.get('interface_id')
        exec_id_sql = f"SELECT CAST(COALESCE(MAX(CAST(execution_id AS NUMERIC)), 0) + 1 AS STRING) AS execution_id_value FROM {param_table} WHERE INTERFACE_ID='{interface_id}'"
        logger.info(f"extracting max execution id using the query {exec_id_sql}")
        exec_id_sql_results = bigquery_client.query(exec_id_sql).result().to_dataframe()
        execution_id = exec_id_sql_results['execution_id_value'].values[0]
        logger.info(f"latest run details for offers will be added in {param_table}")
        insert_query = f"""INSERT INTO {param_table}
                           (CAMPAIGN_TYPE,DATE_LAST_RUN,REC_CREATE_TMS,REC_CHNG_TMS,EXECUTION_ID,INTERFACE_ID) VALUES
                           ('CLI', '{JOB_DATE_TIME}', '{JOB_DATE_TIME}', '{JOB_DATE_TIME}','{execution_id}','{interface_id}'),
                           ('PCH', '{JOB_DATE_TIME}', '{JOB_DATE_TIME}', '{JOB_DATE_TIME}','{execution_id}','{interface_id}'),
                           ('AAU', '{JOB_DATE_TIME}', '{JOB_DATE_TIME}', '{JOB_DATE_TIME}','{execution_id}','{interface_id}')"""
        insert_query_results = bigquery_client.query(insert_query)
        insert_query_results.result()

    def get_oracle_table_segment_list(self, config: dict):
        spark_config = config.get(consts.SPARK)
        segment_configs = spark_config.get(UPDATE_JOB_ARGS)

        return [segment_name for segment_name in segment_configs]

    def check_count_task(self, config: dict, segment_name: str, group_id: str):
        bigquery_client = bigquery.Client()
        table = f"pcb-{DEPLOY_ENV}-processing.domain_marketing.CME_{segment_name}_STG"
        count_sql = f"SELECT COUNT(*) AS cnt FROM {table}"
        logger.info(f"extracting count before oracle table update for {table}")
        count_sql_results = bigquery_client.query(count_sql).result().to_dataframe()
        count = int(count_sql_results['cnt'].values[0])
        logger.info(f"If count is zero than task group will skip for {segment_name}")
        return f"{group_id}.{consts.CLUSTER_CREATING_TASK_ID}" if count > 0 else f"{group_id}.skip_segment"

    def oracle_table_updates(self, cluster_name: str, config: dict, segment_name: str, **context):
        """
            Initializing spark for creating the output file ,
            using the BQ data. Spark parameters are maintained in configuration file itself
            """
        spark_config = config.get(consts.SPARK)
        segment_configs = spark_config.get(UPDATE_JOB_ARGS)
        updater_job_args = segment_configs.get(segment_name)

        arglist = []
        for k, v in updater_job_args.items():
            if k == "oracle_merge_args":
                continue
            arglist.append(f'{v}')

        arglist = build_spark_logging_info(dag_id='{{dag.dag_id}}', default_args=self.default_args,
                                           arg_list=arglist)
        oracle_merge_args = segment_configs.get(segment_name).get('oracle_merge_args')
        if oracle_merge_args:
            for key, value in oracle_merge_args.items():
                if value is not None:
                    arglist.append(f"{key}={value}")

        spark_oracle_updater_job = {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_config[UPDATE_JAR_URI],
                consts.MAIN_CLASS: spark_config[UPDATER_MAIN_CLASS],
                consts.FILE_URIS: spark_config[consts.FILE_URIS],
                consts.ARGS: arglist
            }
        }
        return DataprocSubmitJobOperator(
            task_id=f"{segment_name}_spark_oracle_updater_job",
            job=spark_oracle_updater_job,
            region=self.dataproc_config.get(consts.LOCATION),
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
        )

    def build_cluster_creating_task(self, job_size: str, cluster_name: str):
        return DataprocCreateClusterOperator(
            task_id=consts.CLUSTER_CREATING_TASK_ID,
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            cluster_config=get_cluster_config_by_job_size(self.deploy_env, self.gcp_config.get(consts.NETWORK_TAG),
                                                          job_size),
            region=self.dataproc_config.get(consts.LOCATION),
            cluster_name=cluster_name
        )

    def build_oracle_table_updates_group(self, cluster_name: str, config: dict, body_segments: Union[list, set]):
        with TaskGroup(group_id="oracle_table_updates_group") as task_group:
            previous_task = None

            for segment_name in body_segments:
                with TaskGroup(group_id=f"{segment_name}") as segment_group:
                    check_count = BranchPythonOperator(
                        task_id=f"check_count_{segment_name}",
                        python_callable=self.check_count_task,
                        op_kwargs={"config": config, "segment_name": segment_name, "group_id": segment_group.group_id}
                    )
                    skip_segment = EmptyOperator(task_id="skip_segment")

                    cluster_task = self.build_cluster_creating_task(
                        job_size=config.get('job_size'),
                        cluster_name=cluster_name
                    )

                    spark_oracle_updater_task = self.oracle_table_updates(
                        cluster_name=cluster_name,
                        config=config,
                        segment_name=segment_name
                    )

                    segment_end = EmptyOperator(
                        task_id="segment_end",
                        trigger_rule="none_failed_min_one_success",
                    )

                    check_count >> skip_segment >> segment_end
                    check_count >> cluster_task >> spark_oracle_updater_task >> segment_end

                if previous_task:
                    previous_task >> segment_group
                previous_task = segment_group

        return task_group

    def postprocessing_job(self, config: dict, upstream_task: list):
        activity_var = config.get('activity_name', '')
        cluster_name = config.get('cluster_name')
        task_grp_id = f'{activity_var}_post_trans_job'

        with (TaskGroup(group_id=task_grp_id)):
            oracle_table_updates_task = self.build_oracle_table_updates_group(cluster_name, config,
                                                                              self.get_oracle_table_segment_list(
                                                                                  config))
            upstream_task.append(oracle_table_updates_task)
            update_cme_batch_param = PythonOperator(
                task_id=f"{activity_var}_update_cme_batch_param",
                python_callable=self.update_cme_batch_param,
                op_kwargs={'config': config}
            )
            upstream_task.append(update_cme_batch_param)
            control_record_saving_task = PythonOperator(
                task_id='save_job_to_control_table',
                trigger_rule='none_failed',
                python_callable=self.build_control_record_saving_job,
                op_kwargs={'status': 'SUCCESS'}
            )
            upstream_task.append(control_record_saving_task)
        return upstream_task
