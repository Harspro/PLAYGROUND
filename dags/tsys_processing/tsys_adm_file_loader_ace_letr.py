import pendulum
import util.constants as consts
from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import timedelta
from tsys_processing.tsys_adm_file_loader import TsysADMFileLoader
from tsys_processing.tsys_file_loader_base import TSYS_FILE_LOADER
from typing import Final
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.miscutils import get_cluster_name_for_dag
from dag_factory.terminus_dag_factory import add_tags

INITIAL_DEFAULT_ARGS: Final = {
    "owner": "team-centaurs",
    'capability': 'Terminus Data Platform',
    'severity': 'P3',
    'sub_capability': 'Data Movement',
    'business_impact': 'N/A',
    'customer_impact': 'N/A',
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 10,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True
}


class TsysAceLetrLoader(TsysADMFileLoader):

    def is_trailer(self, segment_name: str):
        print('is_trailer called')
        return 'TS2A_TRLR' == segment_name or 'LETR_TRLR' == segment_name

    def get_tsys_count(self, context, segment_name: str, record_count_column):
        tsys_count = 1
        print('####### ACE LETTER get_tsys_count #########')
        if not self.is_trailer(segment_name):
            if segment_name == "LETR":
                tsys_count_str = context['ti'].xcom_pull(task_ids=f"{'LETR_TRLR'}.populate_load_result",
                                                         key=f"{consts.RECORD_COUNT}")
            else:
                tsys_count_str = context['ti'].xcom_pull(task_ids=f"{'TS2A_TRLR'}.populate_load_result",
                                                         key=f"{consts.RECORD_COUNT}")
            tsys_count = int(tsys_count_str)
        return tsys_count

    def check_record_count(self, bigquery_config: dict, segment_name: str, **context):
        if not self.is_trailer(segment_name):
            if segment_name == "LETR":
                tsys_count_str = context['ti'].xcom_pull(task_ids=f"{'LETR_TRLR'}.populate_load_result",
                                                         key=f"{consts.RECORD_COUNT}")
            else:
                tsys_count_str = context['ti'].xcom_pull(task_ids=f"{'TS2A_TRLR'}.populate_load_result",
                                                         key=f"{consts.RECORD_COUNT}")
            tsys_count = int(tsys_count_str)
            print(tsys_count)
            if tsys_count > 0:
                return f'{segment_name}.parse_file'
            else:
                return f'{segment_name}.populate_load_result'
        else:
            return f'{segment_name}.parse_file'

    def validate_row_count(self, bigquery_client, bq_ext_table_id: str, segment_name: str,
                           record_count_column: str, context, tsys_count: str):
        if self.is_trailer(segment_name):
            query = f"SELECT {consts.RECORD_COUNT} FROM {bq_ext_table_id}"
        else:
            query = f"SELECT COUNT(1) AS {consts.RECORD_COUNT} FROM {bq_ext_table_id}"
        results = bigquery_client.query(query).result().to_dataframe()
        num = results[consts.RECORD_COUNT].values[0]
        if self.is_trailer(segment_name):
            context['ti'].xcom_push(key=consts.RECORD_COUNT, value=f'{num}')
        else:
            if segment_name == "LETR":
                tsys_count = context['ti'].xcom_pull(task_ids=f"{'LETR_TRLR'}.populate_load_result",
                                                     key=f"{consts.RECORD_COUNT}")
            else:
                tsys_count = context['ti'].xcom_pull(task_ids=f"{'TS2A_TRLR'}.populate_load_result",
                                                     key=f"{consts.RECORD_COUNT}")
            if int(tsys_count) != int(num):
                raise AirflowException(f"tsys count {tsys_count} does not match bigquery count {num}")

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:

        self.default_args.update(dag_config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))

        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=None,
            start_date=pendulum.today(self.local_tz).add(days=-2),
            dagrun_timeout=timedelta(hours=5),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True,
            tags=dag_config.get(consts.TAGS, [])

        )

        with dag:
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(TSYS_FILE_LOADER, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            # move file from landing zone to processing zone.
            # Dataproc usually does not have permission to read from landing zone
            job_size = dag_config.get(consts.DATAPROC_JOB_SIZE)
            cluster_name = get_cluster_name_for_dag(dag_id)

            file_staging_task = self.build_file_staging_task(dag_config)
            cluster_creating_task = self.build_cluster_creating_task(cluster_name, job_size)
            preprocess_task = self.build_preprocessing_task_group(dag_id, cluster_name, dag_config)

            # Two trailer rows for ACE LETTER
            trailer_ts2a = self.build_segment_task_group(cluster_name, dag_config,
                                                         'TS2A_TRLR')
            trailer_letr = self.build_segment_task_group(cluster_name, dag_config,
                                                         'LETR_TRLR')
            body_segment_tasks_ts2a = self.build_segment_task_group(cluster_name, dag_config,
                                                                    'TS2A')
            body_segment_tasks_letr = self.build_segment_task_group(cluster_name, dag_config,
                                                                    'LETR')
            postprocess_task = self.build_postprocessing_task_group(dag_id, dag_config)
            done = EmptyOperator(task_id='Done')
            file_staging_task >> cluster_creating_task >> preprocess_task
            preprocess_task >> trailer_letr >> body_segment_tasks_letr >> postprocess_task
            preprocess_task >> trailer_ts2a >> body_segment_tasks_ts2a >> postprocess_task
            postprocess_task >> done
        return add_tags(dag)
