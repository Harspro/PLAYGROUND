import logging
from datetime import timedelta

import pendulum
import util.constants as consts
from airflow import DAG
from airflow import settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)
from util.logging_utils import build_spark_logging_info
from util.miscutils import (
    read_variable_or_file,
    get_ephemeral_cluster_config,
    read_yamlfile_env
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

CLICKID_CONFIG_FILENAME = f'{settings.DAGS_FOLDER}/clickid_processing/clickid_outbound_config.yaml'
DATE_PATTERN = '%Y%m%d'


class ClickidDagsBuilder:
    def __init__(self, config_filename: str):
        self.gcp_config = read_variable_or_file("gcp_config")
        self.deploy_env = self.gcp_config['deployment_environment_name']

        self.dataproc_config = read_variable_or_file('dataproc_config')
        self.cluster_cfg = get_ephemeral_cluster_config(self.deploy_env, self.gcp_config.get("network_tag"))
        self.cluster_cfg['worker_config']['num_instances'] = 4

        self.dag_config = read_yamlfile_env(config_filename, self.deploy_env)
        self.default_args = {
            'owner': 'team-growth-and-sales-alerts',
            'capability': 'TBD',
            'severity': 'P3',
            'sub_capability': 'TBD',
            'business_impact': 'TBD',
            'customer_impact': 'TBD',
            'depends_on_past': False
        }
        self.tzinfo = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)

    def get_report_dates(self, **context):
        # Option-1: Hard code to today, yesterday
        # current_date = date.today().strftime(DATE_PATTERN)
        # yesterday_date = (date.today() - timedelta(days=1)).strftime(DATE_PATTERN)

        # Option-2: Logical dates from the dag run
        current_date = context['next_ds_nodash']
        yesterday_date = context['ds_nodash']

        # Allow override for manual trigger
        conf_current_date = context['dag_run'].conf.get('current_report_date')
        conf_last_date = context['dag_run'].conf.get('last_report_date')
        logger.info(f'conf: {conf_current_date}, {conf_last_date}')
        current_report_date = current_date
        if conf_current_date:
            current_report_date = conf_current_date

        last_report_date = yesterday_date
        if conf_last_date:
            last_report_date = conf_last_date

        return current_report_date, last_report_date

    def build_clickid_spark_job_tmpl(self, config: dict, **context):
        job_tmpl = {
            'reference': {'project_id': self.dataproc_config.get('project_id')},
            'placement': {'cluster_name': config['cluster_name']},
            'spark_job': config["spark_job"].copy()
        }

        current_report_date, last_report_date = self.get_report_dates(**context)
        logger.info(f'current_report_date: {current_report_date}, last_report_date: {last_report_date}')

        job_tmpl['spark_job']['args']['pcb.clickid.processor.current.report.date'] = current_report_date

        if 'pcb.clickid.processor.last.report.date' in job_tmpl['spark_job']['args']:
            job_tmpl['spark_job']['args']['pcb.clickid.processor.last.report.date'] = last_report_date

        arglist = []
        for k, v in job_tmpl['spark_job']['args'].items():
            arglist.append(f'{k}={v}')
        job_tmpl['spark_job']['args'] = arglist
        dag = context.get('dag')
        dag_id = dag.dag_id
        run_id = context.get('run_id')
        job_tmpl['spark_job']['args'] = build_spark_logging_info(dag_id=dag_id, default_args=self.default_args, arg_list=job_tmpl['spark_job']['args'], run_id=run_id)
        context['ti'].xcom_push(key='spark_job', value=job_tmpl)

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        with DAG(
                dag_id=dag_id,
                default_args=self.default_args,
                render_template_as_native_obj=True,
                dagrun_timeout=timedelta(minutes=180),
                schedule=config['schedule_cron'],
                start_date=pendulum.today(self.tzinfo).add(days=-10),
                catchup=False,
                description=config['description'],
                tags=config['tags'],
                is_paused_upon_creation=True
        ) as dag:

            start = EmptyOperator(task_id="start")

            build_spark_job_tmpl = PythonOperator(
                task_id="build_clickid_spark_job_tmpl",
                python_callable=self.build_clickid_spark_job_tmpl,
                op_kwargs={'config': config}
            )

            create_cluster = DataprocCreateClusterOperator(
                task_id="create_cluster",
                project_id=self.gcp_config.get("processing_zone_project_id"),
                cluster_config=self.cluster_cfg,
                region="northamerica-northeast1",
                cluster_name=config['cluster_name']
            )

            google_clickid_spark_task = DataprocSubmitJobOperator(
                task_id="clickid_spark_job",
                job="{{ ti.xcom_pull(task_ids='build_clickid_spark_job_tmpl', key='spark_job') }}",
                region=self.dataproc_config.get('location'),
                project_id=self.dataproc_config.get('project_id'),
                gcp_conn_id=self.gcp_config.get('processing_zone_connection_id'),
            )

            start >> build_spark_job_tmpl >> create_cluster >> google_clickid_spark_task
            return add_tags(dag)

    def create_all_dags(self) -> dict:
        dags = {}
        for dag_id, config in self.dag_config.items():
            dags[dag_id] = self.create_dag(dag_id, config)

        return dags


clickid_dags_builder = ClickidDagsBuilder(CLICKID_CONFIG_FILENAME)
globals().update(clickid_dags_builder.create_all_dags())
