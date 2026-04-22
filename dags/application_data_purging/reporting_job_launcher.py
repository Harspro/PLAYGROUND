import logging
from datetime import timedelta
from typing import Final

import pendulum
import util.constants as consts
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from util.gcs_utils import (read_file_by_uri, write_file_by_uri)
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
    get_ephemeral_cluster_config
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

PURGING_PROCESSOR_PREFIX: Final = 'pcb.purging.processor'
DAG_ID: Final = 'application_data_purging_report'
BATCH_STATUS: Final = 'batch_status'
GENERATOR_CONFIG: Final = 'sql.script.generator'
TABLES: Final = 'tables'
ADM: Final = 'adm'
EXPERIAN: Final = 'experian'
EAPPS: Final = 'eapps'
NAME: Final = 'name'
SCHEMAS: Final = 'schemas'
PROCESSOR_CONFIG: Final = 'purging.processor'
OUTPUT_LOCATION: Final = 'output.location'
GS_TAG: Final[str] = "team-growth-and-sales"


class ReportingJobLauncher:
    def __init__(self, config_filename: str = None, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)
        self.ephemeral_cluster_name = 'application-data-purging-ephemeral'

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/application_data_purging/config'

        if config_filename is None:
            config_filename = 'application_data_purging_config.yaml'

        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)

        self.batch_config = read_yamlfile_env(f'{config_dir}/application_data_purging_report_config.yaml', self.deploy_env)
        self.batch_size = 5

        self.default_args = {
            'owner': 'team-growth-and-sales-alerts',
            'capability': 'CustomerAcquisition',
            'severity': 'P3',
            'sub_capability': 'Applications',
            'business_impact': 'Compliance Issue if the data is not purged',
            'customer_impact': 'None',
            'depends_on_past': False,
            "email": [],
            "retries": 0,
            "email_on_failure": False,
            "email_on_retry": False
        }

        self.local_tz = pendulum.timezone('America/Toronto')

    def concat_reports(self, output_location: str, timestamp: str, stage: str):
        for category, tables in self.batch_config.get(GENERATOR_CONFIG).get(TABLES).items():
            report_list = ['DB Name,Table Name,Total Rows,Num of Rows to be Purged']
            category_upper = category.upper()
            for table in tables:
                name = table.get(NAME)
                schemas = [x.strip() for x in table.get(SCHEMAS).split(',')]
                for schema in schemas:
                    table_id = schema + '.' + name
                    report_location = f'{output_location}temp/{timestamp}/{category_upper}/{table_id}.csv'
                    report_list.append(read_file_by_uri(report_location))

            logger.info(f'report_list={report_list}')

            report_location = f'{output_location}/reports/REPORT_{category_upper}_{stage}_{timestamp}.csv'
            content = '\n'.join(report_list)
            logger.info(f'report_location={report_location}')
            logger.info(f'content={content}')
            write_file_by_uri(report_location, content)

    def create_task(self, table_id: str, timestamp: str):
        spark_job_config = self.job_config[consts.SPARK_JOB]

        arg_list = [f'{PURGING_PROCESSOR_PREFIX}.table.id={table_id}',
                    f'{PURGING_PROCESSOR_PREFIX}.batch.status=REPORT',
                    f'{PURGING_PROCESSOR_PREFIX}.report.timestamp={timestamp}']

        spark_job = {
            consts.REFERENCE: {consts.PROJECT_ID: self.dataproc_config.get(consts.PROJECT_ID)},
            consts.PLACEMENT: {consts.CLUSTER_NAME: self.ephemeral_cluster_name},
            consts.SPARK_JOB: {
                consts.JAR_FILE_URIS: spark_job_config[consts.JAR_FILE_URIS],
                consts.MAIN_CLASS: spark_job_config[consts.MAIN_CLASS],
                consts.FILE_URIS: spark_job_config[consts.FILE_URIS],
                consts.PROPERTIES: spark_job_config[consts.PROPERTIES],
                consts.ARGS: arg_list
            }
        }

        report_task = DataprocSubmitJobOperator(
            task_id='create_report_for_' + table_id.replace('.', '_'),
            job=spark_job,
            region=self.dataproc_config.get(consts.LOCATION),
            project_id=self.dataproc_config.get(consts.PROJECT_ID),
            gcp_conn_id=self.gcp_config.get(consts.PROCESSING_ZONE_CONNECTION_ID),
            trigger_rule=TriggerRule.NONE_SKIPPED
        )

        return report_task

    def create_dag(self, dag_id: str) -> DAG:
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=None,
            catchup=False,
            is_paused_upon_creation=True,
            start_date=pendulum.today(self.local_tz).add(days=-2),
            dagrun_timeout=timedelta(hours=24),
            tags=[GS_TAG]
        )

        with dag:
            start_point = EmptyOperator(task_id='start')
            end_point = EmptyOperator(task_id='end')

            cluster_creating_task = DataprocCreateClusterOperator(
                task_id=consts.CLUSTER_CREATING_TASK_ID,
                project_id=self.dataproc_config.get(consts.PROJECT_ID),
                cluster_config=get_ephemeral_cluster_config(self.deploy_env, self.gcp_config.get(consts.NETWORK_TAG),
                                                            9),
                region=self.dataproc_config.get(consts.LOCATION),
                cluster_name=self.ephemeral_cluster_name
            )

            batch_start = EmptyOperator(task_id='batch_start')

            start_point >> cluster_creating_task >> batch_start

            output_location = self.batch_config.get(PROCESSOR_CONFIG).get(OUTPUT_LOCATION)
            timestamp = "{{ dag_run.get_task_instance('start').start_date.strftime('%Y%m%d%H%M%S%f') }}"
            stage = "{{ dag_run.conf.get('stage')  or 'PRE_PURGE' }}"

            concat_reports = PythonOperator(
                task_id='concat_reports',
                python_callable=self.concat_reports,
                op_kwargs={'output_location': output_location,
                           'timestamp': timestamp,
                           'stage': stage},
                trigger_rule=TriggerRule.NONE_SKIPPED
            )

            load_purged_apps = TriggerDagRunOperator(
                task_id='load_purged_apps_in_bq',
                trigger_dag_id='load_purged_apps_in_bq',
                conf={'report_timestamp': timestamp}
            )

            concat_reports >> load_purged_apps >> end_point

            counter = 0
            batch_num = 0
            curr_batch = []
            leading_task = batch_start

            for category, tables in self.batch_config.get(GENERATOR_CONFIG).get(TABLES).items():
                for table in tables:
                    name = table.get(NAME)
                    schemas = [x.strip() for x in table.get(SCHEMAS).split(',')]
                    for schema in schemas:
                        if counter == self.batch_size:
                            counter = 0
                            leading_task >> curr_batch
                            leading_task = EmptyOperator(task_id=f'batch_{batch_num}',
                                                         trigger_rule=TriggerRule.NONE_SKIPPED)
                            curr_batch >> leading_task
                            batch_num += 1
                            curr_batch = []

                        curr_batch.append(self.create_task(schema + '.' + name, timestamp))
                        counter += 1

            if curr_batch:
                (leading_task >> curr_batch >> EmptyOperator(task_id=f'batch_{batch_num}', trigger_rule=TriggerRule.NONE_SKIPPED) >> concat_reports)
            else:
                leading_task >> concat_reports

        return add_tags(dag)

    def create(self) -> dict:
        logger.info(f"job config. type: {type(self.job_config)}, value: {self.job_config}")
        return {DAG_ID: self.create_dag(DAG_ID)}


globals().update(ReportingJobLauncher().create())
