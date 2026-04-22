import logging
import pendulum
import util.constants as consts
import util.gcs_utils as gcs_utils
from airflow import models
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from typing import Final
from util.miscutils import read_variable_or_file
from dag_factory.terminus_dag_factory import add_tags

default_args = {
    'owner': 'team-growth-and-sales-alerts',
    'capability': 'CustomerAcquisition',
    'severity': 'P3',
    'sub_capability': 'Applications',
    'business_impact': 'Purge reporting error if data is not recorded in bigquery',
    'customer_impact': 'None',
    'depends_on_past': False,
    "email": [],
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False
}

gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
BUCKET_NAME: Final[str] = 'pcb-' + deploy_env + '-staging-extract'
PREFIX: Final[str] = "purging/reports/REPORT_"
PRE_PURGE: Final[str] = '_PRE_PURGE_'
SUFFIX: Final[str] = '.csv'
CSV_FORMAT_STRING: Final[str] = '%Y%m%d%H%M%S%f'
PURGE_TYPES = ('ADM', 'EXPERIAN', 'EAPPS')
GS_TAG: Final[str] = "team-growth-and-sales"


def validate_purge_reports(**context):
    """
        Get latest report file based on date.
        Fail the dag, if all the records are not purged
        Log tables which are not successfully purged
    """
    load_purged_apps = True
    report_datetime = context['dag_run'].conf['report_timestamp']
    logging.info("report_datetime: %s", report_datetime)
    for ptype in PURGE_TYPES:
        report = f"{PREFIX}{ptype}{PRE_PURGE}{report_datetime}{SUFFIX}"
        logging.info(report)
        csv_report = gcs_utils.read_file(BUCKET_NAME, report).split('\n')
        if csv_report:
            lines = csv_report[1:]
            for line in lines:
                columns = line.split(',')
                if len(columns) >= 4:
                    if int(columns[3]) > 0:
                        load_purged_apps = False
                        logging.error("For %s rows remaining to be purged %s", ptype, line)
        else:
            raise AirflowException("Failed to read the report: %s" % report)

    if not load_purged_apps:
        raise AirflowException("All rows are not successfully purged. Please purge them and rerun")


local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)
with models.DAG(
        dag_id="load_purged_apps_in_bq",
        schedule=None,
        start_date=pendulum.today(local_tz).add(days=-2),
        is_paused_upon_creation=True,
        catchup=False,
        default_args=default_args,
        tags=[GS_TAG]
) as dag:
    start_point = EmptyOperator(task_id='start')
    end_point = EmptyOperator(task_id='end')

    validate_purge = PythonOperator(
        task_id="validate_purge_reports",
        python_callable=validate_purge_reports
    )

    load_purged_adm = TriggerDagRunOperator(
        task_id='purgelist_adm',
        trigger_dag_id='purgelist_adm'
    )

    load_purged_experian = TriggerDagRunOperator(
        task_id='purgelist_experian',
        trigger_dag_id='purgelist_experian'
    )

    load_purged_eapps = TriggerDagRunOperator(
        task_id='purgelist_eapps',
        trigger_dag_id='purgelist_eapps'
    )

add_tags(dag)

start_point >> validate_purge >> [load_purged_adm, load_purged_experian, load_purged_eapps] >> end_point
