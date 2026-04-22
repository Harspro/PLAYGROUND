import logging
import json
from datetime import timedelta, datetime, date
import pendulum
from airflow import DAG
from airflow import settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import util.constants as consts
from util.miscutils import read_variable_or_file, read_file_env, save_job_to_control_table, get_dagrun_last
from util.bq_utils import run_bq_query
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowFailException
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)
gcp_config = read_variable_or_file(consts.GCP_CONFIG)
deploy_env_name = gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
local_tz = pendulum.timezone('America/Toronto')

DAG_DEFAULT_ARGS = {
    'owner': 'team-digital-adoption-alerts',
    'capability': 'Account Management',
    'severity': 'P3',
    'sub_capability': 'Account Management',
    'business_impact': 'This DAG will identify the eligible accounts for the loyalty offer',
    'customer_impact': 'Customer will miss out the loyalty offer for the day',
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retry_delay': timedelta(seconds=10)
}


def save_job_details_to_control_table(status: str, **context):
    # Get current date in EST timezone at execution time
    dag_run_date = datetime.now(local_tz).strftime('%Y-%m-%d %H:%M:%S %z')

    job_params_str = json.dumps({
        'status': status,
        'dag_run_date': dag_run_date
    })
    save_job_to_control_table(job_params_str, **context)


def get_last_dag_run_date_from_control_table(dag_id: str, **context):
    DEFAULT_CUTOFF_DATE = date(2025, 7, 1).strftime('%Y-%m-%d')

    result = get_dagrun_last(dag_id)

    # Get job_params if previous runs exist
    job_params = {}
    if result.total_rows > 0:
        job_params = json.loads(result.to_dataframe()['job_params'].iloc[0])

    # Use default date if no runs or missing dag_run_date key
    if result.total_rows == 0 or 'dag_run_date' not in job_params:
        last_dag_run = DEFAULT_CUTOFF_DATE
        logger.info(f"Returning the default cutoff date: {last_dag_run}")
    else:
        # Parse previous run's dag_run_date
        dag_run_date_full = job_params['dag_run_date']
        last_dag_run = datetime.strptime(dag_run_date_full, '%Y-%m-%d %H:%M:%S %z').strftime('%Y-%m-%d')
        logger.info(f"Prior run exists. Returning the last run date: {last_dag_run}")

    # Push to XCom and return
    context['ti'].xcom_push(key='last_dag_run_date', value=last_dag_run)
    return last_dag_run


def check_for_record_count(last_dag_run: str = None):
    if last_dag_run is None:
        raise AirflowFailException("last_dag_run is None - upstream task may have failed or XCom data is missing")

    validation_query = read_file_env(f"{settings.DAGS_FOLDER}/digital_adoption/sql/cli_offer_kafka_count_check.sql", deploy_env_name)
    validation_query = validation_query.replace("{last_dag_run_date}", last_dag_run)
    result = run_bq_query(validation_query).result()
    count = list(result)[0]['record_count']
    if count > 0:
        return 'kafka_writer_task_for_cli'
    else:
        logger.info("Warning: No active Credit Limit Increase Offers")
        return 'end'


with DAG(dag_id='cli_offer_event_kafka_writer',
         default_args=DAG_DEFAULT_ARGS,
         schedule="00 1 * * *",
         description='DAG to identify the eligible accounts for loyalty offers ',
         start_date=datetime(2025, 6, 1, tzinfo=local_tz),
         max_active_runs=1,
         catchup=False,
         dagrun_timeout=timedelta(hours=24),
         is_paused_upon_creation=True
         ) as dag:

    add_tags(dag)

    start_point = EmptyOperator(task_id=consts.START_TASK_ID)
    end_point = EmptyOperator(task_id=consts.END_TASK_ID, trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    get_last_run_task = PythonOperator(
        task_id='get_last_dag_run_date',
        python_callable=get_last_dag_run_date_from_control_table,
        op_kwargs={'dag_id': dag.dag_id}
    )

    check_for_record_count_task = BranchPythonOperator(
        task_id='branch_on_record_existence',
        python_callable=check_for_record_count,
        op_kwargs={'last_dag_run': "{{ ti.xcom_pull(task_ids='get_last_dag_run_date', key='last_dag_run_date') }}"}
    )

    cli_kafka_writer_task = TriggerDagRunOperator(
        task_id='kafka_writer_task_for_cli',
        trigger_dag_id='cli_offer_event_kafka_publish',
        wait_for_completion=True,
        conf={
            "replacements": {
                "{last_dag_run_date}": "{{ ti.xcom_pull(task_ids='get_last_dag_run_date', key='last_dag_run_date') }}"
            }
        }
    )

    control_record_saving_task = PythonOperator(
        task_id='save_job_to_control_table',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        python_callable=save_job_details_to_control_table,
        op_kwargs={'status': 'SUCCESS'}
    )

    start_point >> get_last_run_task >> check_for_record_count_task
    check_for_record_count_task >> cli_kafka_writer_task >> control_record_saving_task >> end_point
    check_for_record_count_task >> control_record_saving_task >> end_point
