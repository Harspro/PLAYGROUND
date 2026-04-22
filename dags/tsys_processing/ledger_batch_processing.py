import json
import logging
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)
from google.cloud import storage, bigquery
import pendulum

from util.miscutils import (
    read_variable_or_file,
    get_cluster_config_by_job_size,
    get_cluster_name_for_dag,
    save_job_to_control_table,
    get_dagrun_last,
    get_dagruns_after
)

# Constants
LEDGER_BATCH_CONFIG = 'tsys_processing/ledger_batch_config'
DATAPROC_CONFIG = 'dataproc_config'
GCP_CONFIG = 'gcp_config'
JOB_HISTORY_CONFIG = 'tsys_processing/job_history_config'
DEPLOYMENT_ENVIRONMENT_NAME = 'deployment_environment_name'

ODS_TRANSACTION_FETCHING_JOB = 'ods_transaction_fetching_job'
BQ_TRANSACTION_TABLE = 'bq_transaction_table'
BQ_LEDGER_HISTORY_TABLE = 'bq_ledger_history_table'
BQ_EVE_DATASETS = 'bq_eve_datasets'
BQ_DOMAIN_ACCOUNT_MANAGEMENT_DATASET = 'domain_account_management_dataset'
BQ_DOMAIN_CUSTOMER_MANAGEMENT_DATASET = 'domain_customer_management_dataset'

LEDGER_SPARK_BATCH_JOB = 'ledger_spark_batch_job'
JAR_FILE_URIS = 'jar_file_uris'
FILE_URIS = 'file_uris'
MAIN_CLASS = 'main_class'
ARGS = 'args'
PROPERTIES = 'properties'
SOURCE_FOLDER = 'source_folder'
TABLE_NAME = 'table_name'
FFM_ACCTM_SEGMENTS = 'ffm_acctm_segments'

APPLICATION_CONF = 'application.conf'

PROJECT_ID = 'project_id'
REFERENCE = 'reference'
PLACEMENT = 'placement'
SPARK_JOB = 'spark_job'
TASK_INSTANCE = 'ti'
CLUSTER_NAME = 'cluster_name'
LOCATION = 'location'

PROCESSING_ZONE_CONNECTION_ID = 'processing_zone_connection_id'
CURATED_ZONE_PROJECT_ID = 'curated_zone_project_id'
DATASET_ID = 'dataset_id'
HISTORY_FILE_PREFIX = 'history_file_prefix'

BUCKET_NAME = 'bucket_name'
FOLDER_NAME = 'folder_name'
DATE_PATTERN = '%Y%m%d'
PREVIOUS_LEDGER_DATE = 'previous_ledger_date'
CURRENT_LEDGER_DATE = 'current_ledger_date'
ODS_FOLDER_PATH = 'ods_folder_path'

PROCESSING_BUCKET_EXTRACT = 'processing_bucket_extract'
ODS_FOLDER_NAME = 'ods_folder_name'

DEPENDENCIES_VERIFICATION_TASK_ID = 'check_upstream_dependencies'
ODS_TRANSACTIONS_FETCHING_TASK_ID = 'fetch_transactions'
BQ_TRANSACTIONS_LOADING_TASK_ID = 'load_transactions_to_bigquery'
BQ_LEDGER_HISTORY_LOADING_TASK_ID = 'load_ledger_history_to_bigquery'
LEDGER_DATES_CHECK_TASK_ID = 'check_ledger_dates'
RUN_TYPE_CHECK_TASK_ID = 'is_initial_run'
CLUSTER_CREATING_TASK_ID = 'create_cluster'
RERUN_NOOP_TASK_ID = 'noop_skip_rerun'
INITIAL_LEDGER_FEED_GEN_TASK_ID = 'initial_ledger_feed_generation'
REGULAR_LEDGER_FEED_GEN_TASK_ID = 'regular_ledger_feed_generation'
INITIAL_RUN = 'initial_run'

AM00_HISTORY_FILE_PREFIX = 'tsys_pcb_mc_acctmaster_daily_am00_rec'
AM02_HISTORY_FILE_PREFIX = 'tsys_pcb_mc_acctmaster_daily_am02_rec'
TRIAD_REPORT_HISTORY_FILE_PREFIX = 'ts2_rptrec'
DEPENDENCIES_NOT_READY = 'noop_dependencies_not_ready'
OUTPUT_PATH = 'output_path'
AM00_PARQUET_LOCATION = 'am00_parquet_location'
AM02_PARQUET_LOCATION = 'am02_parquet_location'
TRIAD_REPORT_PARQUET_LOCATION = 'triad_report_parquet_location'

NONE_FAILED = 'none_failed'
NONE_FAILED_OR_SKIPPED = 'none_failed_min_one_success'

# code
default_args = {
    'owner': 'team-defenders-alerts',
    'capability': 'risk-management',
    'severity': 'P3',
    'sub_capability': 'fraud',
    'business_impact': 'Missing Ledger data will impact FFM Scoring/Adjudicating transactions',
    'customer_impact': 'N/A',
    'depends_on_past': False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False
}


dataproc_config = read_variable_or_file(DATAPROC_CONFIG)
LEDGER_CLUSTER_NAME = get_cluster_name_for_dag(LEDGER_SPARK_BATCH_JOB)

gcp_config = read_variable_or_file(GCP_CONFIG)
deploy_env = gcp_config[DEPLOYMENT_ENVIRONMENT_NAME]
ledger_batch_config = read_variable_or_file(LEDGER_BATCH_CONFIG, deploy_env)
job_history_config = read_variable_or_file(JOB_HISTORY_CONFIG, deploy_env)

# Update LEDGER_TIME depending on env
LEDGER_TIME = 'ledger_time_default'
if deploy_env == 'prod':
    LEDGER_TIME = 'ledger_time_prod'


def build_transaction_fetching_job(p_previous_ledger_date: str, p_current_ledger_date: str, p_ods_folder_path: str):
    arg_list = [APPLICATION_CONF, p_ods_folder_path]
    for k, v in ledger_batch_config[ODS_TRANSACTION_FETCHING_JOB][ARGS].items():
        arg_list.append(f'{k}={v}')

    arg_list.append(f'pcb.bigquery.loader.source.where.condition=CREATE_DT '
                    f'BETWEEN TO_DATE(\'{p_previous_ledger_date}\', \'YYYYMMDDHH24MISS\') '
                    f'AND TO_DATE(\'{p_current_ledger_date}\', \'YYYYMMDDHH24MISS\')')

    return {
        REFERENCE: {PROJECT_ID: dataproc_config.get(PROJECT_ID)},
        PLACEMENT: {CLUSTER_NAME: LEDGER_CLUSTER_NAME},
        SPARK_JOB: {
            JAR_FILE_URIS: ledger_batch_config[ODS_TRANSACTION_FETCHING_JOB][JAR_FILE_URIS],
            MAIN_CLASS: ledger_batch_config[ODS_TRANSACTION_FETCHING_JOB][MAIN_CLASS],
            FILE_URIS: ledger_batch_config[ODS_TRANSACTION_FETCHING_JOB][FILE_URIS],
            ARGS: arg_list
        },
    }


def build_transaction_loading_job(ods_folder_path: str):
    ods_dataset_id = ledger_batch_config[BQ_TRANSACTION_TABLE][DATASET_ID]
    source_folder = ledger_batch_config[BQ_TRANSACTION_TABLE][SOURCE_FOLDER]
    txn_table_name = ledger_batch_config[BQ_TRANSACTION_TABLE][TABLE_NAME]
    parquet_files_path = f"{ods_folder_path}/{source_folder}/*.parquet"

    logging.info(f"Starting import of data for table {txn_table_name} from {parquet_files_path}")

    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET
    )

    bq_table_id = f"{gcp_config[CURATED_ZONE_PROJECT_ID]}.{ods_dataset_id}.{txn_table_name}"

    bq_load_job = bq_client.load_table_from_uri(
        parquet_files_path,
        bq_table_id,
        job_config=job_config
    )
    bq_load_job.result()

    target_table = bq_client.get_table(bq_table_id)
    logging.info(
        f"Finished import of data for table {txn_table_name} from {ods_folder_path}. Imported {target_table.num_rows} rows.")


def assemble_transaction_loading_tasks(upstream_task, downstream_task):
    cluster_creating_task = DataprocCreateClusterOperator(
        task_id=CLUSTER_CREATING_TASK_ID,
        project_id=dataproc_config.get(PROJECT_ID),
        cluster_config=get_cluster_config_by_job_size(deploy_env, gcp_config.get("network_tag"), 'medium'),
        region=dataproc_config.get(LOCATION),
        cluster_name=LEDGER_CLUSTER_NAME,
        trigger_rule=NONE_FAILED_OR_SKIPPED
    )

    previous_ledger_date = f"{{{{ ti.xcom_pull(task_ids='{LEDGER_DATES_CHECK_TASK_ID}', key='{PREVIOUS_LEDGER_DATE}') }}}}"
    current_ledger_date = f"{{{{ ti.xcom_pull(task_ids='{LEDGER_DATES_CHECK_TASK_ID}', key='{CURRENT_LEDGER_DATE}') }}}}"

    bucket_name = ledger_batch_config[PROCESSING_BUCKET_EXTRACT]
    ods_folder_name = ledger_batch_config[ODS_TRANSACTION_FETCHING_JOB][ODS_FOLDER_NAME]

    ods_folder_path = f"gs://{bucket_name}/{ods_folder_name}/partial"

    ods_transactions_fetching_task = DataprocSubmitJobOperator(
        task_id=ODS_TRANSACTIONS_FETCHING_TASK_ID,
        job=build_transaction_fetching_job(previous_ledger_date, current_ledger_date, ods_folder_path),
        region=dataproc_config.get(LOCATION),
        project_id=dataproc_config.get(PROJECT_ID),
        gcp_conn_id=gcp_config.get(PROCESSING_ZONE_CONNECTION_ID),
        trigger_rule=NONE_FAILED_OR_SKIPPED
    )

    bq_transactions_loading_task = PythonOperator(
        task_id=BQ_TRANSACTIONS_LOADING_TASK_ID,
        python_callable=build_transaction_loading_job,
        op_kwargs={
            ODS_FOLDER_PATH: ods_folder_path
        },
        trigger_rule=NONE_FAILED_OR_SKIPPED
    )

    upstream_task >> cluster_creating_task >> ods_transactions_fetching_task >> bq_transactions_loading_task >> downstream_task


def get_previous_ledger_date(ledger_time: str):
    ledger_result = get_dagrun_last('ledger_spark_batch_job')
    num_ledger_rows = ledger_result.total_rows

    if num_ledger_rows == 0:
        print('First ever run of ledger. Returning yesterday date')
        yesterday = date.today() - timedelta(days=1)
        prev_ledger_date = yesterday.strftime(DATE_PATTERN) + ledger_time
    else:
        print('A prior ledger job exists')
        ledger_row = next(iter(ledger_result))
        prev_ledger_date = json.loads(ledger_row['job_params'])['current_ledger_date']

    return prev_ledger_date


def get_ledger_dates(**context):
    # use fixed ledger_time to ease handling of re-run
    ledger_time = ledger_batch_config[ODS_TRANSACTION_FETCHING_JOB][LEDGER_TIME]

    previous_ledger_date = get_previous_ledger_date(ledger_time)
    current_ledger_date = datetime.now().strftime(DATE_PATTERN) + ledger_time

    logging.info(f'Branching: previous_ledger_date={previous_ledger_date}, current_ledger_date={current_ledger_date}')

    context[TASK_INSTANCE].xcom_push(key=PREVIOUS_LEDGER_DATE, value=previous_ledger_date)
    context[TASK_INSTANCE].xcom_push(key=CURRENT_LEDGER_DATE, value=current_ledger_date)
    context[TASK_INSTANCE].xcom_push(key=LEDGER_TIME, value=ledger_time)

    if previous_ledger_date == current_ledger_date:
        logging.info('is rerun. skip fetching transactions')
        return RERUN_NOOP_TASK_ID
    else:
        logging.info('start fetching transactions.')
        return CLUSTER_CREATING_TASK_ID


def get_current_day_job_history_file(history_file_prefix: str):
    storage_client = storage.Client()
    bucket_name = job_history_config[BUCKET_NAME]
    folder_name = job_history_config[FOLDER_NAME]

    blobs = storage_client.list_blobs(bucket_name, prefix=folder_name)

    history_file_name = f'{history_file_prefix}_SUCCESS_{datetime.now().strftime(DATE_PATTERN)}'

    logging.info(f'retrieving history file {history_file_name} from {bucket_name}/{folder_name}')

    for blob in blobs:
        if history_file_name in blob.name:
            return blob


def get_unprocessed_files(**context):

    ledger_result = get_dagrun_last('ledger_spark_batch_job')
    num_ledger_rows = ledger_result.total_rows
    FFM_SEGMENTS = ledger_batch_config[FFM_ACCTM_SEGMENTS]

    if num_ledger_rows == 0:
        print('First ever run of ledger')
        context[TASK_INSTANCE].xcom_push(key=INITIAL_RUN, value=True)

        acctm_result = get_dagrun_last('tsys_pcb_mc_acctmaster_daily')
        triad_result = get_dagrun_last('TS2_rptrec')

        # For the first ever ledger run we need an acctmaster file
        if acctm_result.total_rows == 0:
            print('No prior acct master file. Aborting')
            return (None, [None])
        else:
            acctm_row = next(iter(acctm_result))
            acctm_job_params = json.loads(acctm_row['job_params'])
            acctm_path = acctm_job_params['extract_path']

            acctm_segments = acctm_job_params.get('segments_processed', [])
            print(f'{acctm_path}, {acctm_segments}')
            if set(FFM_SEGMENTS) <= set(acctm_segments):
                print('Initial ledger run: All FFM segments succeeded in AcctMaster')
            else:
                print('Initial ledger run: Not all FFM segments succeeded in AcctMaster')
                return (None, [None])

            if triad_result.total_rows == 0:
                print('No prior Triad file')
                triad_path = ''
            else:
                triad_row = next(iter(triad_result))
                triad_path = json.loads(triad_row['job_params'])['extract_path']
            print(f"{acctm_path}, {triad_path}")
            return acctm_path, [triad_path]
    else:
        print('A prior ledger job exists')
        context[TASK_INSTANCE].xcom_push(key=INITIAL_RUN, value=False)

        ledger_row = next(iter(ledger_result))
        ledger_timestamp = ledger_row['load_timestamp']
        acctm_result = get_dagruns_after('tsys_pcb_mc_acctmaster_daily', ledger_timestamp)

        if acctm_result.total_rows == 0:
            print('No new acct master after the last ledger run')
            return (None, [None])
        else:
            print('One or more new acct master found after the last ledger run')
            acctm_rows = list(acctm_result)
            acctm_job_params = json.loads(acctm_rows[-1]['job_params'])
            acctm_path = acctm_job_params['extract_path']
            acctm_segments = acctm_job_params.get('segments_processed', [])
            print(f'acctm_path: {acctm_path}, acctm_segments: {acctm_segments}')
            if set(FFM_SEGMENTS) <= set(acctm_segments):
                print('All FFM segments succeeded in AcctMaster')
            else:
                print('Not all FFM segments succeeded in AcctMaster')
                return (None, [None])

        triad_result = get_dagruns_after('TS2_rptrec', ledger_timestamp)
        num_triad_rows = triad_result.total_rows

        if num_triad_rows == 0:
            print('No new triad file after the last ledger run. Reusing older one')
            triad_result_last = get_dagrun_last('TS2_rptrec')
            if triad_result_last.total_rows == 0:
                print('No prior triad file found. Upload one first. Logically cant happen')
                return (None, [None])
            triad_row = next(iter(triad_result_last))
            triad_path = json.loads(triad_row['job_params'])['extract_path']
            return acctm_path, [triad_path]
        else:
            print('One or more new triad file(s) found')
            triad_rows = list(triad_result)
            triad_paths = [json.loads(t['job_params'])['extract_path'] for t in triad_rows]
            return acctm_path, triad_paths


def check_dependencies(**context):
    acctm_path, triad_paths = get_unprocessed_files(**context)

    if acctm_path is None or None in triad_paths:
        logging.info('upstream dependency is not ready.')
        return DEPENDENCIES_NOT_READY
    else:
        am00_path = acctm_path + '/AM00_REC/*.parquet'
        am02_path = acctm_path + '/AM02_REC/*.parquet'
        triad_path = ', '.join(triad_paths)

        logging.info(f'am00_path={am00_path}, am02_path={am02_path}, triad_path={triad_path}')

        context[TASK_INSTANCE].xcom_push(key=AM00_PARQUET_LOCATION, value=am00_path)
        context[TASK_INSTANCE].xcom_push(key=AM02_PARQUET_LOCATION, value=am02_path)
        context[TASK_INSTANCE].xcom_push(key=TRIAD_REPORT_PARQUET_LOCATION, value=triad_path)

        return LEDGER_DATES_CHECK_TASK_ID


def build_ledger_spark_batch_job(am00_parquet_location: str,
                                 am02_parquet_location: str,
                                 triad_report_parquet_location: str,
                                 previous_ledger_date: str,
                                 current_ledger_date: str,
                                 env: str,
                                 initial_run: bool):
    arg_list = [
        f'pcb.ledger.processor.accountmaster.locations.am00={am00_parquet_location}',
        f'pcb.ledger.processor.accountmaster.locations.am02={am02_parquet_location}',
        f'pcb.ledger.processor.triad.report.location={triad_report_parquet_location}',
        f'pcb.ledger.processor.ledger.date.previous={previous_ledger_date}',
        f'pcb.ledger.processor.ledger.date.current={current_ledger_date}',
        f'pcb.ledger.processor.bigquery.project={gcp_config[CURATED_ZONE_PROJECT_ID]}',
        f'pcb.ledger.processor.ods-replica.dataset={ledger_batch_config[BQ_TRANSACTION_TABLE][DATASET_ID]}',
        f'pcb.ledger.processor.ledger.dataset={ledger_batch_config[BQ_LEDGER_HISTORY_TABLE][DATASET_ID]}',
        f'pcb.ledger.processor.domain-account-management.dataset={ledger_batch_config[BQ_EVE_DATASETS][BQ_DOMAIN_ACCOUNT_MANAGEMENT_DATASET]}',
        f'pcb.ledger.processor.domain-customer-management.dataset={ledger_batch_config[BQ_EVE_DATASETS][BQ_DOMAIN_CUSTOMER_MANAGEMENT_DATASET]}',
        f'pcb.ledger.processor.job.history.path=gs://{job_history_config[BUCKET_NAME]}/{job_history_config[FOLDER_NAME]}/',
        f'pcb.ledger.processor.initial={initial_run}'
    ]

    return {
        REFERENCE: {PROJECT_ID: dataproc_config.get(PROJECT_ID)},
        PLACEMENT: {CLUSTER_NAME: LEDGER_CLUSTER_NAME},
        SPARK_JOB: {
            JAR_FILE_URIS: ledger_batch_config[LEDGER_SPARK_BATCH_JOB][JAR_FILE_URIS],
            FILE_URIS: ledger_batch_config[LEDGER_SPARK_BATCH_JOB][FILE_URIS],
            MAIN_CLASS: ledger_batch_config[LEDGER_SPARK_BATCH_JOB][MAIN_CLASS],
            ARGS: arg_list,
            PROPERTIES: ledger_batch_config[LEDGER_SPARK_BATCH_JOB][PROPERTIES],
        },
    }


def build_ledger_spark_batch_task(task_id: str, initial_run: bool):
    am00_parquet_location = f"{{{{ ti.xcom_pull(task_ids='{DEPENDENCIES_VERIFICATION_TASK_ID}', key='{AM00_PARQUET_LOCATION}') }}}}"
    am02_parquet_location = f"{{{{ ti.xcom_pull(task_ids='{DEPENDENCIES_VERIFICATION_TASK_ID}', key='{AM02_PARQUET_LOCATION}') }}}}"
    triad_report_parquet_location = f"{{{{ ti.xcom_pull(task_ids='{DEPENDENCIES_VERIFICATION_TASK_ID}', key='{TRIAD_REPORT_PARQUET_LOCATION}') }}}}"
    previous_ledger_date = f"{{{{ ti.xcom_pull(task_ids='{LEDGER_DATES_CHECK_TASK_ID}', key='{PREVIOUS_LEDGER_DATE}') }}}}"
    current_ledger_date = f"{{{{ ti.xcom_pull(task_ids='{LEDGER_DATES_CHECK_TASK_ID}', key='{CURRENT_LEDGER_DATE}') }}}}"

    return DataprocSubmitJobOperator(
        task_id=task_id,
        job=build_ledger_spark_batch_job(am00_parquet_location,
                                         am02_parquet_location,
                                         triad_report_parquet_location,
                                         previous_ledger_date,
                                         current_ledger_date,
                                         deploy_env,
                                         initial_run),
        region=dataproc_config.get(LOCATION),
        project_id=dataproc_config.get(PROJECT_ID),
        gcp_conn_id=gcp_config.get(PROCESSING_ZONE_CONNECTION_ID),
        trigger_rule=NONE_FAILED_OR_SKIPPED
    )


def is_initial_run(**context):
    initial_run_flag = context[TASK_INSTANCE].xcom_pull(task_ids=DEPENDENCIES_VERIFICATION_TASK_ID, key=INITIAL_RUN)
    if ledger_batch_config["force_initial_run"]:
        initial_run_flag = True

    if initial_run_flag:
        logging.info("Start initial run of ledger job.")
        return INITIAL_LEDGER_FEED_GEN_TASK_ID
    else:
        logging.info("Start regular run of ledger job.")
        return REGULAR_LEDGER_FEED_GEN_TASK_ID


def build_ledger_history_loading_job(**context):
    current_ledger_date = context[TASK_INSTANCE].xcom_pull(task_ids=LEDGER_DATES_CHECK_TASK_ID, key=CURRENT_LEDGER_DATE)
    ledger_time = context[TASK_INSTANCE].xcom_pull(task_ids=LEDGER_DATES_CHECK_TASK_ID, key=LEDGER_TIME)
    length = len(ledger_time)

    ledger_dataset_id = ledger_batch_config[BQ_LEDGER_HISTORY_TABLE][DATASET_ID]
    ledger_history_table_name = ledger_batch_config[BQ_LEDGER_HISTORY_TABLE][TABLE_NAME]
    parquet_files_path = f"gs://{job_history_config[BUCKET_NAME]}/{job_history_config[FOLDER_NAME]}/ACCOUNT_LEDGER_DATES/{current_ledger_date[:-length]}/*.parquet"

    logging.info(f"Starting import of data for table {ledger_history_table_name} from {parquet_files_path}")

    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET
    )

    bq_table_id = f"{gcp_config[CURATED_ZONE_PROJECT_ID]}.{ledger_dataset_id}.{ledger_history_table_name}"

    bq_load_job = bq_client.load_table_from_uri(
        parquet_files_path,
        bq_table_id,
        job_config=job_config
    )
    bq_load_job.result()

    target_table = bq_client.get_table(bq_table_id)
    logging.info(
        f"Finished import of data for table {ledger_history_table_name} from {parquet_files_path}. Imported {target_table.num_rows} rows.")


def build_save_dag_to_bq_task():
    job_params_str = json.dumps(
        {
            "am00_parquet_location": f"{{{{ ti.xcom_pull(task_ids='{DEPENDENCIES_VERIFICATION_TASK_ID}', key='{AM00_PARQUET_LOCATION}') }}}}",
            "am02_parquet_location": f"{{{{ ti.xcom_pull(task_ids='{DEPENDENCIES_VERIFICATION_TASK_ID}', key='{AM02_PARQUET_LOCATION}') }}}}",
            "triad_report_parquet_location": f"{{{{ ti.xcom_pull(task_ids='{DEPENDENCIES_VERIFICATION_TASK_ID}', key='{TRIAD_REPORT_PARQUET_LOCATION}') }}}}",
            "previous_ledger_date": f"{{{{ ti.xcom_pull(task_ids='{LEDGER_DATES_CHECK_TASK_ID}', key='{PREVIOUS_LEDGER_DATE}') }}}}",
            "current_ledger_date": f"{{{{ ti.xcom_pull(task_ids='{LEDGER_DATES_CHECK_TASK_ID}', key='{CURRENT_LEDGER_DATE}') }}}}"
        }
    )
    return PythonOperator(
        task_id='save_job_to_control_table',
        python_callable=save_job_to_control_table,
        op_kwargs={"job_params": job_params_str},
        retries=0
    )


ledger_time = ledger_batch_config[ODS_TRANSACTION_FETCHING_JOB][LEDGER_TIME]
l_hour, l_minute = ledger_time[0:2], ledger_time[2:4]
sch_cron_str = f'{l_minute} {l_hour} * * *'

local_tz = pendulum.timezone('America/Toronto')
severity_tag = default_args['severity']


with DAG(
        LEDGER_SPARK_BATCH_JOB,
        default_args=default_args,
        description='Ledger batch scheduler DAG',
        schedule=sch_cron_str,
        is_paused_upon_creation=True,
        start_date=pendulum.today('America/Toronto').add(days=-2),
        tags=[severity_tag],
        catchup=False,
        dagrun_timeout=timedelta(minutes=180)
) as dag:
    start_point = EmptyOperator(task_id="start")
    end_point = EmptyOperator(task_id="end", trigger_rule=NONE_FAILED)

    dependencies_verification_task = BranchPythonOperator(
        task_id=DEPENDENCIES_VERIFICATION_TASK_ID,
        python_callable=check_dependencies,
        trigger_rule=NONE_FAILED_OR_SKIPPED)

    ledger_dates_check_task = BranchPythonOperator(
        task_id=LEDGER_DATES_CHECK_TASK_ID,
        python_callable=get_ledger_dates,
        trigger_rule=NONE_FAILED_OR_SKIPPED)

    run_type_check_task = BranchPythonOperator(
        task_id=RUN_TYPE_CHECK_TASK_ID,
        python_callable=is_initial_run,
        trigger_rule=NONE_FAILED_OR_SKIPPED)

    noop_dep_not_ready = EmptyOperator(task_id=DEPENDENCIES_NOT_READY)

    skipping = EmptyOperator(task_id=RERUN_NOOP_TASK_ID)

    initial_ledger_run_task = build_ledger_spark_batch_task(INITIAL_LEDGER_FEED_GEN_TASK_ID, True)
    regular_ledger_run_task = build_ledger_spark_batch_task(REGULAR_LEDGER_FEED_GEN_TASK_ID, False)

    bq_ledger_history_loading_task = PythonOperator(
        task_id=BQ_LEDGER_HISTORY_LOADING_TASK_ID,
        python_callable=build_ledger_history_loading_job,
        trigger_rule=NONE_FAILED_OR_SKIPPED
    )

    save_dag_to_bq_task = build_save_dag_to_bq_task()

    run_type_check_task >> [initial_ledger_run_task, regular_ledger_run_task] >> bq_ledger_history_loading_task >> save_dag_to_bq_task >> end_point

    assemble_transaction_loading_tasks(ledger_dates_check_task, run_type_check_task)

    dependencies_verification_task >> noop_dep_not_ready >> end_point

    start_point >> dependencies_verification_task >> ledger_dates_check_task >> skipping >> end_point
