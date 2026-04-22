import pytz
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator
)

from util.miscutils import (
    read_variable_or_file,
    get_cluster_name,
    get_ephemeral_cluster_config
)

import util.constants as consts

dataproc_config = read_variable_or_file("dataproc_config")
gcp_config = read_variable_or_file("gcp_config")
deploy_env = gcp_config['deployment_environment_name']

dags_config = read_variable_or_file("db_tran_processing/db_tran_config", deploy_env)

# Values from config file
processing_bucket = dags_config["processing_bucket"]
region = dags_config["region"]
java_main_class = dags_config["main_class"]
jar_file_uris = dags_config["jar_file_uris"]
loader_file_uris = dags_config["loader_file_uris"]
generator_file_uris = dags_config["generator_file_uris"]
batch_auth_folder_name = dags_config["batch_auth_folder_name"]
cardguard_auth_folder_name = dags_config["cardguard_auth_folder_name"]

# Change number of worked nodes to 6
cluster_config = get_ephemeral_cluster_config(deploy_env, gcp_config.get("network_tag"))
cluster_config["worker_config"]["num_instances"] = 6

is_ephemeral = True
DAG_ID = 'db_tran_auth'
cluster_name = get_cluster_name(is_ephemeral, dataproc_config, dag_id=DAG_ID)

# Current Date
utc_now = pytz.utc.localize(datetime.utcnow())
time_est = utc_now.astimezone(pytz.timezone("America/Toronto"))
time_est_minus_1 = utc_now.astimezone(pytz.timezone("America/Toronto")) - timedelta(1)

current_date = time_est.strftime("%Y%m%d")
current_date_minus_1 = time_est_minus_1.strftime("%Y%m%d")

if deploy_env == "prod":
    file_extension = dags_config["file_extension_prod"]
    batch_auth_date = current_date_minus_1
    feed_generator_date = current_date_minus_1
    card_guard_auth_date = current_date
    inbound_bucket = dags_config["inbound_bucket_prod"]
    schedule_interval_value = "00 05 * * 2-6"
else:
    file_extension = dags_config["file_extension_lower"]
    batch_auth_date = current_date
    feed_generator_date = current_date
    card_guard_auth_date = current_date
    inbound_bucket = dags_config["inbound_bucket"]
    schedule_interval_value = "00 17 * * 1-5"

'''
Scheduled run:
UAT: batch auth log runs with current date , card guard auth runs with current date and feed generator runs with current date
PROD: batch auth log runs with t-1 , card guard auth runs with current date and feed generator runs with t-1, where t is current date
For re run , over ride the date parameters by using this DAG config - {"ba_date":"[date]","cga_date":"[date]","fg_date":"[date]"}
'''
batch_auth_run_date = "{{ dag_run.conf.get('ba_date') or " + str(batch_auth_date) + " }}"
card_guard_auth_run_date = "{{ dag_run.conf.get('cga_date') or " + str(card_guard_auth_date) + " }}"
feed_generator_run_date = "{{ dag_run.conf.get('fg_date') or " + str(feed_generator_date) + " }}"

DAG_DEFAULT_ARGS = {
    "owner": "team-defenders-alerts",
    'capability': 'risk-management',
    'severity': 'P3',
    'sub_capability': 'fraud',
    'business_impact': 'missing DBTRAN off-us Authorizations for cases',
    'customer_impact': 'N/A',
    "start_date": pendulum.today('America/Toronto').add(days=-1),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True
}

severity_tag = DAG_DEFAULT_ARGS['severity']


dag = DAG(
    dag_id=DAG_ID,
    schedule=schedule_interval_value,
    tags=[severity_tag],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=3,
    default_args=DAG_DEFAULT_ARGS
)

crdg_auth_prefix = "{{ dag_run.conf.get('crdg_auth_prefix') or 'tsys_pcb_crdg_auth_' }}"
batch_auth_prefix = "{{ dag_run.conf.get('batch_auth_prefix') or 'tsys_pcb_batchauth_' }}"

card_guard_auth_file_prefix = f"TS2_crdg_auth/{crdg_auth_prefix}" + card_guard_auth_run_date
batch_auth_file_prefix = f"TS2_batchauth/{batch_auth_prefix}" + batch_auth_run_date

poll_card_gaurd_auth_files = GCSObjectsWithPrefixExistenceSensor(
    task_id='poll_card_gaurd_auth_files',
    google_cloud_conn_id=gcp_config.get("landing_zone_connection_id"),
    bucket=inbound_bucket,
    prefix=card_guard_auth_file_prefix,
    poke_interval=5 * 60,
    timeout=24 * 60 * 60,
    dag=dag)

poll_batch_auth_files = GCSObjectsWithPrefixExistenceSensor(
    task_id='poll_batch_auth_files',
    google_cloud_conn_id=gcp_config.get("landing_zone_connection_id"),
    bucket=inbound_bucket,
    prefix=batch_auth_file_prefix,
    poke_interval=5 * 60,
    timeout=24 * 60 * 60,
    dag=dag)

move_batchauth_landing_to_processing = GCSToGCSOperator(
    task_id='move_batchauth_file_to_processed',
    gcp_conn_id=gcp_config.get("landing_zone_connection_id"),
    source_bucket=inbound_bucket,
    source_objects=[batch_auth_file_prefix],
    destination_bucket=processing_bucket,
    match_glob="**/*" + file_extension,
    dag=dag)

move_cardguard_landing_to_processing = GCSToGCSOperator(
    task_id='move_cardguard_file_to_processed',
    gcp_conn_id=gcp_config.get("landing_zone_connection_id"),
    source_bucket=inbound_bucket,
    source_objects=[card_guard_auth_file_prefix],
    destination_bucket=processing_bucket,
    match_glob="**/*" + file_extension,
    dag=dag)

create_dataproc_cluster = DataprocCreateClusterOperator(
    task_id="create_dataproc_cluster",
    project_id=gcp_config.get("processing_zone_project_id"),
    cluster_config=cluster_config,
    region=region,
    cluster_name=cluster_name,
    dag=dag)

dag_owner = DAG_DEFAULT_ARGS['owner']
dag_info = f"[dag_name: {'{{dag.dag_id}}'}, dag_run_id: {'{{run_id}}'}, dag_owner: {dag_owner}]"

batch_auth_spark_job_config = {
    "reference": {"project_id": dataproc_config.get('project_id')},
    "placement": {"cluster_name": cluster_name},
    "spark_job": {
        "jar_file_uris": jar_file_uris,
        "main_class": java_main_class,
        "file_uris": loader_file_uris,
        "args": [
            "execution.type=BATCH_AUTH_LOG_LOADER",
            "config.file.name=application.conf",
            f'execution.date={str(batch_auth_run_date)}',
            f'{consts.SPARK_DAG_INFO}={dag_info}'
        ]
    },
}

card_guard_auth_job_config = {
    "reference": {"project_id": dataproc_config.get('project_id')},
    "placement": {"cluster_name": cluster_name},
    "spark_job": {
        "jar_file_uris": jar_file_uris,
        "main_class": java_main_class,
        "file_uris": loader_file_uris,
        "args": [
            "execution.type=CARD_GUARD_AUTH_LOG_LOADER",
            "config.file.name=application.conf",
            f'execution.date={str(card_guard_auth_run_date)}',
            f'{consts.SPARK_DAG_INFO}={dag_info}'
        ]
    },
}

feed_generator_job_config = {
    "reference": {"project_id": dataproc_config.get('project_id')},
    "placement": {"cluster_name": cluster_name},
    "spark_job": {
        "jar_file_uris": jar_file_uris,
        "main_class": java_main_class,
        "file_uris": generator_file_uris,
        "args": [
            "execution.type=DBTRAN_AUTH_FEED_GENERATOR",
            "config.file.name=application.conf",
            f'execution.date={str(feed_generator_run_date)}',
            f'{consts.SPARK_DAG_INFO}={dag_info}'
        ]
    },
}

submit_batch_auth_spark_job = DataprocSubmitJobOperator(
    task_id="batchguardauth_job",
    job=batch_auth_spark_job_config,
    region=region,
    project_id=gcp_config.get("processing_zone_project_id"),
    dag=dag
)

submit_card_auth_spark_job = DataprocSubmitJobOperator(
    task_id="cardguardauth_job",
    job=card_guard_auth_job_config,
    region=region,
    project_id=gcp_config.get("processing_zone_project_id"),
    dag=dag
)

submit_feed_generator_spark_job = DataprocSubmitJobOperator(
    task_id="feedgenerator_job",
    job=feed_generator_job_config,
    region=region,
    project_id=gcp_config.get("processing_zone_project_id"),
    dag=dag
)

(
    [poll_batch_auth_files >> move_batchauth_landing_to_processing,
     poll_card_gaurd_auth_files >> move_cardguard_landing_to_processing] >> create_dataproc_cluster >> [submit_batch_auth_spark_job,
                                                                                                        submit_card_auth_spark_job] >> submit_feed_generator_spark_job
)
