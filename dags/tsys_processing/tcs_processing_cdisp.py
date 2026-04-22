from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from util.miscutils import read_variable_or_file
from util.gcs_utils import (
    delete_blobs,
    read_file_latin,
    write_file,
    list_blobs_with_prefix
)
from tsys_processing.tcs_processing_common import (
    bq_table_load,
    get_dtl_line,
    get_audit_fields
)
from dag_factory.terminus_dag_factory import add_tags

DEPLOY_ENV = read_variable_or_file('gcp_config')['deployment_environment_name']
BQ_DATASET = "domain_dispute"
DAG_NAME = "tcs_processing"
PROJECT_ID = f"pcb-{DEPLOY_ENV}-landing"
STAGING_BUCKET = f"pcb-{DEPLOY_ENV}-staging"
TCS_CASEDATA_FILE_PREFIX = "tsys_pcb_pcmc_tcs_casedata_"
TCS_CASEDISPOSITION_FILE_PREFIX = "tsys_pcb_pcmc_tcs_casedisposition_"
TCS_WORKBASKET_FILE_PREFIX = "tsys_pcb_pcmc_tcs_workbasket_"
TABLE_TCS_DISPOSITION_RPT = "TCS_DISPOSITION_RPT"
FIELDS_501 = 11


SCHEMA_TCS_DISPOSITION_RPT = [
    bigquery.SchemaField("INTERACTIONID", "STRING", "NULLABLE"),
    bigquery.SchemaField("INTCREATEDATETIME", "DATETIME", "NULLABLE"),
    bigquery.SchemaField("INTCOMPLETIONDATETIME", "DATETIME", "NULLABLE"),
    bigquery.SchemaField("CREATEOPERATOR", "STRING", "NULLABLE"),
    bigquery.SchemaField("PRIMARYACTIVITY", "STRING", "NULLABLE"),
    bigquery.SchemaField("SECONDARYACTIVITY", "STRING", "NULLABLE"),
    bigquery.SchemaField("TSYSACCOUNTID", "STRING", "NULLABLE"),
    bigquery.SchemaField("CPC", "STRING", "NULLABLE"),
    bigquery.SchemaField("TPC", "STRING", "NULLABLE"),
    bigquery.SchemaField("REC_CREATE_TMS", "DATETIME", "NULLABLE"),
    bigquery.SchemaField("REC_CHNG_TMS", "DATETIME", "NULLABLE"),
    bigquery.SchemaField("REC_CHNG_ID", "STRING", "NULLABLE"),
    bigquery.SchemaField("BATCH_RUN_LOG_UID", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("FILE_HDR_DT", "DATE", "REQUIRED")
]


def process_tcs_casedisposition_file(**context):
    gcs_bucket = context["dag_run"].conf["bucket"]
    file_path = context["dag_run"].conf["name"]
    print("gcs_bucket = " + gcs_bucket + " file_path = " + file_path)

    audit_fields = get_audit_fields(file_path)

    v_501 = ""
    cntr = 0

    lines = read_file_latin(gcs_bucket, file_path)
    print("File read. Starting transformation...")

    hdr_date = lines.split('\n', 1)[0].split('|', 2)[1].split(' ', 2)[0]
    est_rec = (len(lines) - 75) / 115
    out_min = datetime.now().minute
    date_frmt = "%d-%b-%Y %H:%M:%S"
    for line in lines.splitlines(True):
        sline = line.split('|', 2)
        if sline[0] == "001":
            v_hdr = line
        if sline[0] == "999":
            v_tlr = line
        if sline[0] != "001" and sline[0] != "999":
            date_flds = {1, 2}
            ignor_flds = {0}
            first_flds = {0}

            new_line = get_dtl_line(line, date_flds, ignor_flds, first_flds, date_frmt, FIELDS_501)
            v_501 = v_501 + new_line.rstrip('\n') + audit_fields + '|' + hdr_date + '\n'

            cntr += 1
            if out_min != datetime.now().minute:
                print(str(cntr) + " out of approx. " + str(int(est_rec)))
                out_min = datetime.now().minute

    write_file(STAGING_BUCKET, file_path + '_hdr', v_hdr)
    write_file(STAGING_BUCKET, file_path + '_tlr', v_tlr)
    write_file(STAGING_BUCKET, file_path + '_501', v_501)

    bq_table_load(table_name=TABLE_TCS_DISPOSITION_RPT, table_schema=SCHEMA_TCS_DISPOSITION_RPT, file_path=file_path + '_501', project_id=PROJECT_ID, dataset_name=BQ_DATASET, staging_bucket=STAGING_BUCKET)

    list_blobs = list_blobs_with_prefix(STAGING_BUCKET, file_path + '_', delimiter=None)
    delete_blobs(STAGING_BUCKET, list_blobs)


DAG_DEFAULT_ARGS = {
    "owner": "team-digital-adoption-alerts",
    "capability": "payments",
    "sub_capability": "EMT",
    "business_impact": "Daily orchestration process of TCS Case Disposition file not done",
    "customer_impact": "None",
    "severity": "P3",
    "start_date": datetime(2023, 1, 1),
    "depends_on_past": False,
    "retries": 0,
    "max_active_runs": 10
}

with DAG(
    dag_id="tcs_casedisposition_processing",
    default_args=DAG_DEFAULT_ARGS,
    schedule=None,
    is_paused_upon_creation=True,
    description="DAG to load TCS tables."
) as dag:

    add_tags(dag)

    pipeline_start = EmptyOperator(
        task_id="pipeline_start"
    )

    load_tcs_casedisposition_in_bq = PythonOperator(
        task_id="load_tcs_casedisposition_in_bq",
        python_callable=process_tcs_casedisposition_file,
        trigger_rule="all_success"
    )

    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule="all_success"
    )

    pipeline_start >> load_tcs_casedisposition_in_bq >> pipeline_complete
