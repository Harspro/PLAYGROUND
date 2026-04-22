import pendulum
from datetime import datetime
from google.cloud import bigquery
from util.bq_utils import (
    bg_query_success
)


def get_audit_fields(file_path):
    # set audit fields
    datetime_now = datetime.now(pendulum.timezone('America/Toronto'))
    rec_cc_tms = datetime_now.strftime('%Y-%m-%dT%H:%M:%S')
    batch_run_log_uid = datetime_now.strftime('%Y%m%d%H%M%S')
    rec_chng_id = file_path.split('/')[1]
    audit_fields = '|' + rec_cc_tms + '|' + rec_cc_tms + '|' + rec_chng_id + '|' + batch_run_log_uid
    print(audit_fields)
    return audit_fields


def get_dtl_line(line, date_flds, ignor_flds, first_flds, date_frmt, num_of_flds):
    conv_line = ""
    split_line = line.split('|', 2)
    detail_item = split_line[2].split('|', num_of_flds - 2)
    for i in range(0, num_of_flds - 2):
        if i in date_flds and detail_item[i].strip() != "":
            detail_item[i] = datetime.strptime(detail_item[i], date_frmt).strftime("%Y-%m-%d %H:%M:%S")
        if i in first_flds:
            conv_line = conv_line + detail_item[i]
        if i not in ignor_flds:
            conv_line = conv_line + "|" + detail_item[i]
    return conv_line


def bq_table_load(table_name, table_schema, file_path, project_id, dataset_name, staging_bucket):
    client = bigquery.Client()
    table_id = project_id + '.' + dataset_name + '.' + table_name
    source_file_uri = "gs://{}/{}".format(staging_bucket, file_path)

    job_config = bigquery.LoadJobConfig(
        schema=table_schema,
        write_disposition='WRITE_APPEND',
        source_format='CSV',
        field_delimiter='|'
    )

    job = client.load_table_from_uri(source_file_uri, table_id, job_config=job_config)
    results = job.result()
    print(results)
    bg_query_success(job)
