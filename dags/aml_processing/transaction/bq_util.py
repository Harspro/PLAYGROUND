import logging
from google.cloud import bigquery


def run_bq_query(sql_query, job_config=None, timeout_seconds=60 * 30):
    sql = sql_query
    client = bigquery.Client()
    query_job = client.query(sql, job_config=job_config)
    query_job.result(timeout=timeout_seconds, retry=None)
    if query_job.done() and query_job.error_result is None:
        logging.info("Query job completed successfully")

    return query_job


def run_bq_dml_with_log(pre: str, post: str, sql: str, job_config: bigquery.QueryJobConfig = None):
    logging.info(f"{pre},SQL:${sql}")
    query_job = run_bq_query(sql, job_config)
    logging.info("Affected rows: " + str(query_job.num_dml_affected_rows))
    logging.info(f"{post}")


def run_bq_select(sql_select: str, job_config: bigquery.QueryJobConfig = None) -> bigquery.table.RowIterator:
    sql = sql_select
    client = bigquery.Client()
    logging.info(f"Running select SQL:\n ${sql}")
    query_job = client.query(sql, job_config=job_config)
    results = query_job.result()
    if query_job.done() and query_job.error_result is None:
        logging.info("Query job completed successfully")
    else:
        logging.error(f"BQ select failed:${query_job.errors}\nsql:${sql}")
    return results
