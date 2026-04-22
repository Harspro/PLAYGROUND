import logging
from typing import List
from util.miscutils import read_variable_or_file
from datetime import datetime, date
from google.cloud import bigquery
from aml_processing.transaction.agg_domain_at31 import AT31_TRANSACTION_TABLE, BQTABLE_TRANSACTION_AGG_CONTROL
from aml_processing.transaction.bq_util import run_bq_dml_with_log

############################################################
# Configs and constants
############################################################
DEPLOY_ENV = read_variable_or_file('gcp_config')['deployment_environment_name']


def log_job_start(dag_run_id: str, start_date: date, end_date: date):
    # dag_run_id = context['run_id']
    if start_date is not None and end_date is not None:
        logging.info(f'Loading for date range:{start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}')
        sql = f"""INSERT INTO `{BQTABLE_TRANSACTION_AGG_CONTROL}`(
              RUN_ID ,
              FILE_CREATE_DT ,
              REC_LOAD_TIMESTAMP ,
              JOB_START_TIME ,
              STATUS
             )
             SELECT DISTINCT
                  @dag_run_id,
                  FILE_CREATE_DT,
                  REC_LOAD_TIMESTAMP,
                  CURRENT_DATETIME(),
                  'S'
             FROM {AT31_TRANSACTION_TABLE}  t
             WHERE   t.file_create_dt between @start_date and @end_date
             """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dag_run_id", "STRING", dag_run_id),
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date.strftime("%Y-%m-%d")),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date.strftime("%Y-%m-%d"))
            ]
        )
    else:
        sql = f"""INSERT INTO `{BQTABLE_TRANSACTION_AGG_CONTROL}`(
                  RUN_ID ,
                  FILE_CREATE_DT ,
                  REC_LOAD_TIMESTAMP ,
                  JOB_START_TIME ,
                  STATUS
                 )
                 SELECT DISTINCT
                      @dag_run_id,
                      FILE_CREATE_DT,
                      REC_LOAD_TIMESTAMP,
                      CURRENT_DATETIME(),
                      'S'
                 FROM {AT31_TRANSACTION_TABLE}  t
                 WHERE NOT EXISTS ( SELECT 1
                                    FROM `{BQTABLE_TRANSACTION_AGG_CONTROL}`  c
                                    WHERE c.FILE_CREATE_DT = t.FILE_CREATE_DT
                                         AND c.REC_LOAD_TIMESTAMP = t.REC_LOAD_TIMESTAMP
                                         AND c.STATUS = 'C'
                                   )
                --   AND T.FILE_CREATE_DT >= DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH)
            """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dag_run_id", "STRING", dag_run_id)
            ]
        )
    run_bq_dml_with_log('Finding unprocessed feeds', 'Completed finding unprocessed feeds', sql, job_config)

    sql_update_latest_ver = f"""
     UPDATE `{BQTABLE_TRANSACTION_AGG_CONTROL}` C
     SET C.LATEST_VERSION = CASE WHEN CR.R_LATEST = 1 THEN 'Y' ELSE 'N' END
     FROM (
           SELECT RUN_ID
                ,FILE_CREATE_DT
                ,REC_LOAD_TIMESTAMP
               ,RANK() OVER(PARTITION BY FILE_CREATE_DT ORDER BY REC_LOAD_TIMESTAMP DESC) R_LATEST
           FROM `{BQTABLE_TRANSACTION_AGG_CONTROL}`
         ) CR
     WHERE CR.FILE_CREATE_DT = C.FILE_CREATE_DT
          AND CR.REC_LOAD_TIMESTAMP= C.REC_LOAD_TIMESTAMP
          AND C.RUN_ID = CR.RUN_ID
          AND C.RUN_ID = @dag_run_id
     """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("dag_run_id", "STRING", dag_run_id)

        ]
    )
    run_bq_dml_with_log('updating latest version flag', 'Completed updating later version flag.',
                        sql_update_latest_ver, job_config)
    return


def log_job_end(dag_run_id: str):
    sql = f""" UPDATE {BQTABLE_TRANSACTION_AGG_CONTROL}
               SET JOB_END_TIME = CURRENT_DATETIME(),
                    STATUS = 'C'
               WHERE RUN_ID = @dag_run_id
                     AND JOB_END_TIME IS NULL
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("dag_run_id", "STRING", dag_run_id)

        ]
    )
    run_bq_dml_with_log('logging job complete', 'Completed logging job end.', sql, job_config)


def log_task_start(dag_run_id: str, file_create_dt_list: List[datetime.date]):
    sql = f""" UPDATE {BQTABLE_TRANSACTION_AGG_CONTROL}
               SET JOB_START_TIME = CURRENT_DATETIME(),
                    STATUS = 'S'
               WHERE RUN_ID = @dag_run_id
                and FILE_CREATE_DT in UNNEST (@file_create_dt_list)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("dag_run_id", "STRING", dag_run_id),
            bigquery.ArrayQueryParameter("file_create_dt_list", "DATE", file_create_dt_list)

        ]
    )
    run_bq_dml_with_log("Updating control table task start", "Competed updating control table task start", sql,
                        job_config)


def log_task_complete(dag_run_id: str, file_create_dt_list: List[datetime.date]):
    sql = f""" UPDATE {BQTABLE_TRANSACTION_AGG_CONTROL}
               SET JOB_END_TIME = CURRENT_DATETIME(),
                    STATUS = 'C'
               WHERE RUN_ID = @dag_run_id
               and FILE_CREATE_DT  in UNNEST (@file_create_dt_list)
             """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("dag_run_id", "STRING", dag_run_id),
            bigquery.ArrayQueryParameter("file_create_dt_list", "DATE", file_create_dt_list)
        ]
    )
    run_bq_dml_with_log("Updating control table task complete", "Competed updating control table task complete", sql,
                        job_config)
