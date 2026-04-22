# TSYS TRIAD Variable Limits PCMM-1853
import logging
import pendulum
import util.constants as consts
import copy
from abc import ABC
from airflow import settings, DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator
)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowFailException
from datetime import timedelta, datetime
from google.cloud import bigquery
from util.bq_utils import (
    run_bq_query,
    create_external_table,
    count_records_tbl
)
from util.miscutils import (
    read_file_env,
    read_variable_or_file,
    read_yamlfile_env
)
from typing import Dict
from dag_factory.terminus_dag_factory import add_tags

doc_md = """
### A DAG to calculate PCMA funds transfer limits

- The source data is the CDA segment of the TRIAD file.
- CDA data is parsed by another DAG and stored in BigQuery as `pcb-{env}-landing.domain_account_management.TSYS_CDA_OUTCOMES_TRIAD_RPT`
- This DAG gets automatically triggered once the prior DAG finishes processing.
- This DAG suports calculation of limits from any date in the table. Trigger with the following example conf.
````
{"file_create_dt": "2023-09-27"}
````
"""


class TriadVariableLimitsLoader(ABC):
    def __init__(self):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.config_dir = f'{settings.DAGS_FOLDER}/config'
        self.dag_config_fname = f"{self.config_dir}/tsys_triad_limits_config.yaml"
        self.job_config = read_yamlfile_env(f"{self.dag_config_fname}", self.deploy_env)
        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)
        self.default_args = {
            "owner": "team-money-movement-eng",
            "depends_on_past": False,
            "wait_for_downstream": False,
            "retries": 0,
            "retry_delay": timedelta(seconds=10),
            'capability': 'Payments',
            'severity': 'P3',
            'sub_capability': '<tbd>',  # TODO
            'business_impact': '<tbd>',
            'customer_impact': '<tbd>'
        }
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)

    def get_file_create_dt_bq(self, ext_bq_table: str):
        client = bigquery.Client()
        QUERY_SQL = f"""
            SELECT FILE_CREATE_DT
            FROM `{ext_bq_table}`
            ORDER BY FILE_CREATE_DT
            LIMIT 1;
        """
        query_job = client.query(QUERY_SQL)
        bq_result = query_job.result()
        if bq_result.total_rows > 0:
            result_row = next(bq_result)
            file_hdr_dt = str(result_row['FILE_CREATE_DT'])
        else:
            file_hdr_dt = ''
            raise AirflowFailException(f"FILE_CREATE_DT not found in ext table: {ext_bq_table}")

        return file_hdr_dt

    def get_file_create_dt(self, ext_bq_table: str, **context):
        file_create_dt = context['dag_run'].conf.get('file_create_dt')
        if file_create_dt:
            logging.info('File hdr date read from dag run conf:')
        else:
            logging.info(f"Getting file hdr date from BQ ext table: {ext_bq_table}")
            file_create_dt = self.get_file_create_dt_bq(ext_bq_table)
        logging.info(f'File create dt is {file_create_dt}')
        return file_create_dt

    def calculate_limits(self, cda_filepath: str, sql_file_path: str, ext_bq_table: str, cda_table_name: str, output_path: str, **context):
        # First load the cda file into an ext bq table
        bq_client = bigquery.Client()
        logging.info(f"tablename: {ext_bq_table}")
        create_external_table(bq_client, ext_bq_table, cda_filepath, expiration_hours=1)

        create_dt = self.get_file_create_dt(ext_bq_table, **context)
        logging.info(f'Calculating limits from {cda_table_name} using sql file {sql_file_path} and output to {output_path}')
        sql = read_file_env(f"{settings.DAGS_FOLDER}/" + sql_file_path, self.deploy_env)
        sql = sql.replace('{file_create_dt}', create_dt)
        sql = sql.replace('{cda_table_name}', cda_table_name)
        sqlexp = f"""
            EXPORT DATA OPTIONS (
                uri='{output_path}',
                format='PARQUET',
                overwrite=true ) AS
            SELECT * FROM
                ({sql});
        """
        logging.info(f"sqlexp: {sqlexp}")
        run_bq_query(sqlexp)
        logging.info(f"Created cda variable limits files in {output_path}")

    def load_limits_proc_bq_ext(self, file_path: str, ext_bq_table: str):
        logging.info(f'Loading the variable limits file: {file_path} as an external table: {ext_bq_table}')
        sqlexp = f"""
            CREATE OR REPLACE EXTERNAL TABLE
                `{ext_bq_table}`
             OPTIONS (
                format = 'PARQUET',
                uris = ['{file_path}']
            );
        """
        logging.info(f"sqlexp: {sqlexp}")
        run_bq_query(sqlexp)
        count_records_tbl(ext_bq_table)

    def check_threshold_fn(self, file_path: str, ext_bq_table: str, cnt_threshold: int):
        self.load_limits_proc_bq_ext(file_path, ext_bq_table)
        sqlexp = f"""
            SELECT COUNT(DISTINCT customerId) as cnt_cust_id
            FROM `{ext_bq_table}`
        """
        bq_job = run_bq_query(sqlexp)
        rows = bq_job.result()
        cnt_cust_id = next(rows).cnt_cust_id
        logging.info(f"cnt_cust_id = {cnt_cust_id}, threshold = {cnt_threshold}")

        if cnt_cust_id >= cnt_threshold:
            logging.info("Over threshold")
            return 'load_limits_to_landing_tbl'
        else:
            logging.info("Under threshold")
            return 'end'

    def load_limits_to_landing_tbl_fn(self, from_table: str, to_table: str):
        sqlexp = f"""
            CREATE OR REPLACE TABLE
                `{to_table}` AS
            SELECT
                customerId,
                periodType,
                maxLimit,
                requestDate
            FROM `{from_table}`
        """
        run_bq_query(sqlexp)

    def create_trigger_task(self, config: Dict):
        file_path = config["calc_limits"]["output_path"]
        file_path_list = file_path.split('/')
        bucket_name = file_path_list[2]
        folder_name = file_path_list[3]
        file_name = '/'.join(file_path_list[4:])
        cluster_name = config["trigger_dag"]["cluster_name"]

        return TriggerDagRunOperator(
            task_id="trigger_kafka_events",
            trigger_dag_id=config["trigger_dag"]["dag_id"],
            conf={
                consts.BUCKET: bucket_name,
                consts.FOLDER_NAME: folder_name,
                consts.FILE_NAME: file_name,
                consts.CLUSTER_NAME: cluster_name,
            }
        )

    def create_def_args(self, dag_config: dict):
        default_args = copy.deepcopy(self.default_args)
        default_args.update({"business_impact": f'{dag_config.get("business_impact")}',
                             "customer_impact": f'{dag_config.get("customer_impact")}',
                             "sub_capability": f'{dag_config.get("sub_capability")}',
                             "capability": f'{dag_config.get("capability")}'})
        return default_args

    def create_external_bq_table(self, ext_bq_table: str, filepath: str, **context):
        logging.info(f'Running Sql to load the stored parquet file as an external table {ext_bq_table}')
        bq_client = bigquery.Client()
        logging.info(f"tablename: {ext_bq_table}")
        create_external_table(bq_client, ext_bq_table, filepath, expiration_hours=1)

    def create_dag(self, dag_id: str, dag_config: dict, default_args: dict) -> DAG:
        dag = DAG(
            description=self.job_config.get(dag_id).get("description"),
            tags=self.job_config.get(dag_id).get("tags"),
            max_active_runs=5,
            catchup=False,
            is_paused_upon_creation=True,
            dag_id=dag_id,
            default_args=default_args,
            doc_md=doc_md,
            schedule=None,
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
        )

        with dag:
            start = EmptyOperator(task_id=consts.START_TASK_ID)

            calculate_limits_task = PythonOperator(
                task_id="calculate_limits_task",
                python_callable=self.calculate_limits,
                op_kwargs={
                    "cda_filepath": self.job_config[dag_id]["cda_external_table"]["datasource"],
                    "sql_file_path": self.job_config[dag_id]["calc_limits"]["sql_fname"],
                    "ext_bq_table": self.job_config[dag_id]["cda_external_table"]["table_name"],
                    "cda_table_name": self.job_config[dag_id]["calc_limits"]["cda_table_name"],
                    "output_path": self.job_config[dag_id]["calc_limits"]["output_path"]
                }
            )

            check_cust_count = BranchPythonOperator(
                task_id="check_cust_count",
                python_callable=self.check_threshold_fn,
                op_kwargs={
                    "file_path": self.job_config[dag_id]["calc_limits"]["output_path"],
                    "ext_bq_table": self.job_config[dag_id]["calc_limits"]["output_table"],
                    "cnt_threshold": self.job_config[dag_id]["calc_limits"]["cust_cnt_threshold"]
                }
            )

            load_limits_to_landing_tbl = PythonOperator(
                task_id="load_limits_to_landing_tbl",
                python_callable=self.load_limits_to_landing_tbl_fn,
                op_kwargs={
                    "from_table": self.job_config[dag_id]["calc_limits"]["output_table"],
                    "to_table": self.job_config[dag_id]["store_limits"]["table_name"]
                }
            )

            # Commenting for backup and removed the trigger_kafka_events_task before end stage
            # as it is not required after implementing manual limits

            # trigger_kafka_events_task = self.create_trigger_task(self.job_config[dag_id])

            end = EmptyOperator(task_id=consts.END_TASK_ID, trigger_rule="none_failed")

            start \
                >> calculate_limits_task \
                >> check_cust_count \
                >> load_limits_to_landing_tbl \
                >> end

            check_cust_count >> end

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        for key, values in self.job_config.items():
            dags[key] = self.create_dag(key, values, self.create_def_args(values))

        return dags


globals().update(TriadVariableLimitsLoader().create_dags())
