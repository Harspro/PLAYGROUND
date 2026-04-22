import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pendulum
import util.constants as consts

from abc import ABC
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow import settings, DAG
from datetime import timedelta, datetime
from google.cloud import bigquery
from typing import Final

from money_movement.utils.batch_run_log_util import create_execution_entry, update_execution_entry
from util.bq_utils import create_external_table, run_bq_query
from util.gcs_utils import delete_folder
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env, read_file_env
)
from dag_factory.terminus_dag_factory import add_tags

BILL_REFRESH_LOADER: Final = 'bill_refresh_loader'

logger = logging.getLogger(__name__)


class BillRefreshLoader(ABC):
    def __init__(self):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dag_config_fname = f"{settings.DAGS_FOLDER}/{consts.CONFIG}/bill_refresh_processing_config.yaml"
        self.kafka_dag_config_fname = f"{settings.DAGS_FOLDER}/{consts.CONFIG}/file_kafka_connector_writer_configs/bill_refresh_bq_kafka_config.yaml"
        self.kafka_job_config = read_yamlfile_env(f"{self.kafka_dag_config_fname}", self.deploy_env)
        self.job_config = read_yamlfile_env(f"{self.dag_config_fname}", self.deploy_env)
        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)
        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)
        self.default_args = {
            "owner": "team-money-movement-eng",
            "depends_on_past": False,
            "wait_for_downstream": False,
            "retries": 0,
            "retry_delay": timedelta(seconds=10),
            'capability': 'Payments',
            'severity': 'P3',
            'sub_capability': 'EMT',
            'business_impact': 'none',
            'customer_impact': 'None'
        }

    def split_csv_header_detail_trailer_files(self, dag_id, **context):
        bucket_name = context['dag_run'].conf.get('bucket')
        file_name = context['dag_run'].conf.get('name')
        logger.info(f'bucket{bucket_name} file {file_name}')
        csv_file_path = f"gs://{bucket_name}/{file_name}"
        df = pd.read_csv(csv_file_path, names=self.job_config.get(dag_id).get('cols_detail'), sep='\t')
        trailer_row = df.iloc[-1]
        detail_df = df.iloc[:-1]
        date_from_trailer = trailer_row[1]
        detail_df['FILE_CREATE_DT'], detail_df['FILE_NAME'] = [date_from_trailer, file_name.split("/", 1)[-1]]
        all_columns = list(detail_df)
        detail_df[all_columns] = detail_df[all_columns].astype(str).apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        detail_table = pa.Table.from_pandas(detail_df)
        trailer_table = pa.Table.from_pandas(df.tail(1)).rename_columns(self.job_config.get(dag_id).get('cols_trailer'))
        detail_parquet_file_path = f'gs://{self.job_config.get(dag_id).get(consts.STAGING_FOLDER)}/{file_name}_detail.parquet'
        trailer_parquet_file_path = f'gs://{self.job_config.get(dag_id).get(consts.STAGING_FOLDER)}/{file_name}_trailer.parquet'
        pq.write_table(detail_table, detail_parquet_file_path)
        pq.write_table(trailer_table, trailer_parquet_file_path)

    def create_entry(self, **kwargs):
        job_type = "ETL"
        job_name = "daily_data_load"
        source_name = "source_system"
        target_name = "target_system"
        create_user_id = "airflow_user"
        create_function_name = "data_load_dag"
        batch_run_log_uid = create_execution_entry(
            job_type=job_type,
            job_name=job_name,
            source_name=source_name,
            target_name=target_name,
            create_user_id=create_user_id,
            create_function_name=create_function_name
        )
        # Push the UID to XCom for use in the update task
        kwargs['ti'].xcom_push(key='batch_run_log_uid', value=batch_run_log_uid)

    def update_entry(self, cnt_sql_file_path, parquet_file_name, folder_file_name, **kwargs):
        ti = kwargs['ti']
        batch_run_log_uid = ti.xcom_pull(key='batch_run_log_uid', task_ids='create_entry_task')
        read_count = self.get_current_record_read_count(cnt_sql_file_path, folder_file_name)
        write_count = self.get_file_record_write_count(parquet_file_name)
        status = "OK" if read_count == write_count else "RECON_ERROR" if write_count < read_count else None
        update_execution_entry(
            batch_run_log_uid=batch_run_log_uid,
            run_end=datetime.now(),
            read_cnt=read_count,
            write_cnt=write_count,
            nondata_cnt=1,
            status=status,
            update_user_id="airflow_user",
            update_function_name="data_load_dag"
        )

    def update_failure_entry(self, cnt_sql_file_path, folder_file_name, **kwargs):
        ti = kwargs['ti']
        batch_run_log_uid = ti.xcom_pull(key='batch_run_log_uid', task_ids='create_entry_task')
        read_count = self.get_current_record_read_count(cnt_sql_file_path, folder_file_name)
        update_execution_entry(
            batch_run_log_uid=batch_run_log_uid,
            run_end=datetime.now(),
            read_cnt=read_count,
            write_cnt=0,
            status="FAILED",
            update_user_id="airflow_user",
            update_function_name="data_load_dag"
        )

    def validate_bill_refresh_data(self, query_file):
        logger.info("Running Sql file to validate bill refresh file records")
        sql = read_file_env(f"{settings.DAGS_FOLDER}/" + query_file, self.deploy_env)
        bq_client = bigquery.Client()
        bq_query_job = bq_client.query(sql)
        result = bq_query_job.result().to_dataframe().to_string(index=False)
        for row in bq_query_job:
            count_rows = bq_query_job.result().total_rows
            if count_rows > 0:
                raise AirflowException(f"{result}")
            else:
                logger.info("Data Validation is success")

    def load_gcs_to_bq_parquet_append(self, table_name, project_id, dataset_name, gcs_table_staging_dir):
        """
        Loads the parquet files in gcs to big query table (single table)
        """
        logging.info(f"Starting import of data for table {table_name} from {gcs_table_staging_dir}")
        bq_client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.PARQUET
        )
        bq_table_id = f"{project_id}.{dataset_name}.{table_name}"
        bq_load_job = bq_client.load_table_from_uri(
            gcs_table_staging_dir,
            bq_table_id,
            job_config=job_config
        )
        bq_load_job.result()

        target_table = bq_client.get_table(bq_table_id)
        logging.info(
            f"Finished import of data for table {table_name}: "
            f"Imported from GCS Bucket - {gcs_table_staging_dir} to BQ Table name {bq_table_id}. Imported {target_table.num_rows} rows.")

    def get_input_or_current_file_name(self, **context):
        file_name = context[consts.DAG_RUN].conf.get("filename")
        if file_name is not None:
            return f'"{file_name}"'
        else:
            return context[consts.DAG_RUN].conf.get('name').split("/", 1)[-1]

    def get_file_record_write_count(self, file_name):
        parquet_df = pd.read_parquet(file_name)
        parquet_table = pa.Table.from_pandas(parquet_df, preserve_index=False)
        return parquet_table.num_rows

    def get_current_record_read_count(self, cnt_sql_file_path, folder_file_name):
        logger.info("Running Sql to get current file's number of rows from BILLER table")
        sql = read_file_env(f"{settings.DAGS_FOLDER}/" + cnt_sql_file_path, self.deploy_env)
        bq_client = bigquery.Client()
        logger.info(f'filename:{folder_file_name}')
        file_name = folder_file_name.split("/", 1)[-1]
        sql = sql.replace("<<FILE_NAME>>", f"{file_name}")
        result = bq_client.query(sql).result().to_dataframe()
        return int(result['COUNT'])

    def delete_biller_staging_folder(self, dag_id, obj_prefix):
        return delete_folder(self.job_config.get(dag_id).get(consts.PROCESSING_BUCKET_EXTRACT), obj_prefix)

    def get_current_record_write_count(self, parquet_file_name):
        parquet_df = pd.read_parquet(parquet_file_name)
        parquet_table = pa.Table.from_pandas(parquet_df, preserve_index=False)
        return parquet_table.num_rows

    def run_sql_to_create_parquet(self, sql_file_path, **context):
        logger.info("Running Sql file to create parquet file from the current bil refresh file")
        sql = read_file_env(f"{settings.DAGS_FOLDER}/" + sql_file_path, self.deploy_env)
        file_name = self.get_input_or_current_file_name(**context)
        logger.info(f'filename:{file_name}')
        sql = sql.replace("<<FILE_NAME>>", f"{file_name}")
        run_bq_query(sql)
        logger.info("Parquet files created for the current bill refresh file")

    def load_bill_refresh_data_landing_bq(self, dag_id, **context):
        logger.info("loading data into the landing biller table")
        target_table_name = self.job_config.get(dag_id).get(consts.TARGET_TABLE_ID)
        project_id = self.gcp_config.get(consts.LANDING_ZONE_PROJECT_ID)
        dataset_name = self.job_config.get(dag_id).get(consts.DATASET_ID)
        file_name = context['dag_run'].conf.get('name')
        detail_parquet_file_path = f'gs://{self.job_config.get(dag_id).get(consts.STAGING_FOLDER)}/{file_name}_detail.parquet'
        self.load_gcs_to_bq_parquet_append(target_table_name, project_id, dataset_name, detail_parquet_file_path)

    def load_gcs_detail_trailer_parquet_bq(self, dag_id, **context):
        bq_client = bigquery.Client()
        staging_table_name_details = self.job_config.get(dag_id).get(consts.STAGING_TABLE_ID)
        staging_table_name_trailer = self.job_config.get(dag_id).get(consts.EXTERNAL_TABLE_ID)
        file_name = context['dag_run'].conf.get('name')
        detail_parquet_file_path = f'gs://{self.job_config.get(dag_id).get(consts.STAGING_FOLDER)}/{file_name}_detail.parquet'
        trailer_parquet_file_path = f'gs://{self.job_config.get(dag_id).get(consts.STAGING_FOLDER)}/{file_name}_trailer.parquet'
        create_external_table(bq_client, staging_table_name_details, detail_parquet_file_path)
        create_external_table(bq_client, staging_table_name_trailer, trailer_parquet_file_path)

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        dag = DAG(
            description="DAG to load biller data to the table",
            tags=self.job_config.get(dag_id).get(consts.TAGS),
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True,
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=None,
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
        )

        with dag:
            start = EmptyOperator(task_id=consts.START_TASK_ID)
            folder_file_name = "{{ dag_run.conf['name'] }}"
            create_entry_task = PythonOperator(
                task_id='create_entry_task',
                python_callable=self.create_entry
            )
            split_bill_refresh_file = PythonOperator(
                task_id="split_billrefresh_file",
                python_callable=self.split_csv_header_detail_trailer_files,
                op_kwargs={"dag_id": dag_id},

            )
            load_gcs_detail_to_bq = PythonOperator(
                task_id="load_gcs_detail_trailer_to_bq",
                python_callable=self.load_gcs_detail_trailer_parquet_bq,
                op_kwargs={"dag_id": dag_id}
            )
            validate_bill_refresh_data = PythonOperator(
                task_id="validate_bill_refresh_data",
                python_callable=self.validate_bill_refresh_data,
                op_kwargs={consts.QUERY_FILE: self.job_config.get(dag_id).get(consts.QUERY_FILE)}
            )
            delete_biller_staging_folder = PythonOperator(
                task_id="delete_biller_staging_folder",
                python_callable=self.delete_biller_staging_folder,
                op_kwargs={"dag_id": dag_id, consts.OBJ_PREFIX: self.job_config.get(dag_id).get(consts.FOLDER_NAME)}
            )
            load_bill_refresh_data_landing_bq = PythonOperator(
                task_id="load_bill_refresh_data_landing_bq",
                python_callable=self.load_bill_refresh_data_landing_bq,
                op_kwargs={"dag_id": dag_id},

            )
            run_sql_to_create_parquet = PythonOperator(
                task_id="run_sql_to_create_parquet",
                python_callable=self.run_sql_to_create_parquet,
                op_kwargs={"sql_file_path": self.job_config.get(dag_id).get("sql_file_path")}
            )
            kafka_writer_task = TriggerDagRunOperator(
                task_id=self.job_config.get(dag_id).get(consts.KAFKA_WRITER_TASK),
                trigger_dag_id=self.job_config.get(dag_id).get(consts.KAFKA_TRIGGER_DAG_ID),
                logical_date=datetime.now(self.local_tz),
                conf={
                    consts.BUCKET: self.job_config.get(dag_id).get(consts.STAGING_BUCKET),
                    consts.FOLDER_NAME: self.job_config.get(dag_id).get(consts.FOLDER_NAME),
                    consts.FILE_NAME: self.job_config.get(dag_id).get(consts.FILE_NAME),
                    consts.CLUSTER_NAME: self.job_config.get(dag_id).get(consts.KAFKA_CLUSTER_NAME)
                },
                wait_for_completion=True,
                poke_interval=30
            )
            update_entry_task = PythonOperator(
                task_id='update_entry_task',
                python_callable=self.update_entry,
                op_kwargs={"cnt_sql_file_path": self.job_config.get(dag_id).get("cnt_sql_file_path"),
                           "parquet_file_name": self.job_config.get(dag_id).get("parquet_file_name"),
                           "folder_file_name": folder_file_name},
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
            )
            update_failure_entry_task = PythonOperator(
                task_id='update_failure_entry_task',
                python_callable=self.update_failure_entry,
                op_kwargs={"cnt_sql_file_path": self.job_config.get(dag_id).get("cnt_sql_file_path"), "folder_file_name": folder_file_name},
                trigger_rule=TriggerRule.ONE_FAILED
            )
            end = EmptyOperator(task_id=consts.END_TASK_ID)

            (start >> create_entry_task >> split_bill_refresh_file >> delete_biller_staging_folder
             >> load_gcs_detail_to_bq >> validate_bill_refresh_data
             >> load_bill_refresh_data_landing_bq >> run_sql_to_create_parquet >> kafka_writer_task >> update_entry_task
             >> update_failure_entry_task >> end)
        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)
        return dags


globals().update(BillRefreshLoader().create_dags())
