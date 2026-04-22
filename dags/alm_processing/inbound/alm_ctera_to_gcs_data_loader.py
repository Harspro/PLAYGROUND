import os
from abc import ABC
from copy import deepcopy
from datetime import timedelta, datetime
from util.constants import (BIGQUERY, READ_PAUSE_DEPLOY_CONFIG, SCHEDULE_INTERVAL, DAG as DAG_STR)
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import io
import pendulum
import re
import logging
import util.miscutils as misc
from alm_processing.utils import constants as alm_constant
from util.smb_utils import SMBUtil
import util.bq_utils as bq_utils
from alm_processing.utils import bq_util as bqutil
from util.deploy_utils import (
    read_pause_unpause_setting,
    pause_unpause_dag
)
from util.miscutils import (
    read_yamlfile_env_suffix
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)
os.environ['AIRFLOW__OPERATORS__ALLOW_ILLEGAL_ARGUMENTS'] = 'true'


class ALMCteraToGcsCopy(ABC):
    def __init__(self, config_filename: str, ctera_config_name: str, ctera_share_name: str):
        config_dir = f'{settings.DAGS_FOLDER}/alm_processing/dags_config'
        ctera_config_path = f'{settings.DAGS_FOLDER}/config/{ctera_config_name}'
        self.job_config = read_yamlfile_env_suffix(
            f'{config_dir}/{config_filename}',
            alm_constant.DEPLOY_ENV, alm_constant.DEPLOY_ENV_SUFFIX)
        self.default_args = deepcopy(alm_constant.DAG_DEFAULT_ARGS)
        self.local_tz = pendulum.timezone('America/Toronto')
        if misc.get_smb_server_config(ctera_config_path, ctera_share_name, alm_constant.DEPLOY_ENV):
            self._server_ip, self._usename, self._password = misc.get_smb_server_config(ctera_config_path, ctera_share_name, alm_constant.DEPLOY_ENV)
            self._smbutil = SMBUtil(self._server_ip, self._usename, self._password)

        if alm_constant.DEPLOY_ENV == 'prod':
            self._runtime_env = 'prod'
        else:
            self._runtime_env = 'nonprod'

    def get_schedule(self, dag_config: dict):
        if DAG_STR in dag_config:
            return dag_config[DAG_STR].get(SCHEDULE_INTERVAL, None)
        else:
            return None

    def generate_dags(self) -> dict:
        dags = {}
        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)

        return dags

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        with DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            schedule=self.get_schedule(dag_config),
            start_date=datetime(2024, 1, 1, tzinfo=self.local_tz),
            dagrun_timeout=timedelta(hours=5),
            max_active_tasks=20,
            max_active_runs=1,
            catchup=False,
            is_paused_upon_creation=True,
            tags=alm_constant.TAGS
        )as dag:
            if dag_config.get(READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(
                    dag_id, alm_constant.DEPLOY_ENV)
                pause_unpause_dag(dag, is_paused)

            pipeline_start = EmptyOperator(task_id='start')

            @task
            def update_previous_run_ind(config):
                self._update_previous_current_run_ind(config)

            group_id = 'ctera_to_gcs_file_copy'

            @task_group(group_id=group_id)
            def file_processing(file_types):
                try:
                    for file_type in file_types:
                        ctera_shared_folder = dag_config[file_type][self._runtime_env][alm_constant.CTERA_SHARED_FOLDER]

                        task_id = f"{file_type}_get_file_list"

                        @task(task_id=task_id)
                        def ctera_file_list(ctera_shared_folder):
                            file_list = self._smbutil.list_dir(ctera_shared_folder)
                            return file_list

                        task_id = f"{file_type}_data_copy"

                        @task(task_id=task_id, max_active_tis_per_dagrun=alm_constant.MAX_ACTIVE_TIS_PER_DAGRUN)
                        def ctera_to_gcs_file_copy(file_name: str, file_type: str, ctera_shared_folder: str, config: dict):
                            logger.info(f"{file_name} file copy is stated !!!!!")
                            self._ctera_file_gcs_copy_final_func(config, file_name, file_type, ctera_shared_folder)

                        ctera_to_gcs_file_copy\
                            .partial(file_type=file_type, config=dag_config, ctera_shared_folder=ctera_shared_folder)\
                            .expand(file_name=ctera_file_list(ctera_shared_folder))

                except Exception as err:
                    logger.error(f"please check the error:{err}")

            group_id = 'check_final_ctera_to_gcs_status'

            @task_group(group_id=group_id)
            def check_final_status(file_types):
                for file_type in file_types:
                    task_id = f"{file_type}_copy_check_status"

                    @task(task_id=task_id, trigger_rule='none_failed')
                    def check_overall_status(file_type, config: dict):
                        self._check_final_status(config, file_type)

                    check_overall_status.partial(config=dag_config).expand(file_type=[file_type])

            file_types = alm_constant.ALM_CTERA_FOLDERS
            update_previous_run_ind_task = update_previous_run_ind(dag_config)
            processing_task = file_processing(file_types)
            check_final_status_task = check_final_status(file_types)
            final_task = self.run_final_task(dag_config, dag)

            pipeline_start >> update_previous_run_ind_task >> processing_task \
                >> check_final_status_task >> final_task

        return add_tags(dag)

    def _ctera_to_gcs_file_copy(self, ctera_shared_folder, file_name, gcs_bucket, gcs_folder, process_type):
        file_copy_status = self._smbutil.copy_ctera_to_gcs(ctera_shared_folder, file_name, gcs_bucket, gcs_folder, file_name)

        logger.info(f"file_copy_status:{file_copy_status}")
        logger.info("==================================================================")
        if file_copy_status == alm_constant.CTERA_SUCCESS:
            file_copy_status = f"{file_copy_status}:{file_name} is {process_type}ed and copied in gcs folder {gcs_folder}"
            if process_type == alm_constant.CTERA_ARCHIVE or process_type == alm_constant.CTERA_BBG_REQ:
                file_copy_status = self._delete_file_from_ctera(file_copy_status, file_name, ctera_shared_folder, gcs_folder)

        return file_copy_status

    def _ctera_to_gcs_file_copy_base_func(self, file_type, file_pattern, process_type, ctera_shared_folder, file_name, config: dict, all_log_records):
        gcs_bucket_name = f"gcs_{process_type}_bucket"
        gcs_folder_name = f"gcs_{process_type}_folder"
        gcs_bucket = config[gcs_bucket_name]
        logger.info(f"file_name:{file_name}")
        logger.info(f"file_pattern:{file_pattern}")
        logger.info(f"process_type:{process_type}")
        logger.info(f"gcs_bucket:{gcs_bucket}")
        logger.info("==================================================================")
        load_dt = datetime.now().astimezone(tz=self.local_tz).strftime('%Y-%m-%d')
        process_ts = datetime.now().astimezone(tz=self.local_tz).strftime('%Y-%m-%d %H:%M:%S')
        current_run_ind = alm_constant.CURRENT_RUN_IND

        log_record = f"{file_name}|{file_pattern}|{file_type}|{process_type}|{gcs_bucket}|"

        try:
            if file_type == alm_constant.CTERA_PROCESS_FILE:
                gcs_folder, file_copy_status = self._ctera_file_pre_process_validation(config, file_pattern, file_name, ctera_shared_folder, process_type, gcs_folder_name)

                if 'FAILED' not in file_copy_status and gcs_folder:
                    file_copy_status = self._ctera_to_gcs_file_copy(ctera_shared_folder, file_name, gcs_bucket, gcs_folder, process_type)

            elif file_type == alm_constant.CTERA_BBG_REQ_FILE:
                gcs_folder = config[gcs_folder_name]
                file_copy_status = self._ctera_to_gcs_file_copy(ctera_shared_folder, file_name, gcs_bucket, gcs_folder, process_type)

            else:
                flag = 0
                for vendor_name in config[gcs_folder_name].keys():
                    if vendor_name in file_name:
                        gcs_folder = config[gcs_folder_name][vendor_name]
                        file_copy_status = self._ctera_to_gcs_file_copy(ctera_shared_folder, file_name, gcs_bucket, gcs_folder, process_type)
                        flag = 1
                    else:
                        if not flag:
                            file_copy_status = "FAILED: Valid vendor code is not present in raw file name!!!!"

            if 'FAILED' in file_copy_status and file_type == alm_constant.CTERA_PROCESS_FILE:
                status, ctera_shared_failed_file = self._move_file_in_failed_folder(ctera_shared_folder, file_name)
                if status == alm_constant.CTERA_SUCCESS:
                    file_copy_status = f"{file_copy_status} and file moved in //{self._server_ip}/{ctera_shared_failed_file} path"

            if alm_constant.CTERA_GCS_FOLDER in locals():
                log_record = log_record + f"{gcs_folder}|{current_run_ind}|{load_dt}|{process_ts}|{file_copy_status}"
            else:
                log_record = log_record + f"''|{current_run_ind}|{load_dt}|{process_ts}|{file_copy_status}"
            all_log_records.append(log_record)
            logger.info("==================================================================")
            return file_copy_status

        except Exception as err:
            if 'file_copy_status' not in locals():
                file_copy_status = f"FAILED:Please check the error:{err}"
                logger.error(f"{file_copy_status}")
            else:
                logger.error(f"{file_copy_status}")

            if alm_constant.CTERA_GCS_FOLDER in locals():
                log_record = log_record + f"{gcs_folder}|{current_run_ind}|{load_dt}|{process_ts}|{file_copy_status}"
            else:
                log_record = log_record + f"''|{current_run_ind}|{load_dt}|{process_ts}|{file_copy_status}"
            all_log_records.append(log_record)
            logger.info("==================================================================")
            return file_copy_status

    def _ctera_file_gcs_copy_final_func(self, config: dict, file_name, file_type, ctera_shared_folder):
        logger.info(f"ctera_shared_folder: {ctera_shared_folder}")

        individual_file_log_record = []
        individual_file_log_record.append(alm_constant.CTERA_LOG_HEADER)

        if file_type == alm_constant.CTERA_PROCESS_FILE:
            process_type = alm_constant.CTERA_PROCESS
            file_pattern = config[file_type][self._runtime_env][alm_constant.CTERA_FILE_PATTERN]
            file_process_copy_status = self._ctera_to_gcs_file_copy_base_func(file_type, file_pattern, process_type, ctera_shared_folder, file_name, config, individual_file_log_record)

            if alm_constant.CTERA_SUCCESS in file_process_copy_status:
                process_type = alm_constant.CTERA_ARCHIVE
                self._ctera_to_gcs_file_copy_base_func(file_type, file_pattern, process_type, ctera_shared_folder, file_name, config, individual_file_log_record)

        elif file_type == alm_constant.CTERA_BBG_REQ_FILE:
            process_type = alm_constant.CTERA_BBG_REQ
            file_pattern = config[file_type][alm_constant.CTERA_FILE_PATTERN]
            self._ctera_to_gcs_file_copy_base_func(file_type, file_pattern, process_type, ctera_shared_folder, file_name, config, individual_file_log_record)
        else:
            process_type = alm_constant.CTERA_ARCHIVE
            file_pattern = ''
            self._ctera_to_gcs_file_copy_base_func(file_type, file_pattern, process_type, ctera_shared_folder, file_name, config, individual_file_log_record)

        self._check_individual_file_status(config, individual_file_log_record)
        logger.info("==================================================================")

    def _ctera_file_pre_process_validation(self, config, file_pattern, file_name, ctera_shared_folder, process_type, gcs_folder_name):
        try:
            gcs_folder = ''
            if re.findall(file_pattern, file_name):
                vendor_name = file_name.split('_')[0]
                gcs_folder, file_copy_status = self._gcs_folder_calcutaiton(config, process_type, vendor_name, gcs_folder_name, file_name)

                logger.info(f"{gcs_folder_name}:{gcs_folder}")
                logger.info("==================================================================")
            else:
                file_copy_status = f"FAILED:{file_name} is not matching with {file_pattern}"
                logger.error(f"{file_copy_status}")

            return gcs_folder, file_copy_status

        except Exception as err:
            logger.info(f"Please check the error:{err}")

    def _gcs_folder_calcutaiton(self, config, process_type, vendor_name, gcs_folder_name, file_name):
        try:
            file_copy_status = ''
            gcs_folder = ''
            if process_type == alm_constant.CTERA_ARCHIVE:
                if vendor_name in config[gcs_folder_name].keys():
                    gcs_folder = config[gcs_folder_name][vendor_name]
                else:
                    file_copy_status = f"FAILED:vendor_name:'{vendor_name}' is not supported !!!"
            else:
                type_name = file_name.split('_')[3]
                if type_name in config[gcs_folder_name].keys():
                    gcs_folder = config[gcs_folder_name][type_name]
                else:
                    file_copy_status = f"FAILED:type_name:'{type_name}' is not supported !!!"

            return gcs_folder, file_copy_status

        except Exception as err:
            logger.info(f"Please check the error:{err}")

    def _delete_file_from_ctera(self, file_copy_status, file_name, ctera_shared_folder, gcs_folder):
        ctera_file_path = f"{ctera_shared_folder}/{file_name}"
        self._smbutil.delete_file(ctera_file_path)
        file_copy_status = f"SUCCESS:{file_name} archived in gcs_folder:{gcs_folder} and removed from //{self._server_ip}/{ctera_file_path} path"
        logger.info(f"file_copy_status:{file_copy_status}")
        logger.info("==================================================================")
        return file_copy_status

    def _check_individual_file_status(self, config, all_log_records):
        if all_log_records:
            self._load_audit_table(config, all_log_records)
            failure_check = any(alm_constant.CTERA_FAILED in logs for logs in all_log_records)

            if failure_check:
                logger.info("FAILED_FILES_STATUS_REPORT")
                logger.info("==================================================================")
                logger.info(all_log_records[0])
                for logs in all_log_records:
                    if alm_constant.CTERA_FAILED in logs:
                        logger.info(f"{logs}")
            else:
                logger.info("Files are successfully copied in GCS!!!!!")
                logger.info("==================================================================")

    def _load_audit_table(self, config, data):
        try:
            client = bigquery.Client()
            partition_column = config[BIGQUERY][alm_constant.BQ_PARTITION_COLUMN]
            table_id = self._destination_table_id(config)
            logger.info(f"Audit Table name is :{table_id}")

            schema = []
            for i in data[0].split('|'):
                field_name = i.lower()
                if re.findall("dt|date", field_name):
                    data_type = 'DATE'
                elif re.findall("ts|time_stamp", field_name):
                    data_type = 'TIMESTAMP'
                else:
                    data_type = 'STRING'

                schema.append(bigquery.SchemaField(f"{field_name}", f"{data_type}"))

            data_string = "\n".join(data)
            file_obj = io.StringIO(data_string)

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                field_delimiter="|",
                schema=schema,
                skip_leading_rows=1,
                autodetect=False,
                schema_update_options=bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                time_partitioning=bigquery.TimePartitioning(field=f"{partition_column}"),
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )

            load_job = client.load_table_from_file(
                file_obj,
                table_id,
                job_config=job_config
            )

            load_job.result()
            logger.info(f"Loaded {load_job.output_rows} rows into {table_id}")

        except Exception as err:
            logger.error(f"please check the error:{err}")

    def _check_final_status(self, config, file_type):
        table_id = self._destination_table_id(config)
        nl = "\n"
        validation_query = f"""SELECT * FROM {table_id}
        WHERE file_type='{file_type}'
        AND current_run_ind='Y' AND
        status LIKE 'FAILED%'"""

        logger.info(f"query:{nl}{validation_query}")
        bq_query_job = bq_utils.run_bq_query(validation_query)
        result = bq_query_job.result().to_dataframe().to_string(index=False)
        for row in bq_query_job:
            count_rows = bq_query_job.result().total_rows
            if count_rows > 0:
                logger.error(f"Some of the {file_type} copy are failed.Please check the consoldiated log!!!!")
                logger.info("==============================================================================")
                logger.info(f"{nl}{result}")
                logger.info("===============================================================================")

                raise AirflowFailException("pls check the above log for failed file")
            else:
                logger.info("=========================================================")
                logger.info(f"All {file_type} copy successfull to GCS")
                logger.info("=========================================================")

    def _update_previous_current_run_ind(self, config):
        try:
            client = bigquery.Client()
            nl = "\n"
            table_id = self._destination_table_id(config)
            table = client.get_table(table_id)
            if table:
                logger.info(f"table_id:{table_id}")
                query = f"""UPDATE {table_id} SET current_run_ind='N'
                WHERE current_run_ind='Y'"""

                logger.info(f"query:{nl}{query}")
                result = bqutil.run_bq_query(query)
                logger.info(f"Table update status:{result}")

        except NotFound:
            logger.info(f"{table_id} table is not created yet!!!!!")

        except Exception as err:
            logger.error(f"please check the error:{err}")

    def _destination_table_id(self, config):
        project_id = config[BIGQUERY][alm_constant.BQ_DESTINATION_PROJECT]
        dataset_id = config[BIGQUERY][alm_constant.BQ_DESTINATION_DATASET]
        table_name = config[BIGQUERY][alm_constant.BQ_DESTINATION_TABLE_NAME]
        return f"{project_id}.{dataset_id}.{table_name}"

    def run_final_task(self, config: dict, dag: DAG):
        return EmptyOperator(
            task_id="end",
            trigger_rule=TriggerRule.NONE_FAILED)

    def _move_file_in_failed_folder(self, ctera_shared_folder, filename):
        try:
            ctera_dir = ctera_shared_folder.rsplit('/', 1)[0]
            ctra_fail_folder_path = f"{ctera_dir}/failed"
            if not self._smbutil.is_dir(ctra_fail_folder_path):
                self._smbutil.make_dir(ctra_fail_folder_path)
            ctera_shared_path_file = f"{ctera_shared_folder}/{filename}"
            ctera_shared_failed_file = f"{ctra_fail_folder_path}/{filename}"
            self._smbutil.rename_file(ctera_shared_path_file, ctera_shared_failed_file)
            status = alm_constant.CTERA_SUCCESS

        except Exception as err:
            status = alm_constant.CTERA_FAILED
            logger.error(f"please check the error:{err}")

        return status, ctera_shared_failed_file
