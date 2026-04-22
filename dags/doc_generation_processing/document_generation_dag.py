import json
import logging
import os
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta, datetime
from functools import partial
from itertools import islice
from typing import Final, Optional

import google.auth
import google.oauth2.id_token
import pendulum
import requests
from airflow import DAG
from airflow.decorators import task_group
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from dag_factory.terminus_dag_factory import add_tags
from google.cloud import storage
from kubernetes.client import \
    V1Volume, V1VolumeMount, V1SecretVolumeSource, V1Toleration, V1LocalObjectReference
from util.bq_utils import run_bq_query
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from util.miscutils import read_yamlfile_env, read_variable_or_file, read_file_env
from doc_generation_processing.custom_gke_start_pod_operator import CustomGKEStartPodOperator

import doc_generation_processing.util.constants as doc_gen_consts
import util.constants as consts

logger = logging.getLogger(__name__)

INITIAL_DEFAULT_ARGS: Final = {
    "owner": "team-telegraphers-alerts",
    'capability': 'TBD',
    'severity': 'P3',
    'sub_capability': 'TBD',
    'business_impact': 'TBD',
    'customer_impact': 'TBD',
    "depends_on_past": False,
    "wait_for_downstream": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=10)
}


class DocumentGeneratorDagBuilder:
    def __init__(self, config_filename: str):
        self.est_zone = doc_gen_consts.EST_ZONE
        self.local_tz = pendulum.timezone(self.est_zone)

        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.dag_config = read_yamlfile_env(config_filename, self.deploy_env)

        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)

    def create_unique_folder_path(self, **kwargs):
        dag_run_id = kwargs['dag_run'].run_id
        unique_folder_path = re.sub(r'[^a-zA-Z0-9]', '_', dag_run_id)
        return unique_folder_path

    def fetch_and_export_data(self, unique_folder_path: str, start_time: Optional[str] = None,
                              end_time: Optional[str] = None):
        """Fetch data from BQ table, and save it directly into GCS bucket without loading into composer memory."""
        if not start_time or not end_time:
            today = datetime.now(tz=self.local_tz)
            yesterday = today - timedelta(days=1)
            start_time = start_time or yesterday.replace(hour=8, minute=0, second=0, microsecond=0).strftime(
                '%Y-%m-%d %H:%M:%S')
            end_time = end_time or today.replace(hour=8, minute=0, second=0, microsecond=0).strftime(
                '%Y-%m-%d %H:%M:%S')

        logger.info(f"Fetching data from {start_time} to {end_time}")

        sql = read_file_env(doc_gen_consts.MAIL_PRINT_EXPORT_JSON_SQL_PATH, self.deploy_env).format(
            unique_folder_path=unique_folder_path,
            start_time=start_time,
            end_time=end_time,
            est_zone=self.est_zone
        )

        logger.info("Executing SQL query to generate JSON file(s) for data processing to AFP.")
        run_bq_query(sql)
        logger.info("JSON file(s) created.")

    def _get_identity_token(self, audience: str) -> str:
        """Fetches an identity token for the Cloud Function authorization."""
        auth_req = google.auth.transport.requests.Request()
        return google.oauth2.id_token.fetch_id_token(auth_req, audience)

    def _read_in_chunks(self, file, chunk_size):
        while True:
            lines = list(islice(file, chunk_size))
            if not lines:
                break
            yield lines

    def _process_record(self, bucket_name: str, unique_folder_path: str, audience: str, identity_token: str, line: str):
        """Parses and sends record to Cloud Function with added bucketName property."""
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {identity_token}"}
        json_payload = json.loads(line.strip())
        json_payload[doc_gen_consts.BUCKET_NAME_JSON_KEY] = bucket_name
        json_payload[doc_gen_consts.UNIQUE_FOLDER_PATH_JSON_KEY] = unique_folder_path
        path = None
        try:
            response = requests.post(audience, headers=headers, json=json_payload)
            if 200 <= response.status_code <= 299:
                response_json = response.json()
                path = response_json.get(doc_gen_consts.PATH_JSON_KEY)
                processing_time_ms = response_json.get(doc_gen_consts.PROCESSING_TIME_MS_JSON_KEY)
                if processing_time_ms is not None:
                    logger.info(f'PDF generation succeeded: processingTimeMs={processing_time_ms}ms')
                status = doc_gen_consts.PDF_GENERATED
            else:
                content_type = response.headers.get('Content-Type', '').lower()
                if 'application/json' in content_type:
                    response_json = response.json()
                    error_message = response_json.get(doc_gen_consts.ERROR_MESSAGE_JSON_KEY, 'Unknown error')
                    processing_time_ms = response_json.get(doc_gen_consts.PROCESSING_TIME_MS_JSON_KEY)
                    log_msg = f'PDF generation failed: {response.status_code} - errorMessage={error_message}'
                    if processing_time_ms is not None:
                        log_msg += f', processingTimeMs={processing_time_ms}ms'
                    logger.error(log_msg)
                else:
                    # backwards compatible - works with error string
                    logger.error(f'PDF generation failed: {response.status_code} - {response.text}')

                status = doc_gen_consts.PDF_GENERATION_FAILED

        except Exception as err:
            logger.error(f'Error while generating PDF: {err}', exc_info=True, stack_info=True)
            status = doc_gen_consts.PDF_GENERATION_FAILED

        return json_payload.get('code'), json_payload.get('uuid'), status, path

    def process_json_to_pdf(self, dag_id: str, bucket_name: str, unique_folder_path: str,
                            prefix: str, audience: str, max_concurrent_records: int, **kwargs):
        dag_run_id = kwargs['dag_run'].run_id
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=f"{unique_folder_path}/{prefix}"))
        if not blobs:
            logger.info("No files found in GCS. Exiting.")
            return

        identity_token = self._get_identity_token(audience)

        def download_and_process_blob(blob):
            """Downloads a JSONL blob from GCS to temp storage and processes
             records concurrently with a limit on active requests."""
            with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as tmp_file:
                blob.download_to_file(tmp_file)
                tmp_file_path = tmp_file.name

            try:
                with open(tmp_file_path, 'r') as file:
                    for batch in self._read_in_chunks(file, max_concurrent_records):
                        with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                            partial_func = partial(self._process_record,
                                                   bucket_name, unique_folder_path, audience, identity_token)
                            results = list(executor.map(partial_func, batch))

                        values = ', '.join([(
                            f"('{result[0]}', '{result[1]}', '{result[2]}', NULL, '{dag_id}', '{dag_run_id}')"
                            if result[3] is None
                            else f"('{result[0]}', '{result[1]}', '{result[2]}', '{result[3]}', '{dag_id}', '{dag_run_id}')"
                        ) for result in results
                        ])
                        sql = read_file_env(doc_gen_consts.MAIL_PRINT_STATUS_INSERT_PDF_STATUS_SQL_PATH,
                                            self.deploy_env).format(
                            insert_values=values
                        )
                        run_bq_query(sql)
            finally:
                os.remove(tmp_file_path)

        for blob in blobs:
            download_and_process_blob(blob)

    def get_valid_env_variables(self, input_bucket: str, output_bucket: str, unique_folder_path: str,
                                output_file_prefix: str, output_file_extension: str, afp_config: list):
        """Get valid environment variables for paths that exist and have data in the GCS bucket."""
        client = storage.Client()
        bucket = client.bucket(input_bucket)

        current_time = datetime.now().strftime("%Y%m%d%H%M")
        existing_paths_config = []

        for config in afp_config:
            path = f"{unique_folder_path}/{config.get('input_path')}"
            blobs = bucket.list_blobs(prefix=path)
            if any(True for _ in blobs):
                existing_paths_config.append(config)

        valid_env_variables = [{
            'INPUT_BUCKET_NAME': input_bucket,
            'INPUT_PDFS_PATH': f"{unique_folder_path}/{path.get('input_path')}",
            'OUTPUT_BUCKET_NAME': output_bucket,
            'OUTPUT_FILE_NAME': f"{output_file_prefix}{path.get('output_file_name')}{current_time}{path.get('output_file_suffix')}",
            'OUTPUT_FILE_EXTENSION': output_file_extension,
            'LICENSE_PATH': '/vault/secrets/prolcnse.lic'
        } for path in existing_paths_config]

        return valid_env_variables

    def insert_afp_status_and_check_failures(self, dag_id: str, **kwargs):
        """Insert AFP processing statuses to database and fail the DAG if any workload tasks failed."""
        dag_run_id = kwargs['dag_run'].run_id
        ti = kwargs['ti']
        env_var_list = ti.xcom_pull(task_ids=doc_gen_consts.AFP_PROCESSING_VALID_ENV_VAR_TASK_ID,
                                    key='return_value')

        if env_var_list is None or len(env_var_list) == 0:
            logger.info("No valid workload instances to process, skipping AFP status insertion")
            raise AirflowSkipException("No valid workload instances to process")

        num_of_workload = len(env_var_list)
        failed_workloads = []

        for i in range(num_of_workload):
            path = ti.xcom_pull(task_ids=doc_gen_consts.AFP_PROCESSING_WORKLOAD_TASK_ID,
                                map_indexes=i, key='path')
            outbound_filename = ti.xcom_pull(task_ids=doc_gen_consts.AFP_PROCESSING_WORKLOAD_TASK_ID,
                                             map_indexes=i, key='outbound_filename')
            status = ti.xcom_pull(task_ids=doc_gen_consts.AFP_PROCESSING_WORKLOAD_TASK_ID,
                                  map_indexes=i, key='job_status')
            if status == doc_gen_consts.SUCCESS_TASK_INSTANCE_STATE:
                sql = read_file_env(doc_gen_consts.MAIL_PRINT_STATUS_INSERT_AFP_SUCCESS_SQL_PATH,
                                    self.deploy_env).format(
                    path=path,
                    outbound_filename=outbound_filename,
                    dag_id=dag_id,
                    dag_run_id=dag_run_id
                )
            else:
                failed_workloads.append(i)
                sql = read_file_env(doc_gen_consts.MAIL_PRINT_STATUS_INSERT_AFP_FAILURE_SQL_PATH,
                                    self.deploy_env).format(
                    path=path,
                    dag_id=dag_id,
                    dag_run_id=dag_run_id
                )
            run_bq_query(sql)

        if failed_workloads:
            failed_count = len(failed_workloads)
            logger.error(f"AFP processing failed for {failed_count} out of {num_of_workload} workload instances: {failed_workloads}")
            raise AirflowFailException(f"AFP processing failed for {failed_count} out of {num_of_workload} workload instances")

    def run_sql_to_create_parquet(self, unique_folder_path: str, dag_id: str, **kwargs):
        dag_run_id = kwargs['dag_run'].run_id

        count_sql = read_file_env(doc_gen_consts.MAIL_PRINT_STATUS_COUNT_SQL_PATH, self.deploy_env).format(
            dag_id=dag_id,
            dag_run_id=dag_run_id
        )

        logging.info("Checking if there are records to export to parquet...")
        count_job = run_bq_query(count_sql)
        record_count = 0
        for row in count_job.result():
            record_count = row.record_count

        if record_count == 0:
            logging.info("No records found to create parquet file. Skipping parquet creation and downstream tasks.")
            raise AirflowSkipException("No records found to create parquet file")

        sql = read_file_env(doc_gen_consts.MAIL_PRINT_STATUS_PARQUET_SQL_PATH, self.deploy_env).format(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            unique_folder_path=unique_folder_path
        )

        logging.info(f"{sql}")

        logging.info("Running Sql to create parquet file for all statuses created through this DAG execution.")
        run_bq_query(sql)
        logging.info("Parquet file(s) created.")

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))
        with (DAG(
                dag_id=dag_id,
                default_args=self.default_args,
                description=(
                    'This DAG processes mail requests by retrieving records from domain_communication.MAIL_REQUEST. '
                    'The records are then utilized to generate the necessary documents, which are sent to Broadridge '
                    'for printing and delivery to the customer.'
                ),
                render_template_as_native_obj=True,
                schedule=config[consts.DAG].get(consts.SCHEDULE_INTERVAL),
                start_date=datetime(2025, 1, 1, tzinfo=self.local_tz),
                catchup=False,
                max_active_tasks=20,
                max_active_runs=1,
                dagrun_timeout=timedelta(hours=24),
                tags=config[consts.TAGS],
                is_paused_upon_creation=True
        ) as dag):
            dag_config = config.get(consts.DAG)
            if dag_config.get(consts.READ_PAUSE_DEPLOY_CONFIG):
                is_paused = read_pause_unpause_setting(dag_id, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            start = EmptyOperator(task_id=consts.START_TASK_ID)

            create_unique_folder_path_task = PythonOperator(
                task_id="create_unique_folder_path",
                python_callable=self.create_unique_folder_path
            )

            xcom_unique_folder_path = "{{ task_instance.xcom_pull(task_ids='create_unique_folder_path', key='return_value') }}"
            export_data_task = PythonOperator(
                task_id="export_and_transform_bq_data",
                python_callable=self.fetch_and_export_data,
                op_kwargs={
                    'start_time': "{{ dag_run.conf.get('start_time') }}",
                    'end_time': "{{ dag_run.conf.get('end_time') }}",
                    'unique_folder_path': xcom_unique_folder_path
                }
            )

            cloud_function_config = dag_config.get('cloud_function')
            generate_pdfs_task = PythonOperator(
                task_id="process_data_and_generate_pdfs",
                python_callable=self.process_json_to_pdf,
                op_kwargs={
                    'dag_id': dag_id,
                    'bucket_name': dag_config.get("input_bucket"),
                    'unique_folder_path': xcom_unique_folder_path,
                    'prefix': "data/",
                    'audience': cloud_function_config.get("audience"),
                    'max_concurrent_records': cloud_function_config.get("max_concurrent_records")
                }
            )

            if self.deploy_env == consts.DEV_ENV:
                noop_afp_task = EmptyOperator(task_id="no_op_afp")
                generate_and_upload_afp_task = noop_afp_task
            else:
                workload_config = dag_config.get('workload')
                workload_env_config = workload_config.get(self.deploy_env)

                @task_group(group_id="afp_processing")
                def afp_processing():
                    get_valid_env_variables_task = PythonOperator(
                        task_id="get_valid_env_variables",
                        python_callable=self.get_valid_env_variables,
                        op_kwargs={
                            'input_bucket': dag_config.get('input_bucket'),
                            'output_bucket': workload_env_config.get('output_bucket'),
                            'unique_folder_path': xcom_unique_folder_path,
                            'output_file_prefix': workload_env_config.get('afp_file_prefix'),
                            'output_file_extension': workload_env_config.get('afp_file_extension'),
                            'afp_config': workload_config.get('afp')
                        }
                    )

                    def on_callback_func(context):
                        ti = context['ti']
                        ti.xcom_push(key="job_status", value=ti.state)

                    generate_and_upload_afp_workload = CustomGKEStartPodOperator.partial(
                        task_id="generate_and_upload_afp",
                        name="pdf-to-afp-converter-pod",
                        namespace=workload_config.get('namespace'),
                        project_id=workload_env_config.get(consts.PROJECT_ID),
                        location=dag_config[consts.LOCATION],
                        cluster_name=workload_env_config.get(consts.CLUSTER_NAME),
                        image=workload_env_config.get('image'),
                        service_account_name="pdf-job-runner",
                        node_selector={'nodepoolpurpose': 'batch'},
                        image_pull_secrets=[V1LocalObjectReference(
                            name='docker-registry'
                        )],
                        tolerations=[V1Toleration(
                            key='nodepoolpurpose',
                            operator='Equal',
                            value='batch',
                            effect='NoSchedule'
                        )],
                        volumes=[V1Volume(
                            name='license-volume',
                            secret=V1SecretVolumeSource(
                                secret_name='secretkv',
                                items=[
                                    {
                                        'key': 'prolcnse.lic.b64',
                                        'path': 'prolcnse.lic'
                                    }
                                ]
                            )
                        )],
                        volume_mounts=[V1VolumeMount(
                            mount_path='/vault/secrets',
                            name='license-volume',
                            read_only=True
                        )],
                        on_success_callback=on_callback_func,
                        is_delete_operator_pod=True
                    ).expand(
                        env_vars=XComArg(get_valid_env_variables_task)
                    )

                    insert_afp_status_and_check_failures = PythonOperator(
                        task_id="insert_afp_status_and_check_failures",
                        trigger_rule=TriggerRule.ALL_DONE,
                        python_callable=self.insert_afp_status_and_check_failures,
                        op_kwargs={
                            'dag_id': dag_id
                        }
                    )
                    generate_and_upload_afp_workload >> insert_afp_status_and_check_failures

                generate_and_upload_afp_task = afp_processing()

            run_sql_to_create_status_parquet = PythonOperator(
                task_id="run_sql_to_create_status_parquet",
                trigger_rule=TriggerRule.ALL_DONE,
                python_callable=self.run_sql_to_create_parquet,
                op_kwargs={
                    'unique_folder_path': xcom_unique_folder_path,
                    consts.DAG_ID: dag_id
                }
            )

            kafka_config = dag_config.get('kafka_config')
            kafka_writer_task = TriggerDagRunOperator(
                task_id=kafka_config.get(consts.KAFKA_WRITER_TASK_ID),
                trigger_dag_id=kafka_config.get(consts.KAFKA_TRIGGER_DAG_ID),
                logical_date=datetime.now(self.local_tz),
                conf={
                    consts.BUCKET: kafka_config.get(consts.BUCKET),
                    consts.FOLDER_NAME: f'{kafka_config.get(consts.FOLDER_PREFIX)}/{dag_id}/{xcom_unique_folder_path}',
                    consts.FILE_NAME: kafka_config.get(consts.FILE_NAME),
                    consts.CLUSTER_NAME: kafka_config.get(consts.KAFKA_CLUSTER_NAME)
                },
                wait_for_completion=dag_config.get(consts.WAIT_FOR_COMPLETION),
                poke_interval=dag_config.get(consts.POKE_INTERVAL)
            )

            end = EmptyOperator(task_id=consts.END_TASK_ID)

            start >> create_unique_folder_path_task >> export_data_task >> generate_pdfs_task \
                >> generate_and_upload_afp_task

            # conditional dependencies
            # Ensure that kafka events are sent in case of following scenarios
            # - PDF status were created (regardless of AFP statuses)
            # - AFP status were created
            generate_and_upload_afp_task >> run_sql_to_create_status_parquet
            run_sql_to_create_status_parquet >> kafka_writer_task

            # Fail the DAG if either of the tasks fail
            generate_pdfs_task >> end
            generate_and_upload_afp_task >> end
            kafka_writer_task >> end

            return add_tags(dag)

    def create_all_dags(self) -> dict:
        dags = {}
        for dag_id, config in self.dag_config.items():
            dags[dag_id] = self.create_dag(dag_id, config)

        return dags


doc_generation_processing_dags_builder = DocumentGeneratorDagBuilder(doc_gen_consts.DOC_GENERATION_CONFIG_PATH)
globals().update(doc_generation_processing_dags_builder.create_all_dags())
