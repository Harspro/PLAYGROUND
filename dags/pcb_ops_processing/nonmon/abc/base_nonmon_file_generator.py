import csv
import logging
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Final
from typing import TextIO, BinaryIO

import pendulum
import pytz
import util.constants as consts
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import (
    GCSToGCSOperator
)
from airflow.utils.trigger_rule import TriggerRule
from data_transfer.util.constants import DATA_TRANSFER_AUDIT_TABLE_NAME
from data_transfer.util.utils import (
    create_data_transfer_audit_table_if_not_exists
)
from google.cloud import bigquery, storage
from pcf_operators.email.DefaultEmailOperator import DefaultEmailOperator
from util.auditing_utils.audit_util import AuditUtil
from util.auditing_utils.model.audit_record import AuditRecord
from util.bq_utils import apply_column_transformation, create_external_table
from util.gcs_utils import gcs_file_exists
from util.miscutils import read_file_env, read_variable_or_file, read_yamlfile_env
from pcb_ops_processing.utils.constants import TSYS_ID
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

# Constants
TRANSMISSION_ID: Final[str] = "I9999STD"
CLIENT_NUM: Final[str] = "7607"
HEADER_IDNTFR: Final[str] = "SINFHDR"
TRAILER_IDNTFR: Final[str] = "SINFTRL"
NONMON_BODY_IDENTIFIER: Final[str] = "IIIIII"
RECORDS_PER_FILE: Final[int] = 100_000
JULIAN_DT: Final[str] = (
    datetime.now()
    .astimezone(pytz.timezone("America/Toronto"))
    .strftime('%Y%j')
)
EMAIL_TEMPLATE_FILE_NAME: Final[str] = "nonmon-submission-request-template.html"

DAG_DEFAULT_ARGS: Final = {
    "owner": "team-centaurs",
    "capability": "operations",
    "severity": "P2",
    "sub_capability": "nonmon",
    "business_impact": "Operations would not be able to transfer file to the Tsys vendor which send out Memo to the customer",
    "customer_impact": "TBD",
    "depends_on_past": False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False
}


@dataclass
class NonmonFileRegistry:
    output_file: BinaryIO
    output_filename: str
    staging_file: TextIO
    staging_filename: str


def write_header(destination_bucket: str, folder_name: str, output_file_name: str,
                 staging_bucket: str, character_set: str) -> NonmonFileRegistry:
    header = (
        TRANSMISSION_ID.ljust(8)
        + CLIENT_NUM.ljust(4)
        + JULIAN_DT.ljust(7)
        + HEADER_IDNTFR.ljust(7)
    ).ljust(596)

    # Create output blob/file
    output_blob, output_filename = create_output_blob(
        destination_bucket, folder_name, output_file_name
    )
    output_file = output_blob.open("wb")
    output_file.write(header.encode(character_set))

    # create staging file with new line for BQ external table
    staging_blob, staging_filename = create_output_blob(
        staging_bucket, folder_name, output_file_name
    )
    staging_file = staging_blob.open("w")
    staging_file.write(header + "\n")

    return NonmonFileRegistry(output_file, output_filename, staging_file, staging_filename)


# Write trailer and close output file
def write_trailer(file_registry: NonmonFileRegistry,
                  record_count: int, character_set: str) -> None:
    trailer = (
        TRANSMISSION_ID.ljust(8)
        + CLIENT_NUM.ljust(4)
        + JULIAN_DT.ljust(7)
        + str(record_count).zfill(9)
        + TRAILER_IDNTFR.ljust(7)
    ).ljust(596)

    file_registry.output_file.write(trailer.encode(character_set))
    file_registry.output_file.close()

    # Write trailer and close staging file with newline
    file_registry.staging_file.write(trailer + "\n")
    file_registry.staging_file.close()

    logger.info(
        f"Wrote batch with {record_count} records."
    )


def write_record(file_registry: NonmonFileRegistry, nonmon_record: str, character_set: str) -> None:
    # Write fixed-width record (no newline)
    file_registry.output_file.write(nonmon_record.encode(character_set))

    # Write staged record with newline
    file_registry.staging_file.write(nonmon_record + "\n")


def build_trailer_table(table_id: str) -> str:
    return f""" CREATE TABLE IF NOT EXISTS `{table_id}`
                (
                    TRANSMISSION_ID STRING,
                    CLIENT_NUMBER STRING,
                    JULIAN_DATE STRING,
                    TOTAL_RECORD_COUNT STRING,
                    TRAILER_IDENTIFIER STRING,
                    REC_LOAD_TIMESTAMP DATETIME,
                    FILE_NAME STRING,
                )
            """


def build_trailer_query(transformed_views: list, table_id: str) -> str:
    select_queries = []  # List to hold individual SELECT statements

    for view in transformed_views:
        view_id = view['id']
        record = view['columns'].split(',')[0].strip()
        filename = view['columns'].split(',')[1].strip()

        logger.info(f"Building query for view: {view_id}")
        logger.info(f"record :: {record}")
        logger.info(f"filename :: {filename}")

        select_query = f"""
                    SELECT
                        SUBSTR({record}, 1, 8)    AS TRANSMISSION_ID,
                        SUBSTR({record}, 9, 4)    AS CLIENT_NUMBER,
                        SUBSTR({record}, 13, 7)   AS JULIAN_DATE,
                        SUBSTR({record}, 20, 9)   AS TOTAL_RECORD_COUNT,
                        SUBSTR({record}, 29, 8)   AS TRAILER_IDENTIFIER,
                        CURRENT_DATETIME('America/Toronto') AS REC_LOAD_TIMESTAMP,
                        {filename}
                    FROM
                    `{view_id}`
                    WHERE REGEXP_CONTAINS({record}, r'{TRAILER_IDNTFR}')
        """
        select_queries.append(select_query)

    # Join all SELECT queries with UNION ALL
    union_all_query = "\nUNION ALL\n".join(select_queries)

    # Wrap the UNION ALL query with INSERT INTO
    full_query = f"""
    INSERT INTO {table_id}
    {union_all_query}
    """

    logger.info("Final combined query for trailer:")
    logger.info(full_query)

    return full_query


def assemble_output_file_path(
        source_file_path: Path, file_name_prefix: str, file_suffix: str
) -> tuple[str, str, str]:
    folder_name = str(source_file_path.parent)
    source_file_name = source_file_path.name

    # Get the part between last slash and last underscore
    start_pos = len(folder_name) + 1
    end_pos = source_file_name.rindex('_')
    description = source_file_name[start_pos:end_pos]
    current_time = datetime.now().astimezone(
        pytz.timezone("America/Toronto")
    )

    wildcard_output_file_name = (
        f"{file_name_prefix}_{description}_"
        f"{current_time.strftime('%Y%m%d')}*.{file_suffix}"
    )
    output_file_name = f"{file_name_prefix}_{description}.{file_suffix}"
    return (
        f"{file_name_prefix}/{output_file_name}",
        output_file_name,
        f"{file_name_prefix}/{wildcard_output_file_name}"
    )


def create_output_blob(
        bucket_name: str, folder_name: str, output_file_name: str
) -> tuple[storage.Blob, str]:
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    current_time = datetime.now().astimezone(
        pytz.timezone("America/Toronto")
    )
    file_creation_timestamp = current_time.strftime("%Y%m%d%H%M%S")

    base_name, extension = output_file_name.rsplit('.', 1)
    filename = f"{base_name}_{file_creation_timestamp}.{extension}"

    destination_blob_path = f"{folder_name}/{filename}"
    blob = bucket.blob(destination_blob_path)
    return blob, filename


def create_audit_record(
        audit_util: AuditUtil, config: dict, deploy_env, landing_project_id: str,
        processing_project_id: str, **context
) -> None:
    create_data_transfer_audit_table_if_not_exists()
    source_bucket = context["dag_run"].conf.get("bucket")
    source_path = context["dag_run"].conf.get("name")
    source_file_name = Path(source_path).name
    file_suffix = 'prod' if deploy_env == 'prod' else 'uatv'
    destination_bucket = config.get(consts.DESTINATION_BUCKET)
    destination_path, output_file_name, wildcard_destination_path = (
        assemble_output_file_path(
            Path(context["dag_run"].conf.get("name")),
            config.get(consts.FILENAME_PREFIX),
            file_suffix
        )
    )
    logger.info(f"destination_path:{destination_path}, output_file_name: {output_file_name}")
    logger.info(f"wildcard_destination_path: {wildcard_destination_path}")
    audit_record_values = f"""
                        '{landing_project_id}',
                        '',
                        '',
                        'file',
                        'gs://{source_bucket}/{source_path}',
                        '{source_file_name}',
                        '{processing_project_id}',
                        'gs://{destination_bucket}/{wildcard_destination_path}',
                        '{context.get('dag').dag_id}',
                        '{context.get('run_id')}',
                        'NONMON',
                        'manual',
                        '{landing_project_id}',
                        CURRENT_TIMESTAMP()
        """

    audit_record = AuditRecord(
        consts.DOMAIN_TECHNICAL_DATASET_ID,
        DATA_TRANSFER_AUDIT_TABLE_NAME,
        audit_record_values
    )
    audit_util.record_request_received(audit_record)
    context['ti'].xcom_push(key='audit_record', value=asdict(audit_record))
    context['ti'].xcom_push(key='source_path', value=source_path)
    context['ti'].xcom_push(key='output_file_path', value=destination_path)
    context['ti'].xcom_push(key='output_file_name', value=output_file_name)


def create_email(
        config: dict, deploy_env: str, config_dir: str, **context
) -> None:
    email_template_path = f'{config_dir}/{EMAIL_TEMPLATE_FILE_NAME}'
    email_template = read_file_env(email_template_path)
    today = (
        datetime.now()
        .astimezone(pytz.timezone("America/Toronto"))
        .strftime('%d%b%y')
        .upper()
    )
    record_count = str(
        context['ti'].xcom_pull(
            task_ids='create_nonmon_files_task',
            key='file_row_count'
        )
    )
    output_files = context['ti'].xcom_pull(
        task_ids='create_nonmon_files_task',
        key='output_file_list'
    )
    batch_location = "<br>".join(output_files)
    file_name = ", ".join([Path(f).name for f in output_files])
    email_config = config.get(consts.EMAIL)
    logger.info(f"email_config: {email_config}")
    email_body = email_template.format(batch_location=batch_location,
                                       submission_date=today,
                                       file_name=file_name,
                                       run_date=today,
                                       operator_id=TSYS_ID,
                                       batch_type=email_config.get(consts.DESCRIPTION),
                                       approver=email_config.get(consts.APPROVER, ''),
                                       num_accounts_affected=record_count,
                                       description=email_config.get(consts.DESCRIPTION),
                                       purpose=email_config.get(consts.PURPOSE))

    context['ti'].xcom_push(
        key=consts.SUBJECT,
        value=email_config.get(consts.SUBJECT)
    )
    context['ti'].xcom_push(
        key=consts.RECIPIENTS,
        value=email_config.get(consts.RECIPIENTS, {}).get(deploy_env)
    )
    context['ti'].xcom_push(key='email_body', value=email_body)


def count_csv_rows(config: dict, source_path: str, **context) -> None:
    staging_bucket = config.get(consts.STAGING_BUCKET)

    if not gcs_file_exists(staging_bucket, source_path):
        raise ValueError(
            f"File {source_path} not found in bucket {staging_bucket}"
        )

    client = storage.Client()
    bucket = client.bucket(staging_bucket)
    blob = bucket.blob(source_path)

    with blob.open("r") as file:
        total_rows = sum(1 for _ in file) - 1  # skips header row

    # Calculate batches
    num_batches = (total_rows + RECORDS_PER_FILE - 1) // RECORDS_PER_FILE
    logger.info(f"Input File :::: {source_path} has {total_rows} rows.")
    logger.info(
        f"Number of Batch files to be created :::: {num_batches}"
    )

    context['ti'].xcom_push(key='total_rows', value=total_rows)
    context['ti'].xcom_push(key='num_batches', value=num_batches)


class BaseNonmonFileGenerator(ABC):

    def __init__(self, config_filename: str):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.config_dir = (
            f'{settings.DAGS_FOLDER}/pcb_ops_processing/nonmon/config'
        )
        self.deploy_env = self.gcp_config.get(
            consts.DEPLOYMENT_ENVIRONMENT_NAME
        )
        self.config = read_yamlfile_env(
            f'{self.config_dir}/{config_filename}',
            self.deploy_env
        )

        # Handle case when config is archived/missing
        if self.config is None:
            logger.warning(f"Config file {config_filename} not found.")
            self.config = {}

        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)
        self.landing_project_id = self.gcp_config.get(
            consts.LANDING_ZONE_PROJECT_ID
        )
        self.processing_project_id = self.gcp_config.get(
            consts.PROCESSING_ZONE_PROJECT_ID
        )
        self.default_args = deepcopy(DAG_DEFAULT_ARGS)
        self.audit_util = AuditUtil('create_audit_record', 'audit_record')

    @abstractmethod
    def generate_nonmon_record(self, account_id: str, original_row=None) -> str:
        """
        Generate fixed-width nonmon record.
        Extract required data from original_row as needed.
        """
        pass

    @abstractmethod
    def build_nonmon_table(self, table_id: str) -> str:
        """
        Build nonmon table DDL.
        """
        pass

    @abstractmethod
    def build_nonmon_query(
            self, transformed_views: list, table_id: str
    ) -> str:
        """
        Build nonmon query for inserting transformed data into BigQuery.
        """
        pass

    def create_nonmon_files(
            self, config: dict, source_path: str, output_file_path: str,
            output_file_name: str, **context
    ) -> None:
        client = storage.Client()
        staging_bucket = config.get(consts.STAGING_BUCKET)
        bucket = client.bucket(staging_bucket)
        blob = bucket.blob(source_path)
        folder_name = str(Path(output_file_path).parent)

        logger.info(f"Source bucket: {staging_bucket}")
        logger.info(f"Source path: {source_path}")

        destination_bucket = config.get(consts.DESTINATION_BUCKET)
        character_set = config.get(consts.CHARACTER_SET)

        # Pull total_rows from XCom (output of count_csv_rows task)
        ti = context['ti']
        total_rows = ti.xcom_pull(task_ids='row_count_task', key='total_rows')
        num_batches = ti.xcom_pull(
            task_ids='row_count_task',
            key='num_batches'
        )

        logger.info(f"Total rows: {total_rows}")
        logger.info(f"Number of output files: {num_batches}")

        row_count = 0
        batch_row_count = 0
        output_file_list = []
        staging_file_list = []

        file_registry = write_header(destination_bucket, folder_name, output_file_name,
                                     staging_bucket, character_set)

        with blob.open("r") as input_file:
            csv_reader = csv.reader(input_file)
            next(csv_reader)  # Skip CSV header

            for row in csv_reader:
                account_id = row[0]
                try:
                    nonmon_record = self.generate_nonmon_record(
                        account_id, original_row=row
                    )
                except ValueError as e:
                    logger.error(f"Error processing row: {row} - {e}")
                    raise

                write_record(file_registry, nonmon_record, character_set)

                row_count += 1
                batch_row_count += 1

                if batch_row_count >= RECORDS_PER_FILE:
                    write_trailer(file_registry, batch_row_count + 2, character_set)
                    output_file_list.append(f"gs://{destination_bucket}/{folder_name}/{file_registry.output_filename}")
                    staging_file_list.append(f"gs://{staging_bucket}/{folder_name}/{file_registry.staging_filename}")
                    # Prepare next batch
                    batch_row_count = 0

                    if row_count < total_rows:
                        file_registry = write_header(destination_bucket, folder_name, output_file_name,
                                                     staging_bucket, character_set)

            # Handle leftover records if any
            if batch_row_count > 0:
                write_trailer(file_registry, batch_row_count + 2, character_set)
                output_file_list.append(f"gs://{destination_bucket}/{folder_name}/{file_registry.output_filename}")
                staging_file_list.append(f"gs://{staging_bucket}/{folder_name}/{file_registry.staging_filename}")

        logger.info(f"Total rows processed: {row_count}")
        logger.info(f"Output files: {output_file_list}")
        logger.info(f"Staging files: {staging_file_list}")

        # Push metadata to XCom
        context['ti'].xcom_push(key='file_row_count', value=row_count)
        context['ti'].xcom_push(key='output_file_list', value=output_file_list)
        context['ti'].xcom_push(key='staging_file_list', value=staging_file_list)

    def write_to_bq(self, config: dict, **context) -> None:
        staging_file_list = context['ti'].xcom_pull(
            task_ids='create_nonmon_files_task',
            key='staging_file_list'
        )
        bq_conf = config.get(consts.BIGQUERY)
        project_id = bq_conf.get(consts.PROJECT_ID)
        dataset_id = bq_conf.get(consts.DATASET_ID)
        table_name = bq_conf.get(consts.TABLE_NAME)
        trailer_table_name = bq_conf.get(
            consts.TRAILER_SEGMENT_NAME, {}
        ).get(consts.TABLE_NAME)
        bq_client = bigquery.Client()
        created_tables = []
        transformed_views = []

        for file_uri in staging_file_list:
            # e.g., pcb_tsys_pcmc_nmsbm__inc987654321_20250707054507.uatv
            file_name_with_ext = Path(file_uri).name
            file_name = Path(file_name_with_ext).stem  # removes .uatv

            # Build external table ID
            ext_table_id = f"{dataset_id}.{file_name.upper()}_DBEXT"
            ext_table = create_external_table(
                bq_client, ext_table_id, file_uri, file_format='CSV'
            )
            additional_columns = [f"'{file_name_with_ext}' as FILE_NAME"]
            view_tf = apply_column_transformation(
                bq_client, ext_table, additional_columns
            )
            created_tables.append(ext_table_id)
            transformed_views.append(view_tf)

            logger.info(f"Created external table: {ext_table_id}")
            logger.info(f"Transformed View: {transformed_views}")

        nonmon_table_id = f"{project_id}.{dataset_id}.{table_name}"
        trailer_table_id = f"{project_id}.{dataset_id}.{trailer_table_name}"
        nonmon_ddl = self.build_nonmon_table(nonmon_table_id)
        bq_client.query(nonmon_ddl).result()
        trailer_ddl = build_trailer_table(trailer_table_id)
        bq_client.query(trailer_ddl).result()
        nonmon_query = self.build_nonmon_query(
            transformed_views, nonmon_table_id
        )
        bq_client.query(nonmon_query).result()
        trailer_query = build_trailer_query(
            transformed_views, trailer_table_id
        )
        bq_client.query(trailer_query).result()

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        self.default_args.update(
            dag_config.get(consts.DEFAULT_ARGS, DAG_DEFAULT_ARGS)
        )
        dag = DAG(
            dag_id=dag_id,
            schedule=None,
            render_template_as_native_obj=True,
            start_date=datetime(2025, 1, 1, tzinfo=self.local_tz),
            is_paused_upon_creation=True,
            max_active_runs=1,
            catchup=False,
            default_args=self.default_args
        )
        with dag:
            start = EmptyOperator(task_id='start')
            end = EmptyOperator(task_id='end')

            create_audit_record_task = PythonOperator(
                task_id='create_audit_record',
                python_callable=create_audit_record,
                op_kwargs={
                    'audit_util': self.audit_util,
                    'config': dag_config,
                    'deploy_env': self.deploy_env,
                    'landing_project_id': self.landing_project_id,
                    'processing_project_id': self.processing_project_id
                },
                on_failure_callback=self.audit_util.record_request_failure
            )

            file_staging_task = GCSToGCSOperator(
                task_id="staging_input_file",
                gcp_conn_id=self.gcp_config.get(
                    consts.LANDING_ZONE_CONNECTION_ID
                ),
                source_bucket="{{ dag_run.conf['bucket'] }}",
                source_object="{{ dag_run.conf['name'] }}",
                destination_bucket=dag_config.get(consts.STAGING_BUCKET),
                destination_object="{{ dag_run.conf['name'] }}",
                on_failure_callback=self.audit_util.record_request_failure
            )

            row_count_task = PythonOperator(
                task_id="row_count_task",
                python_callable=count_csv_rows,
                op_kwargs={
                    'config': dag_config,
                    'source_path': (
                        "{{ ti.xcom_pull(task_ids='create_audit_record', "
                        "key='source_path') }}"
                    )
                }
            )

            create_nonmon_files_task = PythonOperator(
                task_id="create_nonmon_files_task",
                python_callable=self.create_nonmon_files,
                op_kwargs={
                    'config': dag_config,
                    'source_path': (
                        "{{ ti.xcom_pull(task_ids='create_audit_record', "
                        "key='source_path') }}"
                    ),
                    'output_file_path': (
                        "{{ ti.xcom_pull(task_ids='create_audit_record', "
                        "key='output_file_path') }}"
                    ),
                    'output_file_name': (
                        "{{ ti.xcom_pull(task_ids='create_audit_record', "
                        "key='output_file_name') }}"
                    )
                }
            )

            write_to_bq_task = PythonOperator(
                task_id='write_to_bq_task',
                python_callable=self.write_to_bq,
                op_kwargs={
                    'config': dag_config
                }
            )

            create_email_task = PythonOperator(
                task_id='create_email',
                python_callable=create_email,
                op_kwargs={
                    'config': dag_config,
                    'deploy_env': self.deploy_env,
                    'config_dir': self.config_dir
                },
                trigger_rule=TriggerRule.ALL_SUCCESS,
                on_failure_callback=self.audit_util.record_request_failure,
                on_success_callback=self.audit_util.record_request_success
            )

            send_email_task = DefaultEmailOperator(
                task_id="send_email",
                email_sender="no-reply@pcbank.ca",
                email_recipients=(
                    "{{ ti.xcom_pull(task_ids='create_email', "
                    "key='recipients') }}"
                ),
                email_subject=(
                    "{{ ti.xcom_pull(task_ids='create_email', "
                    "key='subject') }}"
                ),
                email_body=(
                    "{{ ti.xcom_pull(task_ids='create_email', "
                    "key='email_body') }}"
                )
            )

            # Task dependencies
            (
                start
                >> create_audit_record_task
                >> file_staging_task
                >> row_count_task
                >> create_nonmon_files_task
                >> write_to_bq_task
                >> create_email_task
                >> send_email_task
                >> end
            )

            return add_tags(dag)

    def create(self) -> dict:
        dags = {}
        logger.info("dags: %s", dags)

        # Handle case when config_file is empty
        if not self.config:
            logger.warning("No configuration found. Returning empty DAGs dictionary.")
            return dags

        for job_id, config in self.config.items():
            dags[job_id] = self.create_dag(job_id, config)
        return dags
