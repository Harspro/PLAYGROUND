import pytz
import logging
import util.constants as consts
from datetime import datetime
from typing import Final

from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from google.cloud import storage
from google.cloud import bigquery
from pathlib import Path
from dataclasses import asdict
from etl_framework.etl_dag_base import ETLDagBase
from util.auditing_utils.audit_util import AuditUtil
from util.auditing_utils.model.audit_record import AuditRecord
from data_transfer.util.utils import create_data_transfer_audit_table_if_not_exists
from data_transfer.util.constants import DATA_TRANSFER_AUDIT_TABLE_NAME
from util.bq_utils import (
    create_external_table,
    apply_column_transformation,
    resolve_placeholders,
    run_bq_query)
from util.miscutils import (
    read_yamlfile_env_suffix,
    read_env_filepattern,
    read_variable_or_file,
    read_file_env
)
from util.gcs_utils import gcs_file_exists
from pcf_operators.email.DefaultEmailOperator import DefaultEmailOperator
from pcb_ops_processing.utils.constants import TSYS_ID, LANGUAGE_CODE_ENGLISH

logger = logging.getLogger(__name__)

RECORD_LENGTH: Final[int] = 128
ESID_HEADER_LENGTH: Final[int] = 6
EBCDIC_LF: Final[bytes] = b'\x25'
EMAIL_TEMPLATE_FILE_NAME: Final[str] = 'draft5-submission-request-template.html'
DEFAULT_ARGS: Final = {
    'owner': 'team-centaurs',
    'capability': 'operations',
    'severity': 'P2',
    'sub_capability': 'fee-reversal',
    'business_impact': 'Operations would not be able to transfer file to the Tsys vendor which cause Customer will not be able to receive the refunds',
    'customer_impact': 'TBD',
    'depends_on_past': False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False
}


def write_ebcdic_line(batch5_file, character_set, line: str, add_newline: bool = True) -> None:
    line = line.ljust(RECORD_LENGTH)
    ebcdic_bytes = line.encode(character_set)

    if add_newline:
        batch5_file.write(ebcdic_bytes + EBCDIC_LF)
    else:
        batch5_file.write(ebcdic_bytes)


def build_transmit_header_str(bank_number: str, julian_date: str, gregorian_date: str, esid: str) -> str:
    esid = esid.ljust(ESID_HEADER_LENGTH)
    return (f'00000019010{bank_number}0001I7607INS            {julian_date}{gregorian_date}{esid}'
            + '                                           000                           N')


def build_batch_header_str(julian_date: str, bank_number: str) -> str:
    return f'00000029012{bank_number}DRFT5{julian_date}0001   CLR00000000000'.ljust(RECORD_LENGTH)


def assemble_output_file_path(product: str, ticket_id: str, file_suffix: str, timestamp: str) -> tuple[str, str]:
    output_file_name = f'pcb_tsys_{product}_monsbm_{ticket_id}_{timestamp}.{file_suffix}'
    output_folder = f'pcb_tsys_{product}_monsbm'

    return f'{output_folder}/{output_file_name}', output_file_name


def create_audit_record(audit_util, config: dict,
                        deploy_env, landing_project_id, processing_project_id, **context) -> None:
    create_data_transfer_audit_table_if_not_exists()
    source_bucket = context["dag_run"].conf.get("bucket")
    source_path = context["dag_run"].conf.get("name")
    source_file_name = Path(source_path).name
    parts = source_file_name.split('_')
    if len(parts) < 5:
        raise ValueError("Invalid filename format. Expected format: pcb_ops_<product>_monsbm_<ticket_id>_<timestamp>.<env>")
    product = source_file_name.split('_')[2].lower()
    ticket_id = source_file_name.split('_')[4]
    file_suffix = 'prod' if deploy_env == 'prod' else 'uatv'
    current_time = datetime.now().astimezone(pytz.timezone("America/Toronto"))
    timestamp = current_time.strftime("%Y%m%d%H%M%S")
    destination_bucket = config.get(consts.DESTINATION_BUCKET)
    destination_path, output_file_name = assemble_output_file_path(product, ticket_id, file_suffix, timestamp)
    audit_record_values = f"""
                        '{landing_project_id}',
                        '',
                        '',
                        'file',
                        'gs://{source_bucket}/{source_path}',
                        '{source_file_name}',
                        '{processing_project_id}',
                        'gs://{destination_bucket}/{destination_path}',
                        '{context.get('dag').dag_id}',
                        '{context.get('run_id')}',
                        'FEE_REVERSAL',
                        'manual',
                        '{landing_project_id}',
                        CURRENT_TIMESTAMP()
          """

    audit_record = AuditRecord(consts.DOMAIN_TECHNICAL_DATASET_ID, DATA_TRANSFER_AUDIT_TABLE_NAME, audit_record_values)
    audit_util.record_request_received(audit_record)
    context['ti'].xcom_push(key='product', value=product)
    context['ti'].xcom_push(key='audit_record', value=asdict(audit_record))
    context['ti'].xcom_push(key='source_path', value=source_path)
    context['ti'].xcom_push(key='output_file_path', value=destination_path)
    context['ti'].xcom_push(key='output_file_name', value=output_file_name)


class Draft5FileGenerator(ETLDagBase):

    def __init__(self, module_name, config_path, config_filename, dag_default_args):
        super().__init__(module_name, config_path, config_filename, dag_default_args)
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.landing_project_id = self.gcp_config.get(consts.LANDING_ZONE_PROJECT_ID)
        self.processing_project_id = self.gcp_config.get(consts.PROCESSING_ZONE_PROJECT_ID)
        self.deploy_env = self.gcp_config.get(consts.DEPLOYMENT_ENVIRONMENT_NAME)
        self.audit_util = AuditUtil('preprocessing_tasks.create_audit_record', 'audit_record')

    def prepare_input_data(self, config: dict, source_path: str, output_file_name: str, product: str) -> None:
        staging_bucket = config.get(consts.STAGING_BUCKET)
        file_uri = f"gs://{staging_bucket}/{source_path}"

        if not gcs_file_exists(staging_bucket, source_path):
            raise ValueError(f"File not found at {file_uri}")

        context_parameters = self.transformation_config.get(consts.CONTEXT_PARAMETER, {})
        ext_table_id = context_parameters.get('fee.reversal.staging.input.external.table.id')

        bq_client = bigquery.Client()
        ext_table = create_external_table(bq_client, ext_table_id, file_uri, file_format='CSV')

        product_config = config.get(product, {})
        if not product_config:
            raise ValueError(f"Missing config for product: {product}")

        bank_number = product_config.get(consts.BANK_NUMBER)
        esid = product_config.get(consts.ESID, '')

        additional_columns = [
            f'{bank_number} AS BANK_NUMBER',
            f"'{esid}' AS ESID",
            f"'{output_file_name}' as FILE_NAME"
        ]

        apply_column_transformation(bq_client, ext_table, add_columns=additional_columns)

    def preprocessing_job(self, config: dict, upstream_task: list) -> list:
        with TaskGroup(group_id='preprocessing_tasks') as tg:
            create_audit_record_task = PythonOperator(
                task_id='create_audit_record',
                python_callable=create_audit_record,
                op_kwargs={
                    'audit_util': self.audit_util,
                    'config': config,
                    'deploy_env': self.deploy_env,
                    'landing_project_id': self.landing_project_id,
                    'processing_project_id': self.processing_project_id
                },
                on_failure_callback=self.audit_util.record_request_failure
            )

            file_staging_task = GCSToGCSOperator(
                task_id="staging_input_file",
                gcp_conn_id=self.gcp_config.get(consts.LANDING_ZONE_CONNECTION_ID),
                source_bucket="{{ dag_run.conf['bucket'] }}",
                source_object="{{ dag_run.conf['name'] }}",
                destination_bucket=config.get(consts.STAGING_BUCKET),
                destination_object="{{ dag_run.conf['name']}}",
                on_failure_callback=self.audit_util.record_request_failure
            )

            create_input_data_table_task = PythonOperator(
                task_id='prepare_input_data',
                python_callable=self.prepare_input_data,
                op_kwargs={
                    'config': config,
                    'source_path': "{{ ti.xcom_pull(task_ids='preprocessing_tasks.create_audit_record', key='source_path') }}",
                    'output_file_name': "{{ ti.xcom_pull(task_ids='preprocessing_tasks.create_audit_record', key='output_file_name') }}",
                    'product': "{{ ti.xcom_pull(task_ids='preprocessing_tasks.create_audit_record', key='product') }}"
                },
                trigger_rule=TriggerRule.ALL_SUCCESS,
                on_failure_callback=self.audit_util.record_request_failure
            )

            create_audit_record_task >> file_staging_task >> create_input_data_table_task

        upstream_task.append(tg)
        return upstream_task

    def write_query_results(self, batch5_file, query_file_path: str, deploy_env: str, character_set, ebcdic: bool = False, add_newline: bool = True) -> None:
        query = read_file_env(query_file_path, deploy_env)

        context_parameters = self.transformation_config.get(consts.CONTEXT_PARAMETER, {})
        query = resolve_placeholders(query, context_parameters)

        query_results = bigquery.Client().query_and_wait(query)
        count = 0

        for row in query_results:
            row_str = "".join(str(v) for v in row.values()).ljust(RECORD_LENGTH)

            if ebcdic:
                encoded = row_str.encode(character_set)
                if add_newline:
                    batch5_file.write(encoded + EBCDIC_LF)
                else:
                    batch5_file.write(encoded)
            else:
                if add_newline:
                    batch5_file.write((row_str + '\n').encode())
                else:
                    batch5_file.write(row_str.encode())

            count += 1

        if count == 0:
            raise AirflowFailException('Result set is empty')

    def write_draft5_file(self, config: dict, output_file_path: str, product: str) -> None:
        storage_client = storage.Client()
        bucket_name = config.get(consts.DESTINATION_BUCKET)
        character_set = config.get(consts.CHARACTER_SET)
        product_config = config.get(product, {})
        if not product_config:
            raise ValueError(f"Missing config for product: {product}")

        bank_number = product_config.get(consts.BANK_NUMBER)
        esid = product_config.get(consts.ESID, '')
        bucket = storage_client.bucket(bucket_name)

        current_time = datetime.now().astimezone(pytz.timezone("America/Toronto"))
        julian_date = current_time.strftime('%j')
        gregorian_date = current_time.strftime('%d%m%y')

        blob = bucket.blob(output_file_path)
        logger.info(f'Creating draft5 file {blob.name} in bucket {bucket_name} (EBCDIC encoded)')

        sql_path = str(Path(__file__).resolve().parent / 'sql')

        with blob.open("wb") as batch5_file:

            write_ebcdic_line(batch5_file, character_set, build_transmit_header_str(bank_number, julian_date, gregorian_date, esid), add_newline=False)
            write_ebcdic_line(batch5_file, character_set, build_batch_header_str(julian_date, bank_number), add_newline=False)

            self.write_query_results(batch5_file, f'{sql_path}/fetch_details.sql', self.deploy_env, character_set, ebcdic=True, add_newline=False)
            self.write_query_results(batch5_file, f'{sql_path}/fetch_batch_trailer.sql', self.deploy_env, character_set, ebcdic=True, add_newline=False)
            self.write_query_results(batch5_file, f'{sql_path}/fetch_transmit_trailer.sql', self.deploy_env, character_set, ebcdic=True, add_newline=False)

    def get_trailer_data(self) -> tuple[str, str, str]:
        query_stmt = """
            SELECT
                RECORD_COUNT,
                NET_AMOUNT,
                TOTAL_REFUND
            FROM `{fee.reversal.staging.sum.view.id}`
        """
        context_parameters = self.transformation_config.get(consts.CONTEXT_PARAMETER, {})
        query = resolve_placeholders(query_stmt, context_parameters)
        results = run_bq_query(query).result().to_dataframe()
        record_count = str(results['RECORD_COUNT'].values[0])
        net_amount = str(results['NET_AMOUNT'].values[0])
        total_refund = str(round(results['TOTAL_REFUND'].values[0], 2))

        return record_count, net_amount, total_refund

    def get_transaction_type(self) -> tuple[str, str]:
        query_stmt = """
            select
                max(TRANS_CODE) AS TRANS_CODE
            FROM `{fee.reversal.staging.input.external.table.id}`
        """
        context_parameters = self.transformation_config.get(consts.CONTEXT_PARAMETER, {})
        query = resolve_placeholders(query_stmt, context_parameters)
        results = run_bq_query(query).result().to_dataframe()
        trans_code = str(results['TRANS_CODE'].values[0])

        trx_code_table_id = context_parameters.get('draft5.ref.transaction.code.table.id')

        query_stmt = f"""
            SELECT
                TRANSACTION_CODE,
                TRANSACTION_TYPE
            FROM `{trx_code_table_id}`   WHERE TRANSACTION_CODE = CAST('{trans_code}' AS INT64)
            AND LANGUAGE_CODE = '{LANGUAGE_CODE_ENGLISH}'
        """
        query = resolve_placeholders(query_stmt, context_parameters)
        results = run_bq_query(query).result().to_dataframe()
        transaction_code = str(results['TRANSACTION_CODE'].values[0])
        transaction_type = str(results['TRANSACTION_TYPE'].values[0])
        return transaction_code, transaction_type

    def create_email(self, config: dict, deploy_env: str, output_file_path: str, product: str, **context) -> None:
        email_template_path = f'{self.config_dir}/{EMAIL_TEMPLATE_FILE_NAME}'
        email_template = read_file_env(email_template_path)
        today = datetime.now().astimezone(pytz.timezone("America/Toronto")).strftime('%d%b%y').upper()
        bucket_name = config.get(consts.DESTINATION_BUCKET)
        record_count, net_amount, total_refund = self.get_trailer_data()
        trans_code, trans_type = self.get_transaction_type()
        batch_location = f'gs://{bucket_name}/{output_file_path}'
        file_name = Path(output_file_path).name
        email_config = config.get(consts.EMAIL)
        subject_code = email_config.get(consts.SUBJECT, "")
        subject = subject_code.format(product=product.upper())
        purpose_code = email_config.get(consts.PURPOSE, "")
        purpose = purpose_code.format(product=product.upper())
        # batch_type needs to be fixed
        email_body = email_template.format(total_records=record_count,
                                           total_amount=total_refund,
                                           batch_location=batch_location,
                                           submission_date=today,
                                           trans_code=trans_code,
                                           trans_type=trans_type,
                                           file_name=file_name,
                                           run_date=today,
                                           operator_id=TSYS_ID,
                                           batch_type=email_config.get(consts.DESCRIPTION),
                                           approver=email_config.get(consts.APPROVER),
                                           num_accounts_affected=record_count,
                                           description=email_config.get(consts.DESCRIPTION),
                                           purpose=purpose)

        context['ti'].xcom_push(key=consts.SUBJECT, value=subject)
        context['ti'].xcom_push(key=consts.RECIPIENTS, value=email_config.get(consts.RECIPIENTS, {}).get(deploy_env))
        context['ti'].xcom_push(key='email_body', value=email_body)

    def postprocessing_job(self, config: dict, upstream_task: list) -> list:
        with TaskGroup(group_id='postprocessing_tasks') as tg:
            create_draft5_file_task = PythonOperator(
                task_id='create_draft5_file',
                python_callable=self.write_draft5_file,
                op_kwargs={
                    'config': config,
                    'output_file_path': "{{ ti.xcom_pull(task_ids='preprocessing_tasks.create_audit_record', key='output_file_path') }}",
                    'product': "{{ ti.xcom_pull(task_ids='preprocessing_tasks.create_audit_record', key='product') }}"
                },
                trigger_rule=TriggerRule.ALL_SUCCESS,
                on_failure_callback=self.audit_util.record_request_failure
            )

            create_email_task = PythonOperator(
                task_id='create_email',
                python_callable=self.create_email,
                op_kwargs={
                    'config': config,
                    'deploy_env': self.deploy_env,
                    'output_file_path': "{{ ti.xcom_pull(task_ids='preprocessing_tasks.create_audit_record', key='output_file_path') }}",
                    'product': "{{ ti.xcom_pull(task_ids='preprocessing_tasks.create_audit_record', key='product') }}"
                },
                trigger_rule=TriggerRule.ALL_SUCCESS,
                on_failure_callback=self.audit_util.record_request_failure,
                on_success_callback=self.audit_util.record_request_success
            )

            send_email_task = DefaultEmailOperator(
                task_id="send_email",
                email_sender="no-reply@pcbank.ca",
                email_recipients="{{ ti.xcom_pull(task_ids='postprocessing_tasks.create_email', key='recipients') }}",
                email_subject="{{ ti.xcom_pull(task_ids='postprocessing_tasks.create_email', key='subject') }}",
                email_body="{{ ti.xcom_pull(task_ids='postprocessing_tasks.create_email', key='email_body') }}"
            )

            create_draft5_file_task >> create_email_task >> send_email_task

        upstream_task.append(tg)
        return upstream_task


globals().update(Draft5FileGenerator(module_name='pcb_ops_processing',
                                     config_path='fee_reversal/config',
                                     config_filename='pcb_ops_fee_reversal_dags_config.yaml',
                                     dag_default_args=DEFAULT_ARGS).generate_dags())
