from __future__ import annotations

from pathlib import Path
from typing import Sequence

from airflow import AirflowException
from airflow.models import BaseOperator, Variable
from airflow.utils.context import Context
from google.cloud import bigquery

from pcf_operators.email.utils.email_utils import send_email

email_gcs_bucket = Variable.get('email_gcs_bucket')


class TableAttachmentEmailOperator(BaseOperator):
    """
    Airflow custom operator that attaches a table from BigQuery to an email and send it

        send an email with attachment from GCS
        :param email_sender: email from . (templated)
        :param email_recipients: list of emails to send the email to. (templated) ('...@pcbank.ca,..@loblaw.ca,.....' )
        :param email_subject: subject line for the email. (templated)
        :param email_body: content of the email. (templated)
        :param bq_project_name: GCP project ID. (templated)
        :param bq_dataset_name: Big query dataset name. (templated)
        :param bq_table_name: Big query table name. (templated)        :
        :param gcs_file_name: given name , need to store in bucket. (csv,pdf,exl.txt)


         send_email_task = TableAttachmentEmailOperator(
                         task_id="send_email",
                         email_sender='...@pcbank.ca',
                         email_recipients='....@pcbank.ca',
                         email_subject='File Attachment Send',
                         email_body='Please check the attachment',
                         bq_project_name='pcb-nonprod-pds',
                         bq_dataset_name='CENTAURS',
                         bq_table_name='email_data_np_test',
                         attachment_name='test_bq.csv'
                     )

    """
    template_fields: Sequence[str] = (
        "email_sender", "email_recipients", "email_subject", "email_body", "bq_project_name",
        "bq_dataset_name", "bq_table_name", "attachment_name")
    template_fields_renderers = {"email_body": "html"}
    template_ext: Sequence[str] = (".html",)
    ui_color = "#e6faf9"

    def __init__(
            self,
            *,
            email_sender: str,
            email_recipients: list[str] | str,
            email_subject: str,
            email_body: str,
            bq_project_name: str | None = None,
            bq_dataset_name: str,
            bq_table_name: str,
            attachment_name: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.email_sender = email_sender
        self.email_recipients = email_recipients
        self.email_subject = email_subject
        self.email_body = email_body
        self.bq_project_name = bq_project_name
        self.bq_dataset_name = bq_dataset_name
        self.bq_table_name = bq_table_name
        self.gcs_bucket = email_gcs_bucket
        self.attachment_name = attachment_name

    def execute(self, context: Context) -> None:
        bq_client = bigquery.Client()
        bq_project_id = bq_client.project if self.bq_project_name is None or 'None' else self.bq_project_name
        filename = f"{Path(self.attachment_name).stem}.csv"
        dest_location = f"gs://{self.gcs_bucket}/{filename}"
        bq_table_id = f"{bq_project_id}.{self.bq_dataset_name}.{self.bq_table_name}"
        bq_job_config = bigquery.ExtractJobConfig()
        bq_job_config.destination_format = bigquery.DestinationFormat.CSV
        if self.bq_dataset_name and self.bq_table_name:
            bq_extract_job = bq_client.extract_table(bq_table_id, dest_location, job_config=bq_job_config)
            bq_extract_job.result()
        else:
            raise AirflowException(f"The Table {bq_table_id} does not exist")
        attachments = [{'bucket_name': f'{self.gcs_bucket}', 'file_locations': [f'{filename}']}]
        send_email(self.email_sender, self.email_recipients, self.email_subject, self.email_body,
                   attachments, True)
