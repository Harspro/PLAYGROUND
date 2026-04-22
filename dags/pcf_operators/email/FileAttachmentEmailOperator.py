from __future__ import annotations

import logging
from typing import Sequence

from airflow.models import BaseOperator
from airflow.utils.context import Context

from pcf_operators.email.utils.email_utils import send_email

logger = logging.getLogger(__name__)


class FileAttachmentEmailOperator(BaseOperator):
    """
        send an email with attachment from GCS
        :param email_sender: email from . (templated)
        :param email_recipients: list of emails to send the email to. (templated) ('...@pcbank.ca,..@loblaw.ca,.....' )
        :param email_subject: subject line for the email. (templated)
        :param email_body: content of the email. (templated)
        :param gcs_bucket: gcs bucket from where file need to read.
        :param gcs_file_location: file location and name , need to attach. (csv,pdf,exl.txt)


         send_email_task = FileAttachmentEmailOperator(
                         task_id="send_email",
                         email_sender='...@pcbank.ca',
                         email_recipients='....@pcbank.ca',
                         email_subject='File Attachment Send',
                         email_body='Please check the attachment',
                         attachment_locations=attachment_locations= "[{
                                                                            'bucket_name': 'pcbpds-email-data-np',
                                                                            'file_locations': ['test.txt', 'test1.csv']
                                                                        },
                                                                        {
                                                                            'bucket_name': 'trans-union-outbound-pds-np',
                                                                            'file_locations': ['test3.txt', 'test4.png']
                                                                        }]
                                                                        ")
        """
    template_fields: Sequence[str] = ("email_sender", "email_recipients", "email_subject", "email_body",
                                      "attachment_locations")
    template_fields_renderers = {"email_body": "html",
                                 "email_subject": "text"}
    template_ext: Sequence[str] = (".html",)
    ui_color = "#e6faf9"

    def __init__(
            self,
            *,
            email_sender: str,
            email_recipients: list[str] | str,
            email_subject: str,
            email_body: str,
            attachment_locations: list[dict] | str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.email_sender = email_sender
        self.email_recipients = email_recipients
        self.email_subject = email_subject
        self.email_body = email_body
        self.attachment_locations = attachment_locations

    def execute(self, context: Context):
        attachment_locations = list(eval(self.attachment_locations))
        logger.info("attachment_locations: %s", attachment_locations)
        attachments = attachment_locations
        send_email(self.email_sender, self.email_recipients, self.email_subject, self.email_body,
                   attachments, file_attachment=True)
