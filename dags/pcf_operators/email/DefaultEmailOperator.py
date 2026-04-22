from __future__ import annotations

from typing import Sequence

from airflow.models import BaseOperator
from airflow.utils.context import Context

from pcf_operators.email.utils.email_utils import send_email


class DefaultEmailOperator(BaseOperator):
    """
    send an email without attachment
    :param email_sender: email from . (templated)
    :param email_recipients: list of emails to send the email to. (templated) ('...@pcbank.ca,..@loblaw.ca,.....' )
    :param email_subject: subject line for the email. (templated)
    :param email_body: content of the email. (templated)


     send_email_task = DefaultEmailOperator(
                    task_id="send_email",
                    email_sender='...@pcbank.ca',
                    email_recipients='...@pcbank.ca',
                    email_subject='email info',
                    email_body='email content',
                )
    """

    template_fields: Sequence[str] = ("email_sender", "email_recipients", "email_subject", "email_body")
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
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.email_sender = email_sender
        self.email_recipients = email_recipients
        self.email_subject = email_subject
        self.email_body = email_body

    def execute(self, context: Context):
        send_email(self.email_sender, self.email_recipients, self.email_subject, self.email_body)
