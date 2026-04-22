from __future__ import annotations

import logging
import os

from airflow.exceptions import AirflowException
from email_validator import validate_email
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import smtplib
from google.cloud import storage
import pandas as pd

from util.gcs_utils import gcs_file_exists
from pcf_operators.email.utils.constants import MAX_FILE_ATTACHMENT_LIMIT, MAX_FILE_SIZE_LIMIT

logger = logging.getLogger(__name__)


def validate_emails(email_recipients: list[str] | str, email_sender: str):
    domain = ['pcbank.ca', 'loblaw.ca']
    emailaddress_valid_flag = True
    email_recipients.append(email_sender.strip())
    logger.info(f'email_recipients: {email_recipients}')
    for email_address in email_recipients:
        email_valid = validate_email(email_address)
        if email_valid:
            domain_field = email_address.split('@')
            if domain_field[1] not in domain:
                logger.error(f"'{email_address}' is external email address")
                emailaddress_valid_flag = False
        else:
            logger.error(f"the email address '{email_address}' is not valid")
            emailaddress_valid_flag = False
    return emailaddress_valid_flag


def validate_gcs_obj_size(attachment_locations):
    obj_size = 0
    file_count = 0
    for file_location in attachment_locations:
        bucket_name = file_location.get('bucket_name')
        for file_name in file_location.get('file_locations'):
            if file_count >= MAX_FILE_ATTACHMENT_LIMIT:
                raise AirflowException(f"Number of attached files should not be greater than {MAX_FILE_ATTACHMENT_LIMIT}")
            if gcs_file_exists(bucket_name, file_name):
                obj_size = storage.Client().bucket(bucket_name).get_blob(file_name).size / 1000 + obj_size
                file_count += 1
            else:
                raise AirflowException(f"no matching file:{file_name} found in bucket:{bucket_name}")
    logger.info(f"The file size is: {obj_size}kb")
    logger.info(f"Total files attached: {file_count}")
    return obj_size < MAX_FILE_SIZE_LIMIT


def validate_email_body_size(email_body_content: str):
    emai_body_size_bytes = len(email_body_content.encode('utf-8'))
    return emai_body_size_bytes < 1048576


def create_html_table(gcs_bucket, gcs_file_location):
    attachment_location = f"gs://{gcs_bucket}/{gcs_file_location}"
    if gcs_bucket and gcs_file_location:
        bucket_client = storage.Client().bucket(gcs_bucket)
        gcs_blob = bucket_client.blob(gcs_file_location)
        with gcs_blob.open("rb") as file:
            df = pd.read_csv(file)
        table_html = df.to_html()
        return table_html
    else:
        raise AirflowException(f"The attachment File {attachment_location} does not exist")


def send_email(email_sender: str, email_recipients: list[str] | str, email_subject: str, email_body: str,
               attachments=None, file_attachment=False) -> None:
    msg = MIMEMultipart()
    msg['From'] = email_sender
    msg['To'] = email_recipients
    msg['Subject'] = email_subject
    if validate_email_body_size(email_body):
        msg.attach(MIMEText(email_body, "html"))
    else:
        raise AirflowException("Email body content is larger than the maximum size")
    port = 587
    part = MIMEBase('application', 'octet-stream')
    email_recipient_list = email_recipients.split(',')
    email_recipients_add = [email.strip() for email in email_recipient_list]
    if validate_emails(email_recipients_add, email_sender):
        if file_attachment:
            if validate_gcs_obj_size(attachments):
                logger.info(f"attachment_file_path:{attachments}")
                for attachment_file_path in attachments:
                    bucket = attachment_file_path.get('bucket_name')
                    bucket_client = storage.Client().bucket(bucket)
                    for file_name in attachment_file_path.get('file_locations'):
                        gcs_blob = bucket_client.blob(file_name)
                        attachment = gcs_blob.open("rb")
                        part = MIMEApplication(attachment.read())
                        encoders.encode_base64(part)
                        part.add_header('Content-Disposition', "attachments; filename= %s" % os.path.basename(file_name))
                        msg.attach(part)
            else:
                raise AirflowException("Total size of the attachments is larger than 25Mb")
        try:
            smtp_obj = smtplib.SMTP('lclkrsmtp1.gslbint.ngco.com', port)
            logger.info("Made successful SMTP connection")
        except Exception as e:
            logger.critical('Unable to connect email server', exc_info=e)
            raise AirflowException("Error: unable to connect email server")
        try:
            smtp_obj.sendmail(msg['From'], email_recipients_add, msg.as_string())
            logger.info("Successfully sent email")
        except Exception as e:
            logger.critical('Unable to send email', exc_info=e)
            raise AirflowException("Error: unable to send email")
        smtp_obj.quit()
    else:
        raise AirflowException("Email is not Valid")
