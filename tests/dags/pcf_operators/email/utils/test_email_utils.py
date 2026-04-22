import io
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowException
from pcf_operators.email.utils.constants import MAX_FILE_ATTACHMENT_LIMIT, MAX_FILE_SIZE_LIMIT
from pcf_operators.email.utils.email_utils import (validate_emails,
                                                   validate_gcs_obj_size,
                                                   validate_email_body_size,
                                                   create_html_table,
                                                   send_email)


def test_validate_emails_valid_internal_emails():
    with patch('pcf_operators.email.utils.email_utils.validate_email') as mock_validate_email, \
            patch('pcf_operators.email.utils.email_utils.logger') as mock_logger:
        mock_validate_email.return_value = True

        valid = validate_emails(['user1@pcbank.ca', 'user2@loblaw.ca'], 'sender@pcbank.ca')

        assert valid is True
        mock_logger.error.assert_not_called()


def test_validate_emails_invalid_email_format():
    with patch('pcf_operators.email.utils.email_utils.validate_email') as mock_validate_email, \
            patch('pcf_operators.email.utils.email_utils.logger') as mock_logger:
        mock_validate_email.side_effect = lambda x: x != 'invalid-email'

        valid = validate_emails(['user1@pcbank.ca', 'invalid-email'], 'sender@pcbank.ca')

        assert valid is False
        mock_logger.error.assert_called_once_with("the email address 'invalid-email' is not valid")


def test_validate_emails_external_email_address():
    with patch('pcf_operators.email.utils.email_utils.validate_email') as mock_validate_email, \
            patch('pcf_operators.email.utils.email_utils.logger') as mock_logger:
        mock_validate_email.return_value = True

        valid = validate_emails(['user@external.com'], 'sender@pcbank.ca')

        assert valid is False
        mock_logger.error.assert_called_once_with("'user@external.com' is external email address")


def test_validate_emails_mixed_email_validities():
    with patch('pcf_operators.email.utils.email_utils.validate_email') as mock_validate_email, \
            patch('pcf_operators.email.utils.email_utils.logger') as mock_logger:
        def mock_validate(email):
            return email in ['user1@pcbank.ca', 'user2@external.com', 'sender@pcbank.ca']

        mock_validate_email.side_effect = mock_validate

        valid = validate_emails(['user1@pcbank.ca', 'user2@external.com', 'invalid-email'], 'sender@pcbank.ca')

        assert valid is False
        mock_logger.error.assert_any_call("the email address 'invalid-email' is not valid")
        mock_logger.error.assert_any_call("'user2@external.com' is external email address")


def test_validate_gcs_obj_size_success():
    with patch('pcf_operators.email.utils.email_utils.storage.Client') as mock_client, \
            patch('pcf_operators.email.utils.email_utils.logger') as mock_logger, \
            patch('pcf_operators.email.utils.email_utils.gcs_file_exists', return_value=True) as mock_gcs_file_exists:
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.size = 1500 * 1000  # 1.5 MB
        mock_bucket.get_blob.return_value = mock_blob
        mock_client.return_value.bucket.return_value = mock_bucket

        attachment_locations = [
            {'bucket_name': 'test_bucket', 'file_locations': ['file1', 'file2']}
        ]

        valid = validate_gcs_obj_size(attachment_locations)

        assert valid is True
        mock_logger.info.assert_any_call("The file size is: 3000.0kb")
        mock_logger.info.assert_any_call("Total files attached: 2")
        mock_gcs_file_exists.assert_called()


def test_validate_gcs_obj_size_exceeds_file_count_limit():
    with patch('pcf_operators.email.utils.email_utils.storage.Client') as mock_client, \
            patch('pcf_operators.email.utils.email_utils.gcs_file_exists', return_value=True) as mock_gcs_file_exists:
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.size = 1 * 1000  # 1KB
        mock_bucket.get_blob.return_value = mock_blob
        mock_client.return_value.bucket.return_value = mock_bucket

        file_locations = [f'file{x}' for x in range(MAX_FILE_ATTACHMENT_LIMIT + 1)]

        attachment_locations = [
            {'bucket_name': 'test_bucket', 'file_locations': file_locations}
        ]

        with pytest.raises(AirflowException,
                           match=f"Number of attached files should not be greater than {MAX_FILE_ATTACHMENT_LIMIT}"):
            validate_gcs_obj_size(attachment_locations)

        mock_gcs_file_exists.assert_called()


def test_validate_gcs_obj_size_file_not_found():
    with patch('pcf_operators.email.utils.email_utils.storage.Client'), \
            patch('pcf_operators.email.utils.email_utils.logger'), \
            patch('pcf_operators.email.utils.email_utils.gcs_file_exists', return_value=False):
        attachment_locations = [
            {'bucket_name': 'test_bucket', 'file_locations': ['file1']}
        ]

        with pytest.raises(AirflowException, match="no matching file:file1 found in bucket:test_bucket"):
            validate_gcs_obj_size(attachment_locations)


def test_validate_gcs_obj_size_exceeds_size_limit():
    with patch('pcf_operators.email.utils.email_utils.storage.Client') as mock_client, \
            patch('pcf_operators.email.utils.email_utils.logger') as mock_logger, \
            patch('pcf_operators.email.utils.email_utils.gcs_file_exists', return_value=True) as mock_gcs_file_exists:
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.size = 30000 * 1000  # 30MB
        mock_bucket.get_blob.return_value = mock_blob
        mock_client.return_value.bucket.return_value = mock_bucket

        attachment_locations = [
            {'bucket_name': 'test_bucket', 'file_locations': ['file1']}
        ]

        valid = validate_gcs_obj_size(attachment_locations)

        assert valid is False
        mock_logger.info.assert_any_call("The file size is: 30000.0kb")
        mock_logger.info.assert_any_call("Total files attached: 1")
        mock_gcs_file_exists.assert_called()


def test_validate_email_body_size_under_limit():
    small_body = "A" * 500  # 500 characters, well under 1MB
    assert validate_email_body_size(small_body) is True


def test_validate_email_body_size_at_limit():
    body_at_limit = "A" * (1048576 // 2)  # Roughly half a MB due to UTF-8 encoding nuances
    assert validate_email_body_size(body_at_limit) is True


def test_validate_email_body_size_over_limit():
    large_body = "A" * 1048576  # 1 MB of literal 'A's
    assert validate_email_body_size(large_body) is False


def test_create_html_table_success():
    with patch('pcf_operators.email.utils.email_utils.storage.Client') as mock_client:
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_client.return_value.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Mock the content of the CSV file read operation
        csv_content = b"col1,col2\nval1,val2\nval3,val4"
        mock_blob.open.return_value = io.BytesIO(csv_content)

        # Mock pandas read_csv
        with patch('pandas.read_csv', return_value=pd.read_csv(io.BytesIO(csv_content))):
            html_table = create_html_table("test_bucket", "test_file.csv")
            expected_html = "<table border=\"1\" class=\"dataframe\">"  # Beginning of an HTML table
            assert html_table.startswith(expected_html)


def test_create_html_table_missing_parameters():
    with pytest.raises(AirflowException, match="The attachment File gs:///test_file.csv does not exist"):
        create_html_table("", "test_file.csv")

    with pytest.raises(AirflowException, match="The attachment File gs://test_bucket/ does not exist"):
        create_html_table("test_bucket", "")


def test_send_email_success():
    with patch('pcf_operators.email.utils.email_utils.validate_email_body_size', return_value=True), \
            patch('pcf_operators.email.utils.email_utils.validate_emails', return_value=True), \
            patch('pcf_operators.email.utils.email_utils.validate_gcs_obj_size', return_value=True), \
            patch('pcf_operators.email.utils.email_utils.storage.Client') as mock_storage_client, \
            patch('pcf_operators.email.utils.email_utils.smtplib.SMTP') as mock_smtp, \
            patch('pcf_operators.email.utils.email_utils.logger') as mock_logger:
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_smtp_instance = MagicMock()
        mock_blob.open.return_value = io.BytesIO(b"attachment_content")
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_smtp.return_value = mock_smtp_instance

        attachments = [{'bucket_name': 'test_bucket', 'file_locations': ['file1']}]

        send_email(
            email_sender='sender@example.com',
            email_recipients='recipient@example.com',
            email_subject='Test Subject',
            email_body='<html><body>Email Content</body></html>',
            attachments=attachments,
            file_attachment=True
        )

        mock_logger.info.assert_any_call("Successfully sent email")
        mock_smtp_instance.sendmail.assert_called_once()


def test_send_email_body_size_exceeded():
    with patch('pcf_operators.email.utils.email_utils.validate_email_body_size', return_value=False), \
            patch('pcf_operators.email.utils.email_utils.logger'):
        with pytest.raises(AirflowException, match="Email body content is larger than the maximum size"):
            send_email(
                email_sender='sender@example.com',
                email_recipients='recipient@example.com',
                email_subject='Test Subject',
                email_body='A' * 2000000,  # large body
                attachments=None,
                file_attachment=False
            )


def test_send_email_invalid_emails():
    with patch('pcf_operators.email.utils.email_utils.validate_email_body_size', return_value=True), \
            patch('pcf_operators.email.utils.email_utils.validate_emails', return_value=False), \
            patch('pcf_operators.email.utils.email_utils.logger'):
        with pytest.raises(AirflowException, match="Email is not Valid"):
            send_email(
                email_sender='sender@example.com',
                email_recipients='invalid-recipient',
                email_subject='Test Subject',
                email_body='Valid Body',
                attachments=None,
                file_attachment=False
            )


def test_send_email_attachment_size_exceeded():
    with patch('pcf_operators.email.utils.email_utils.validate_email_body_size', return_value=True), \
            patch('pcf_operators.email.utils.email_utils.validate_emails', return_value=True), \
            patch('pcf_operators.email.utils.email_utils.validate_gcs_obj_size', return_value=False), \
            patch('pcf_operators.email.utils.email_utils.logger'):
        attachments = [{'bucket_name': 'test_bucket', 'file_locations': ['file1']}]

        with pytest.raises(AirflowException, match="Total size of the attachments is larger than 25Mb"):
            send_email(
                email_sender='sender@example.com',
                email_recipients='recipient@example.com',
                email_subject='Test Subject',
                email_body='Valid Body',
                attachments=attachments,
                file_attachment=True
            )
