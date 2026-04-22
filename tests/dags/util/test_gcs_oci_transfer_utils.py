import unittest
from unittest.mock import patch, Mock, MagicMock

from dags.util.gcs_oci_transfer_utils import (
    TransferResult,
    _transfer_single_file_impl,
    summarize_transfer_results_task
)


class TestTransferSingleFileImpl(unittest.TestCase):
    """Test cases for _transfer_single_file_impl function."""

    def setUp(self):
        """Set up test fixtures."""
        self.gcp_conn_id = "test_conn"
        self.gcs_bucket = "test-bucket"
        self.oci_base_url = "https://test-oci.com/upload"
        self.item = {
            "file": "test.sql",
            "gcs_path": "purging/test.sql",
            "oci_path": "test.sql",
            "source": "test folder"
        }

    @patch('dags.util.gcs_oci_transfer_utils.requests.Session')
    @patch('dags.util.gcs_oci_transfer_utils.GCSHook')
    def test_transfer_single_file_success(self, mock_gcs_hook_class, mock_session_class):
        """Test successful file transfer."""
        # Mock GCS Hook
        mock_gcs_hook = Mock()
        mock_gcs_hook_class.return_value = mock_gcs_hook
        mock_gcs_client = Mock()
        mock_gcs_hook.get_conn.return_value = mock_gcs_client
        mock_bucket = Mock()
        mock_gcs_client.bucket.return_value = mock_bucket
        mock_blob = Mock()
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = True
        mock_blob.size = 1024
        mock_blob.reload.return_value = None

        # Mock context manager for blob.open()
        mock_context_manager = MagicMock()
        mock_context_manager.__enter__ = Mock(return_value=Mock())
        mock_context_manager.__exit__ = Mock(return_value=None)
        mock_blob.open.return_value = mock_context_manager

        # Mock requests session
        mock_session = Mock()
        mock_session_class.return_value = mock_session
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "OK"
        mock_response.json.return_value = {}
        mock_response.raise_for_status.return_value = None
        mock_session.put.return_value = mock_response
        mock_session.headers = {}

        # --- Act ---
        result = _transfer_single_file_impl(
            gcp_conn_id=self.gcp_conn_id,
            gcs_bucket=self.gcs_bucket,
            item=self.item,
            oci_base_url=self.oci_base_url
        )

        # --- Assert ---
        self.assertIsInstance(result, TransferResult)
        self.assertTrue(result.success)
        self.assertEqual(result.file, "test.sql")
        self.assertIsNone(result.error)

        # Verify GCS operations
        mock_gcs_hook_class.assert_called_once_with(gcp_conn_id=self.gcp_conn_id)
        mock_bucket.blob.assert_called_once_with("purging/test.sql")
        mock_blob.exists.assert_called_once()

        # Verify HTTP request
        mock_session.put.assert_called_once()
        call_args = mock_session.put.call_args
        self.assertEqual(call_args[0][0], "https://test-oci.com/upload/test.sql")

    @patch('dags.util.gcs_oci_transfer_utils.requests.Session')
    @patch('dags.util.gcs_oci_transfer_utils.GCSHook')
    def test_transfer_single_file_blob_not_exists(self, mock_gcs_hook_class, mock_session_class):
        """Test file transfer when blob doesn't exist in GCS."""
        # Mock GCS Hook
        mock_gcs_hook = Mock()
        mock_gcs_hook_class.return_value = mock_gcs_hook
        mock_gcs_client = Mock()
        mock_gcs_hook.get_conn.return_value = mock_gcs_client
        mock_bucket = Mock()
        mock_gcs_client.bucket.return_value = mock_bucket
        mock_blob = Mock()
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = False

        # --- Act ---
        result = _transfer_single_file_impl(
            gcp_conn_id=self.gcp_conn_id,
            gcs_bucket=self.gcs_bucket,
            item=self.item,
            oci_base_url=self.oci_base_url
        )

        # --- Assert ---
        self.assertIsInstance(result, TransferResult)
        self.assertFalse(result.success)
        self.assertEqual(result.file, "test.sql")
        self.assertEqual(result.error, "File does not exist in GCS")


class TestSummarizeTransferResultsImpl(unittest.TestCase):
    """Test cases for the actual implementation logic of summarize_transfer_results_task."""

    def test_summarize_impl_with_failures(self):
        """Test summarize implementation with some failed transfers."""
        # Mock transfer results with failures
        transfer_results = [
            {"success": True, "file": "file1.sql", "error": None},
            {"success": False, "file": "file2.csv", "error": "HTTP 500"},
            {"success": True, "file": "file3.txt", "error": None},
            {"success": False, "file": "file4.sql", "error": "Timeout"}
        ]

        # Get the actual function from the task decorator
        actual_function = summarize_transfer_results_task.function

        # --- Act ---
        with self.assertRaises(Exception) as cm:
            actual_function(transfer_results)

        # --- Assert ---
        self.assertIn("Transfer failed for 2 files", str(cm.exception))


if __name__ == '__main__':
    unittest.main()
