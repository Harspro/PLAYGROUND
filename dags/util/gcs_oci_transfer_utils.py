"""
Utility functions for GCS to OCI file transfer operations with parallel processing.
"""

import logging
import time
from dataclasses import dataclass
from typing import Dict, Any, Optional
import requests
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.decorators import task
from airflow.exceptions import AirflowFailException

logger = logging.getLogger(__name__)


@dataclass
class TransferResult:
    """Result of a single file transfer operation."""
    success: bool
    file: str
    error: Optional[str] = None


def _transfer_single_file_impl(
    gcp_conn_id: str,
    gcs_bucket: str,
    item: Dict[str, Any],
    oci_base_url: str,
    max_retries: int = 3,
    retry_delay: int = 1,
    stream_timeout: int = 300
) -> TransferResult:
    """Transfer a single file from GCS to OCI with retry logic."""
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    gcs_client = gcs_hook.get_conn()
    bucket = gcs_client.bucket(gcs_bucket)

    filename = item["file"]
    gcs_object_name = item["gcs_path"]
    oci_filename = item["oci_path"]
    file_source = item["source"]

    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/octet-stream",
        "Connection": "close"
    })

    try:
        for attempt in range(max_retries):
            try:
                logger.info(
                    f"Transferring {filename} from {file_source} (attempt {attempt + 1}/{max_retries})"
                )

                blob = bucket.blob(gcs_object_name)

                if not blob.exists():
                    logger.warning(
                        f"File {gcs_object_name} does not exist in GCS bucket"
                    )
                    return TransferResult(success=False, file=filename, error="File does not exist in GCS")

                oci_upload_url = f"{oci_base_url.rstrip('/')}/{oci_filename.lstrip('/')}"

                with blob.open("rb") as gcs_stream:
                    response = session.put(
                        oci_upload_url,
                        data=gcs_stream,
                        timeout=stream_timeout,
                        headers=session.headers,
                        allow_redirects=True,
                    )

                if response.status_code in [200, 201, 204]:
                    logger.info(f"Successfully transferred {filename}")
                    return TransferResult(success=True, file=filename)
                else:
                    resp_text_snippet = (response.text or "")[:500]
                    logger.warning(
                        f"Failed to upload {filename} to OCI. Status: {response.status_code}, Response: {resp_text_snippet}"
                    )
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (attempt + 1))
                    else:
                        return TransferResult(
                            success=False,
                            file=filename,
                            error=f"HTTP {response.status_code}: {resp_text_snippet}"
                        )

            except requests.exceptions.Timeout:
                logger.warning(
                    f"Timeout transferring {filename} (attempt {attempt + 1}/{max_retries})"
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    return TransferResult(
                        success=False,
                        file=filename,
                        error="Timeout after multiple attempts"
                    )
            except requests.exceptions.RequestException as e:
                logger.error(
                    f"Error transferring {filename} (attempt {attempt + 1}/{max_retries}): {str(e)}"
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    return TransferResult(success=False, file=filename, error=str(e))
            except Exception as e:
                logger.error(
                    f"Unexpected error transferring {filename} (attempt {attempt + 1}/{max_retries}): {str(e)}"
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    return TransferResult(success=False, file=filename, error=str(e))
    finally:
        session.close()


@task
def gcs_to_oci_file_transfer_task(
    gcp_conn_id: str,
    gcs_bucket: str,
    item: Dict[str, Any],
    oci_base_url: str,
    max_retries: int = 3,
    retry_delay: int = 1,
    stream_timeout: int = 300
) -> Dict[str, Any]:
    """Airflow task for transferring a single file from GCS to OCI using Dynamic Task Mapping."""
    result = _transfer_single_file_impl(
        gcp_conn_id=gcp_conn_id,
        gcs_bucket=gcs_bucket,
        item=item,
        oci_base_url=oci_base_url,
        max_retries=max_retries,
        retry_delay=retry_delay,
        stream_timeout=stream_timeout
    )
    return {
        "success": result.success,
        "file": result.file,
        "error": result.error
    }


@task
def summarize_transfer_results_task(transfer_results):
    """Summarize the results from all parallel transfer tasks."""
    total_successful = 0
    total_failed = []

    for result in transfer_results:
        if result["success"]:
            total_successful += 1
        else:
            total_failed.append({
                "file": result["file"],
                "error": result["error"]
            })

    success = len(total_failed) == 0
    summary = f"Transfer completed: {total_successful}/{total_successful + len(total_failed)} files transferred successfully"
    if total_failed:
        summary += f", {len(total_failed)} files failed"

    logger.info(summary)

    if total_failed:
        logger.warning(f"Failed transfers: {len(total_failed)} files")
        for failed in total_failed:
            logger.error(f"Failed to transfer {failed['file']}: {failed['error']}")

    if not success:
        raise AirflowFailException(f"Transfer failed for {len(total_failed)} files")
