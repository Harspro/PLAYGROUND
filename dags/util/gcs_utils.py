import fnmatch
import logging
from typing import List, Tuple
from google.cloud import storage
from google.cloud import resourcemanager_v3
import pyarrow.parquet as pq
import util.constants as consts

logger = logging.getLogger(__name__)


def delete_folder(bucket_name, prefix_name):
    """
    Deletes all objects in a folder
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix_name)
    for blob in blobs:
        blob.delete()


def gcs_file_exists(bucket, path, client=None):
    """
    Checks if a file exists in GCS
    """
    if not client:
        client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(path)

    if not blob.exists():
        logger.info(
            "Provided GCS file gs://{}/{} does not exist.".format(bucket, path))
        return False
    return True


def delete_blobs(bucket_name, files_names):
    """
    Deletes all objects provided in the iterable 'files_names'
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    for file in files_names:
        try:
            blob = bucket.blob(file)
            blob.delete()
            logger.info(f"The file {file} has been successfully deleted.")
        except Exception as err:
            raise logger.error(
                f"An error was encountered while trying to delete {file}",
                err,
                exc_info=True,
                stack_info=True,
            )


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    logger.info(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )


def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    """
    Get the blobs in a given bucket / folder
    """
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(
        bucket_name, prefix=prefix, delimiter=delimiter)

    items = []
    for blob in blobs:
        items.append(blob.name)

    return items


def copy_blob(
        bucket_name, blob_name, destination_bucket_name, destination_blob_name
):
    """Copies a blob from one bucket to another with a new name."""

    logger.info(
        f"Copying the file {bucket_name}/{blob_name} to {destination_bucket_name}/{destination_blob_name}")

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )

    logger.info(
        "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )


def compose_file(bucket_name, source_blob_list, destination_blob_name, **kwargs):
    """Concatenate source blobs into destination blob."""
    # bucket_name = "your-bucket-name"
    # first_blob_name = "first-object-name"
    # second_blob_name = "second-blob-name"
    # destination_blob_name = "destination-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    destination = bucket.blob(destination_blob_name)
    destination.content_type = "text/plain"

    # sources is a list of Blob instances, up to the max of 32 instances per request

    sources = [bucket.get_blob(i) for i in source_blob_list]
    destination.compose(sources)

    logger.info(
        "New composite object {} in the bucket {} was created by combining {}".format(
            destination_blob_name, bucket_name, str(source_blob_list)
        )
    )
    return destination


def read_file(bucket_name: str, blob_name: str):
    """Read a file to string"""

    logger.info(f"Reading file {bucket_name}/{blob_name}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    return blob.download_as_text()


def read_file_latin(bucket_name: str, blob_name: str):
    """Read a file to string"""

    logger.info(f"Reading file {bucket_name}/{blob_name}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    return blob.download_as_text(encoding="latin1")


def read_file_bytes(bucket_name: str, blob_name: str):
    """Read a file to bytes"""

    logger.info(f"Reading file {bucket_name}/{blob_name}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    return blob.download_as_bytes()


def write_file(bucket_name: str, blob_name: str, content: str):
    """Write a string to a file"""

    logger.info(f"Writing to file {bucket_name}/{blob_name}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(content)


def write_file_bytes(bucket_name: str, prefix: str, file_name: str, content: bytes):
    """Write bytes to a file"""

    logger.info(f"Writing to file {bucket_name}/{prefix}/{file_name}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{prefix}/{file_name}")
    with blob.open("wb") as f:
        f.write(content)


def read_file_by_uri(uri: str):
    uri_no_scheme = uri[5:]
    uri_parts = uri_no_scheme.replace('//', '/').split('/', 1)
    return read_file(uri_parts[0], uri_parts[1])


def write_file_by_uri(uri: str, content: str):
    uri_no_scheme = uri[5:]
    uri_parts = uri_no_scheme.replace('//', '/').split('/', 1)
    return write_file(uri_parts[0], uri_parts[1], content)


def get_bucket_project_id(bucket_name: str):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    project_number = bucket.project_number
    projects_client = resourcemanager_v3.ProjectsClient()
    request = resourcemanager_v3.GetProjectRequest(
        name=f"projects/{project_number}", )
    response = projects_client.get_project(request=request)
    return response.project_id


def get_business_bucket(bucket_name_prefix: str, deploy_env: str):
    env_suffix = consts.NP_SUFFIX if deploy_env != consts.PROD_ENV else ''
    return f'{bucket_name_prefix}{env_suffix}'


def cleanup_gcs_folder(bucket_name: str, folder_prefix: str = "", file_patterns: list = None):
    """
    Clean up GCS folder by deleting files matching specified patterns or entire directory

    Args:
        bucket_name: GCS bucket name
        folder_prefix: Folder prefix to search in
        file_patterns: List of file patterns to match (e.g., ['cdic-0201-*']).
                        If None or empty, deletes all files in the folder_prefix.

    Note:
        If file_patterns is not provided, all files and folders in the folder_prefix will be deleted.
        Use with caution when file_patterns is None.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    deleted_count = 0

    # If no file patterns provided, delete entire folder
    if not file_patterns:
        logger.info(f"No file patterns provided. Cleaning entire folder: {bucket_name}/{folder_prefix}")
        blobs = bucket.list_blobs(prefix=folder_prefix)

        for blob in blobs:

            try:
                blob.delete()
                deleted_count += 1
            except Exception as e:
                logger.error(f"Failed to delete blob {blob.name}: {str(e)}")

        logger.info(f"Cleanup completed. Deleted {deleted_count} files from {bucket_name}/{folder_prefix}")
        return

    # Clean files matching specific patterns
    logger.info(f"Cleaning files matching patterns: {file_patterns} in {bucket_name}/{folder_prefix}")
    for pattern in file_patterns:
        # Convert pattern to prefix for listing (remove wildcard)
        prefix = f"{folder_prefix}{pattern.replace('*', '')}"

        logger.info(f"Searching for files with prefix: {prefix}")
        blobs = bucket.list_blobs(prefix=prefix)

        for blob in blobs:

            # Check if blob matches any of the patterns
            blob_filename = blob.name.replace(folder_prefix, '')
            pattern_match = False
            for p in file_patterns:
                pattern_prefix = p.replace('*', '')
                if blob_filename.startswith(pattern_prefix):
                    pattern_match = True
                    break
            if pattern_match:
                try:
                    blob.delete()
                    deleted_count += 1
                except Exception as e:
                    logger.error(f"Failed to delete blob {blob.name}: {str(e)}")
    logger.info(f"Cleanup completed. Deleted {deleted_count} files from {bucket_name}/{folder_prefix}")


def parse_file_path_pattern(file_path_pattern: str) -> Tuple[str, str]:
    """
    Parse file path pattern into folder path and file pattern.

    :param file_path_pattern: Full file path pattern (e.g., 'folder/file-*.parquet').
    :return: Tuple of (folder_path, file_pattern).
    """
    if '/' in file_path_pattern:
        parts = file_path_pattern.rsplit('/', 1)
        return parts[0] + '/', parts[1]
    return '', file_path_pattern


def get_matching_files(bucket_obj, file_path_pattern: str, file_extensions: List[str] = None) -> List[str]:
    """
    Get list of files matching the pattern from GCS bucket.

    Supports wildcard patterns (e.g., 'folder/file-*.csv') and filters by file extensions.

    :param bucket_obj: GCS bucket object.
    :param file_path_pattern: File path pattern (supports wildcards like *).
    :param file_extensions: List of allowed file extensions (e.g., ['.csv', '.parquet']).
                           If None, returns all matching files.
    :return: List of matching file paths.
    """
    folder_path, file_pattern = parse_file_path_pattern(file_path_pattern)

    # Handle wildcard patterns
    if '*' in file_pattern:
        blobs = list(bucket_obj.list_blobs(prefix=folder_path))
        matching_files = []

        for blob in blobs:
            filename = blob.name.split('/')[-1]

            # Check if filename matches pattern (case-insensitive)
            if not fnmatch.fnmatch(filename.lower(), file_pattern.lower()):
                continue

            # Check file extension if specified
            if file_extensions:
                if not any(blob.name.lower().endswith(ext.lower()) for ext in file_extensions):
                    continue

            matching_files.append(blob.name)

        return matching_files
    else:
        # Single file
        return [file_path_pattern]


def count_rows_in_file(bucket_obj, file_path: str, has_header: bool = True, file_type: str = None) -> int:
    """
    Count rows in a CSV or Parquet file in GCS.

    File type detection priority:
    1. Explicit file_type parameter (from config: 'CSV' or 'PARQUET')
    2. File extension (.csv or .parquet)

    CSV: Streams line by line, excludes header if has_header=True
    Parquet: Reads only metadata footer (fast, no full download)

    :param bucket_obj: GCS bucket object.
    :param file_path: Path to the file in the bucket.
    :param has_header: For CSV files, whether header row exists (default True).
    :param file_type: Explicit file type from config ('CSV' or 'PARQUET'). If not provided, uses extension.
    :return: Number of data rows in the file.
    """
    blob = bucket_obj.blob(file_path)

    if not blob.exists():
        logger.warning(f"File does not exist: gs://{bucket_obj.name}/{file_path}")
        return 0

    # Determine file type: config takes priority, then extension
    if file_type:
        file_type_upper = file_type.upper()
        is_csv = file_type_upper == 'CSV'
        is_parquet = file_type_upper == 'PARQUET'
        logger.debug(f"Using configured file type: {file_type_upper} for gs://{bucket_obj.name}/{file_path}")
    else:
        # Fallback to extension-based detection
        file_lower = file_path.lower()
        is_csv = file_lower.endswith('.csv')
        is_parquet = file_lower.endswith('.parquet')
        logger.debug(f"Using extension-based detection for gs://{bucket_obj.name}/{file_path}")

    try:
        if is_csv:
            with blob.open('rt', encoding='utf-8') as f:
                row_count = sum(1 for _ in f)

            if has_header and row_count > 0:
                row_count -= 1

            logger.info(f"File gs://{bucket_obj.name}/{file_path} has {row_count} data rows (CSV, has_header={has_header})")
            return row_count

        elif is_parquet:
            with blob.open('rb') as f:
                parquet_file = pq.ParquetFile(f)
                row_count = parquet_file.metadata.num_rows

            logger.info(f"File gs://{bucket_obj.name}/{file_path} has {row_count} data rows (Parquet)")
            return row_count

        else:
            logger.warning(f"Unsupported file type for: gs://{bucket_obj.name}/{file_path}. Supported: .csv, .parquet or file_type='CSV'/'PARQUET'")
            return 0

    except Exception as e:
        logger.error(f"Error counting rows in gs://{bucket_obj.name}/{file_path}: {str(e)}")
        return 0


def move_gcs_objects(source_bucket_name: str, destination_bucket_name: str, file_paths: List[str],
                     delete_source: bool = True, client: storage.Client = None) -> List[str]:
    """Move or copy objects from one GCS bucket to another.

    Copies blobs from source bucket to destination bucket, optionally deleting
    the source blobs after successful copy (for move operation).

    Args:
        source_bucket_name: Name of the source GCS bucket
        destination_bucket_name: Name of the destination GCS bucket
        file_paths: List of file paths to move (relative paths within buckets)
        delete_source: If True, delete source blob after copying (default: True for move)
        client: Optional GCS client to reuse (creates new if not provided)

    Returns:
        List of successfully moved/copied file paths

    Raises:
        Exception: If any file operation fails
    """
    if not file_paths:
        logger.info("No files to move. Skipping move operation.")
        return []

    if not client:
        client = storage.Client()

    source_bucket = client.bucket(source_bucket_name)
    destination_bucket = client.bucket(destination_bucket_name)

    operation = "moved" if delete_source else "copied"
    operation_present = "Moving" if delete_source else "Copying"
    successfully_processed = []

    for file_path in file_paths:
        logger.info(f"{operation_present} file: gs://{source_bucket_name}/{file_path} -> gs://{destination_bucket_name}/{file_path}")
        try:
            # Copy blob to destination
            source_blob = source_bucket.blob(file_path)
            source_bucket.copy_blob(
                source_blob,
                destination_bucket,
                file_path
            )

            # Delete source blob if move operation
            if delete_source:
                source_blob.delete()

            successfully_processed.append(file_path)
            logger.info(f"Successfully {operation}: {file_path}")

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            raise

    logger.info(f"Total files {operation}: {len(successfully_processed)}")
    return successfully_processed
