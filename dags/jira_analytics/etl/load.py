"""
This module defines methods used to load files to landing zone or curated zone
at different stages of the ETL workflow.
"""

from typing import List, Any

from google.cloud import bigquery

from jira_analytics.config import JiraAnalyticsConfig as Config
from util.bq_utils import load_records_to_bq


def load_records(records: List[Any], table_id: str, chunk: int = 0, batch_size: int = 0, force_truncate: bool = False) -> None:
    """
    This function loads a record from JIRA cloud to BigQuery's landing zone, and waits until
    the loading is finished.

    :param records: Dataclass object to be uploaded.
    :param table_id: The destination table.
    :param chunk: If we are using chunk loading, the index of the chunk.
    :param batch_size: The maximum number of records to load at once.
    :param force_truncate: Flag for always truncating the table before writing
    """
    # Create the load job config:
    if force_truncate:
        load_method = bigquery.WriteDisposition.WRITE_TRUNCATE
    elif chunk > 0 or Config.UPDATES_ONLY:
        load_method = bigquery.WriteDisposition.WRITE_APPEND
    else:
        load_method = bigquery.WriteDisposition.WRITE_TRUNCATE

    # Load the issues if any.
    if len(records) > 0:
        if batch_size != 0 and len(records) > batch_size:
            batches = ((len(records) - 1) // batch_size) + 1
            for records_batch in [records[i * batch_size:min((i + 1) * batch_size, len(records))] for i in
                                  range(batches)]:
                load_records_to_bq(records=records_batch, table_id=table_id, load_method=load_method, timestamp=True)
        else:
            load_records_to_bq(records=records, table_id=table_id, load_method=load_method, timestamp=True)
