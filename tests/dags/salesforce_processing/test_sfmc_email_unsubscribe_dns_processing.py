import os
from datetime import datetime
from unittest.mock import patch, call

import util.constants as consts

from salesforce_processing.sfmc_email_unsubscribe_dns_processing import SfmcEmailUnsubscribeDagBuilder

current_script_directory = os.path.dirname(os.path.abspath(__file__))


@patch("salesforce_processing.sfmc_email_unsubscribe_dns_processing.TriggerDagRunOperator")
@patch("salesforce_processing.sfmc_email_unsubscribe_dns_processing.GCSToGCSOperator")
@patch("salesforce_processing.sfmc_email_unsubscribe_dns_processing.EmptyOperator")
def test_create_dag(mock_empty,
                    mock_gcs_to_gcs,
                    mock_trigger_dag_run):
    builder = SfmcEmailUnsubscribeDagBuilder(f"{current_script_directory}/config/test_config.yaml")
    builder.default_args = {}

    fixed_now = datetime(2024, 4, 2, 9, 0, 0)
    with patch("salesforce_processing.sfmc_email_unsubscribe_dns_processing.datetime") as mock_datetime:
        mock_datetime.now.return_value = fixed_now
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        builder.create_dag("dag_id", builder.dag_config)

    mock_empty.assert_has_calls([
        call(task_id=consts.START_TASK_ID),
        call(task_id=consts.END_TASK_ID)
    ])

    dag_config = builder.dag_config.get(consts.DAG)
    trigger_config = dag_config.get(consts.TRIGGER_CONFIG)

    mock_trigger_dag_run.assert_has_calls([
        call(
            task_id=dag_config.get('file_load_config').get(consts.LOADING_TASK_ID),
            trigger_dag_id=dag_config.get('file_load_config').get(consts.LOADING_DAG_ID),
            logical_date=fixed_now,
            conf={
                'bucket': trigger_config.get(consts.BUCKET),
                'name': trigger_config.get(consts.NAME),
                'folder_name': trigger_config.get(consts.FOLDER_NAME),
                'file_name': trigger_config.get(consts.FILE_NAME),
            },
            wait_for_completion=dag_config.get(consts.WAIT_FOR_COMPLETION),
            poke_interval=dag_config.get(consts.POKE_INTERVAL)),
        call(
            task_id=dag_config.get("kafka_config").get(consts.KAFKA_WRITER_TASK_ID),
            trigger_dag_id=dag_config.get("kafka_config").get(consts.KAFKA_TRIGGER_DAG_ID),
            logical_date=fixed_now,
            conf={
                'bucket': dag_config.get(consts.STAGING_BUCKET),
                'name': trigger_config.get(consts.NAME),
                'folder_name': trigger_config.get(consts.FOLDER_NAME),
                'file_name': trigger_config.get(consts.FILE_NAME),
                'cluster_name': dag_config.get("kafka_config").get(consts.KAFKA_CLUSTER_NAME)
            },
            wait_for_completion=dag_config.get(consts.WAIT_FOR_COMPLETION),
            poke_interval=dag_config.get(consts.POKE_INTERVAL))
    ])

    mock_gcs_to_gcs.assert_called_once_with(
        task_id=consts.GCS_TO_GCS,
        gcp_conn_id='google_cloud_default',
        source_bucket=trigger_config.get(consts.BUCKET),
        source_object=trigger_config.get(consts.NAME),
        destination_bucket=dag_config.get(consts.STAGING_BUCKET),
        destination_object=trigger_config.get(consts.NAME),
    )
