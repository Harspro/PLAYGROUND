from airflow import settings
from file_kafka_connector_loader.file_kafka_connector_writer_base import FileKafkaConnectorWriter

# Separating out the launching part so that kafka connector writer can be reused.
globals().update(FileKafkaConnectorWriter('tsys_tcs_purge_image_metadata_bq_kafka_config.yaml',
                                          f'{settings.DAGS_FOLDER}/config/file_kafka_connector_writer_configs')
                 .create_dags())
