# for airflow scanning
from airflow import settings
from file_kafka_connector_loader.file_kafka_connector_writer_base import FileKafkaConnectorWriter

globals().update(FileKafkaConnectorWriter('sfmc_push_device_contact_config.yaml',
                                          f'{settings.DAGS_FOLDER}/config/file_kafka_connector_writer_configs')
                 .create_dags())  # pragma: no cover
