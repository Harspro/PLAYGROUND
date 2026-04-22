# for airflow scanning
from airflow import settings
from file_kafka_connector_loader.file_kafka_connector_writer_base import FileKafkaConnectorWriter

# Separating out the launching part so that kafka connector writer can be reused.
globals().update(FileKafkaConnectorWriter('scms_card_embossing_config.yaml',
                                          f'{settings.DAGS_FOLDER}/config/file_kafka_connector_writer_configs')
                 .create_dags())  # pragma: no cover
