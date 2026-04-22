import logging
from airflow import settings
from file_kafka_connector_loader.file_kafka_connector_writer_base import FileKafkaConnectorWriter

logger = logging.getLogger(__name__)

# Separating out the launching part so that kafka connector writer can be reused.

file_kafka_connector_writer = FileKafkaConnectorWriter(
    'tsys_pcb_customer_level_status.yaml',
    f'{settings.DAGS_FOLDER}/config/file_kafka_connector_writer_configs'
)
# Check if the configuration is empty before creating DAGs
if file_kafka_connector_writer.job_config:
    dags = file_kafka_connector_writer.create_dags()
    globals().update(dags)
else:
    logger.info("tsys_pcb_customer_level_status.yaml is empty. No DAGs created.")
