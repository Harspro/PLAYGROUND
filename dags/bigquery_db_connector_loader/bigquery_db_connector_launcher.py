from airflow import settings
from bigquery_db_connector_loader.bigquery_db_connector_base import BigQueryDbConnector

# Separating out the launching part so that bigquery db connector can be reused.
globals().update(BigQueryDbConnector('bigquery_db_connector_config.yaml',
                                     f'{settings.DAGS_FOLDER}/config/bigquery_db_connector_configs')
                 .create_dags())  # pragma: no cover
