from airflow import settings
from bigquery_db_connector_loader.bigquery_db_connector_base import BigQueryDbConnector

globals().update(BigQueryDbConnector('points_comprehension_config.yaml',
                                     f'{settings.DAGS_FOLDER}/config/bigquery_db_connector_configs')
                 .create_dags())  # pragma: no cover
