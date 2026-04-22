# for airflow scanning

from bigquery_loader.bigquery_custom_query_deltaloader import BigQueryCustomQueryDeltaLoader
from bigquery_loader.bigquery_loader_base import LoadingFrequency

# separate out the launching part so that BigQueryCustomQueryDeltaLoader can be reused
globals().update(BigQueryCustomQueryDeltaLoader(config_filename='bigquery_custom_query_deltaloader_config.yaml', loading_frequency=LoadingFrequency.Daily).create_dags())  # pragma: no cover
