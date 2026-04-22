# for airflow scanning

from bigquery_loader.bigquery_deltaloader import BigQueryDeltaLoader
from bigquery_loader.bigquery_loader_base import LoadingFrequency

# separate out the launching part so that BigQueryLoader can be reused
globals().update(BigQueryDeltaLoader('delta_loading_config.yaml', LoadingFrequency.Daily).create_dags())  # pragma: no cover
