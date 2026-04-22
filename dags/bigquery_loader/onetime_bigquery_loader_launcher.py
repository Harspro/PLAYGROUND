# for airflow scanning
from bigquery_loader.bigquery_loader_base import BigQueryLoader, LoadingFrequency

# separate out the launching part so that BigQueryLoader can be reused
globals().update(BigQueryLoader('onetime_loading_config.yaml', LoadingFrequency.Onetime).create_dags())  # pragma: no cover
