# for airflow scanning
from bigquery_loader.bigquery_batchloader import BigQueryBatchLoader
from bigquery_loader.bigquery_loader_base import LoadingFrequency

# separate out the launching part so that BigQueryLoader can be reused
globals().update(BigQueryBatchLoader('chunk_based_loading_config.yaml', LoadingFrequency.Onetime).create_dags())  # pragma: no cover
