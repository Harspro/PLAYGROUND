# for airflow scanning

from bigquery_loader.custom_query_data_flattener_and_load_live_bigquery_table_loader import BigQueryDataFalttenerAndLoader
from bigquery_loader.bigquery_loader_base import LoadingFrequency

# separate out the launching part so that BigQueryCustomQueryDeltaLoader can be reused
globals().update(BigQueryDataFalttenerAndLoader(config_filename='custom_query_data_flattener_and_live_bigquery_table_loader_config.yaml', loading_frequency=LoadingFrequency.Onetime).create_dags())  # pragma: no cover
