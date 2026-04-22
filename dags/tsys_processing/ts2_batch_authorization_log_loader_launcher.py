# for airflow scanning
from airflow import DAG, settings
from tsys_processing.ts2_batch_authorization_log_file_loader import Ts2BatchAuthLogFileLoader

globals().update(Ts2BatchAuthLogFileLoader('ts2_batch_authorization_log_config.yaml').create_dags())  # pragma: no cover
