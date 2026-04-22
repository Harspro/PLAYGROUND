# for airflow scanning
from airflow import DAG
from tsys_processing.generic_file_loader import GenericFileLoader

globals().update(GenericFileLoader('tsys_acct_history_file_ingestion_config.yaml').create_dags())  # pragma: no cover
