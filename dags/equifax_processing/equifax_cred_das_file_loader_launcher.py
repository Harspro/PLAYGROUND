# for airflow scanning
from airflow import DAG
from tsys_processing.generic_file_loader import GenericFileLoader

globals().update(GenericFileLoader('equifax_cred_das_file_ingestion_config.yaml').create_dags())  # pragma: no cover
