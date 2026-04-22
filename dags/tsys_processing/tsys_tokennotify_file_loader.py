# for airflow scanning
from airflow import DAG, settings

from tsys_processing.generic_file_loader import GenericFileLoader

globals().update(GenericFileLoader('tsys_tokennotify_file_loader.yaml').create_dags())  # pragma: no cover
