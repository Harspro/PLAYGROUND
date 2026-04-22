# for airflow scanning
from airflow import DAG

from tsys_processing.generic_file_loader import GenericFileLoader

globals().update(GenericFileLoader('tsys_acc_master_file_monthly_ingestion_config_v2.yaml').create_dags())  # pragma: no cover
