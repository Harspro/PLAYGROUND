# for airflow scanning
from airflow import DAG
from tsys_processing.tsys_adm_file_loader import TsysADMFileLoader

globals().update(TsysADMFileLoader('tsys_adm_file_ingestion_config.yaml').create_dags())  # pragma: no cover
