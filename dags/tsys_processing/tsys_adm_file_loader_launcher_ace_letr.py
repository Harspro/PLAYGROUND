# for airflow scanning
from airflow import DAG
from tsys_processing.tsys_adm_file_loader_ace_letr import TsysAceLetrLoader

globals().update(TsysAceLetrLoader('tsys_adm_file_ingestion_ace_letr_config.yaml').create_dags())
