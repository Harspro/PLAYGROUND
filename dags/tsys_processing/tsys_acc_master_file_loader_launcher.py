# for airflow scanning
from airflow import DAG
from tsys_processing.tsys_acc_master_file_loader import TsysAccMasterFullFileLoader

globals().update(TsysAccMasterFullFileLoader('tsys_acc_master_file_monthly_ingestion_config.yaml').create_dags())  # pragma: no cover
