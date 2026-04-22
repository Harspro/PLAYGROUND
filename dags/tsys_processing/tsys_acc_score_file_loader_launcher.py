# for airflow scanning
from airflow import DAG
from tsys_processing.tsys_acc_scores_file_loader import TsysAccScoresFullFileLoader

globals().update(TsysAccScoresFullFileLoader('tsys_acc_scores_file_ingestion_config.yaml').create_dags())  # pragma: no cover
