# for airflow scanning
from airflow import DAG

from tsys_processing.ts2_cardguard_fraudtxn_file_loader import FraudTxnFileLoader

globals().update(FraudTxnFileLoader('ts2_cardguard_fraudtxn_ingestion_config.yaml').create_dags())  # pragma: no cover
