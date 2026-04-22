# for airflow scanning
from airflow import DAG
from tsys_processing.tsys_pcb_pcmc_stmt_file_loader import TsysPcbPcmcStmtFileLoader

globals().update(TsysPcbPcmcStmtFileLoader('tsys_pcb_pcmc_stmt_ingestion_config.yaml').create_dags())  # pragma: no cover
