# for airflow scanning
from airflow import DAG, settings

from tsys_processing.generic_file_loader import GenericFileLoader

globals().update(GenericFileLoader('tsys_pcb_mc_autopurge_purged_ingestion_config.yaml').create_dags())  # pragma: no cover
