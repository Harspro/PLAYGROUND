# For airflow scanning
from airflow import DAG
from tsys_processing.cif.constants import CIF_CONFIG_FILE_PATH, CIF_CONFIG_FILE_NAME
from tsys_processing.cif.tsys_cif_file_loader import TsysCIFFileLoader

globals().update(TsysCIFFileLoader(CIF_CONFIG_FILE_NAME, CIF_CONFIG_FILE_PATH).create_dags())
