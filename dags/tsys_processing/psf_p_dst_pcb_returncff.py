# for airflow scanning
from airflow import DAG
from tsys_processing.generic_file_loader import GenericFileLoader

globals().update(GenericFileLoader('psf_p_dst_pcb_returncff.yaml').create_dags())  # pragma: no cover
