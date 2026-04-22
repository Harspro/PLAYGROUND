# for airflow scanning
from rms_processing.tsys_rms_file_loader import TsysRmsBatchLoader

globals().update(TsysRmsBatchLoader('tsys_rms_loader.yaml').create_dags())  # pragma: no cover
