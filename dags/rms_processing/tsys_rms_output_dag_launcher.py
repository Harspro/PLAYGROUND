# for airflow scanning
from rms_processing.tsys_rms_output_file_extract import OutputFileCreation

globals().update(OutputFileCreation('tsys_rms_output_file_creation.yaml').create_dags())  # pragma: no cover
