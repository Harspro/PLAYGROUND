# for airflow scanning
from payments_processing.payments_output_file_extract import OutputFileCreation

globals().update(OutputFileCreation('payments_output_file_creation.yaml').create_dags())  # pragma: no cover
