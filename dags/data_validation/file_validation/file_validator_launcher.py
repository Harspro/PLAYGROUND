# For airflow scanning
from airflow import DAG

from data_validation.file_validation.file_validator import FileValidator

globals().update(FileValidator('file_validation_config.yaml').create_dags())  # pragma: no cover
