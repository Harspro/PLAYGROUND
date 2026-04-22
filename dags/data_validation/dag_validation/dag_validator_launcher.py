# For airflow scanning
from airflow import DAG

from data_validation.dag_validation.dag_validator import DagValidator

globals().update(DagValidator('dag_validation_config.yaml').create_dags())  # pragma: no cover
