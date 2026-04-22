from airflow import settings
from dataproc_task_executor.dataproc_task_executor_base import DataprocTaskExecutor

globals().update(
    DataprocTaskExecutor(
        "dataproc_task_executor_config.yaml",
        f"{settings.DAGS_FOLDER}/config/dataproc_task_executor_configs"
    ).create_dags()
)
