from airflow import settings
from python_task_executor.python_task_executor_base import PythonTaskExecutor

globals().update(
    PythonTaskExecutor(
        "python_task_executor_config.yaml",
        f"{settings.DAGS_FOLDER}/config/python_task_executor_configs"
    ).create_dags()
)
