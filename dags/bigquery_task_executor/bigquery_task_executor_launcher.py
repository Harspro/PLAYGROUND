from airflow import settings
from bigquery_task_executor.bigquery_task_executor_base import BigQueryTaskExecutor

# Separating out the launching part so that bigquery task executor can be reused.
globals().update(
    BigQueryTaskExecutor(
        "bigquery_task_executor_config.yaml",
        f"{settings.DAGS_FOLDER}/config/bigquery_task_executor_configs"
    ).create_dags()
)
