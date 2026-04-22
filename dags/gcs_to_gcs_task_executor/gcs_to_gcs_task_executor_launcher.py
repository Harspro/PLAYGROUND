from airflow import settings
from gcs_to_gcs_task_executor.gcs_to_gcs_task_executor_base import GcsToGcsTaskExecutor

# Separating out the launching part so that gcs to gcs task executor can be reused.
globals().update(
    GcsToGcsTaskExecutor(
        "gcs_to_gcs_task_executor_config.yaml",
        f"{settings.DAGS_FOLDER}/config/gcs_to_gcs_task_executor_configs"
    ).create_dags()
)
