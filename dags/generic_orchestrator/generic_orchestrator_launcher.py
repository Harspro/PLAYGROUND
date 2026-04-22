from airflow import settings
from generic_orchestrator.generic_orchestrator_base import GenericOrchestrator

# Separating out the launching part so that generic orchestrator can be reused.
globals().update(
    GenericOrchestrator(
        "generic_orchestrator_config.yaml",
        f"{settings.DAGS_FOLDER}/config/generic_orchestrator_configs"
    ).create_dags()
)
