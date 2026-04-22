import abc
from copy import deepcopy
from datetime import timedelta
from typing import Final

from airflow import DAG
from dag_factory.environment_config import EnvironmentConfig

STANDARD_DEFAULT_ARGS: Final = {
    "owner": "team-centaurs",
    'capability': 'TBD',
    'severity': 'P3',
    'sub_capability': 'TBD',
    'business_impact': 'TBD',
    'customer_impact': 'TBD',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=10)
}


class BaseDagBuilder(abc.ABC):
    def __init__(self, environment_config: EnvironmentConfig):
        self.environment_config = environment_config

    def prepare_default_args(self, config_args: dict = None) -> dict:
        """
        Centralized method to prepare default_args following the common pattern:
        1. Start with STANDARD_DEFAULT_ARGS
        2. Update with config-provided args
        """
        default_args = deepcopy(STANDARD_DEFAULT_ARGS)

        if config_args:
            default_args.update(config_args)

        return default_args

    @abc.abstractmethod
    def build(self, dag_id: str, config: dict) -> DAG:
        pass
