# for airflow scanning
from bq_to_oracle_loader.bq_to_oracle_base import BqToOracleProcessor
from bq_to_oracle_loader.utils import constants as constants

globals().update(BqToOracleProcessor(module_name='bq_to_oracle_loader',
                                     config_path='config',
                                     config_filename='bq_to_oracle_config.yaml',
                                     dag_default_args=constants.INITIAL_DEFAULT_ARGS).generate_dags())  # pragma: no cover
