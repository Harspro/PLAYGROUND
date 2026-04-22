# for airflow scanning

from etl_framework.default_etl_data_loader import DefaultETLDataLoader
from tokenized_consent_capture.utils import constants as cnst_constants

globals().update(DefaultETLDataLoader(module_name='tokenized_consent_capture',
                                      config_path='dags_config',
                                      config_filename='cnst_signature_etl_config.yaml',
                                      dag_default_args=cnst_constants.DAG_DEFAULT_ARGS).generate_dags())  # pragma: no cover
