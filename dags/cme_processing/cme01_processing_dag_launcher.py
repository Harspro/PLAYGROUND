# for airflow scanning
from cme_processing.cme_processing_base import CmeBaseProcessor
from cme_processing.utils import constants as cme_constants

globals().update(CmeBaseProcessor(module_name='cme_processing',
                                  config_path='config',
                                  config_filename='cme_processing_base_config.yaml',
                                  dag_default_args=cme_constants.DAG_DEFAULT_ARGS).generate_dags())  # pragma: no cover
