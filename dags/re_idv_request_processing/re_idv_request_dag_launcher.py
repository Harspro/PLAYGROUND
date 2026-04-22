# for airflow scanning
from re_idv_request_processing.re_idv_request_base import ReIdvProcessor
from re_idv_request_processing.utils import constants as re_idv_constants

globals().update(ReIdvProcessor(module_name='re_idv_request_processing',
                                            config_path='config',
                                            config_filename='re_idv_request_base_config.yaml',
                                            dag_default_args=re_idv_constants.DAG_DEFAULT_ARGS).generate_dags())  # pragma: no cover
