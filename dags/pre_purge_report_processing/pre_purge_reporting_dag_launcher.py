# for airflow scanning
from pre_purge_report_processing.pre_purge_reporting_base import PrePurgeReportingProcessor
from pre_purge_report_processing.utils import constants as pre_purge_report_constants

globals().update(PrePurgeReportingProcessor(module_name='pre_purge_report_processing',
                                            config_path='config',
                                            config_filename='pre_purge_reporting_base_config.yaml',
                                            dag_default_args=pre_purge_report_constants.DAG_DEFAULT_ARGS).generate_dags())  # pragma: no cover
