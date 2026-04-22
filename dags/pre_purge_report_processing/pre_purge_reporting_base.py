import logging

from etl_framework.etl_dag_base import ETLDagBase

logger = logging.getLogger(__name__)


class PrePurgeReportingProcessor(ETLDagBase):
    def __init__(self, module_name, config_path, config_filename, dag_default_args):
        super().__init__(module_name, config_path, config_filename, dag_default_args)

    def preprocessing_job(self, config: dict, upstream_task: list):
        pass

    def postprocessing_job(self, config: dict, upstream_task: list):
        pass
