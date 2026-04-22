from alm_processing.outbound.alm_dag_base import AlmDagBase
from alm_processing.utils import alm_util as util
from alm_processing.utils import bq_util as bqutil
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from util.constants import BIGQUERY
from alm_processing.utils import constants


class AlmBQDataLoader(AlmDagBase):
    def __init__(self, config_filename: str):
        super().__init__(config_filename)

    #   NOTE:  This function has to be implemented as source of data
    def get_data(self, config: dict):
        return PythonOperator(
            task_id="run_bq_query",
            python_callable=bqutil.run_query,
            op_args=[config[BIGQUERY][constants.SQL_FILE_PATH],
                     config[BIGQUERY][constants.BQ_DESTINATION_DATASET],
                     config[BIGQUERY][constants.BQ_TABLE_NAME]],
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

    #   TODO: PLACEHOLDER FOR  STEPS NEEDED FOR PREPROCESSING
    def preprocessing_job(self, dag_config: dict):
        pass

    #   TODO: PLACEHOLDER FOR  STEPS NEEDED FOR POSTPROCESSING
    def postprocessing_job(self, dag_config: dict):
        pass

    #   TODO:  NEED TO ADD DATAPROC FUNCTIONALITY WHEN NEEDED
    def create_dataproc_job(self, dag_config: dict):
        pass

    #   TODO:  NEED TO ADD PARSE FILE FUNCTIONALITY FOR EXTERNAL VENDOR FILES
    def process_file(self, dag_config: dict):
        pass
