# for airflow scanning
from pcmc_estatement_processing.pcb_dgtl_mc_estmtavail_file_generator import EstmtAvailGenerator
from pcmc_estatement_processing.utils import constants

globals().update(EstmtAvailGenerator(module_name='pcmc_estatement_processing',
                                     config_path='config',
                                     config_filename='pcb_dgtl_mc_estmtavail_file_generator_config.yaml',
                                     dag_default_args=constants.DAG_DEFAULT_ARGS).generate_dags())  # pragma: no cover
