from aml_processing.adm_idv.tasks.adm_to_aml_create_ctl_task import AdmToAmlIdvCreateCtlTask
from aml_processing.adm_idv.tasks.adm_to_aml_idv_create_init_stg_task import AdmToAmlIdvCreateInitStgTablesTask
from aml_processing.adm_idv.tasks.adm_to_aml_idv_update_ctl_complete_task import AdmToAmlIdvUpdateCtlCompleteTask
from aml_processing.adm_idv.tasks.adm_to_aml_idv_ftf_task import AdmToAmlIdvFTFTask
from aml_processing.adm_idv.tasks.adm_to_aml_idv_nftf_pav_task import AdmToAmlIdvNFTFTPavTask
from aml_processing.adm_idv.tasks.adm_to_aml_idv_nftf_single_source_task import AdmToAmlIdvNFTFTSingleSourceTask
from aml_processing.adm_idv.tasks.adm_to_aml_idv_nftf_dual_source_task import AdmToAmlIdvNFTFTDualSourceTask
from aml_processing.adm_idv.tasks.adm_to_aml_idv_digital_task import AdmToAmlIdvDigitalTask
from aml_processing.adm_idv.tasks.adm_to_aml_idv_update_digital_ctl_task import AdmToAmlIdvUpdateDigitalCtlTask
from aml_processing.adm_idv.tasks.adm_to_aml_idv_insertion_task import AdmToAmlIdvInsertionTask
from airflow.exceptions import AirflowFailException
from aml_processing.aml_utils import AMLUtils


class ADMIdvToAml:

    @staticmethod
    def get_dag_run_id(context) -> str:
        dag_run_id = context["run_id"]
        return dag_run_id

    def task_create_ctl_table(self, **context):
        dag_run_id = ADMIdvToAml.get_dag_run_id(context)
        cutoff_date_txt = context['params']['idl_cutoff_date']
        cutoff_date = AMLUtils.parse_date(cutoff_date_txt)
        if cutoff_date is None:
            raise AirflowFailException("Cut off date is required")

        task = AdmToAmlIdvCreateCtlTask(dag_run_id, cutoff_date)
        task.execute()

    def task_create_init_stg_tables(self, **context):
        dag_run_id = ADMIdvToAml.get_dag_run_id(context)
        task = AdmToAmlIdvCreateInitStgTablesTask(dag_run_id)
        task.execute()

    def task_idv_ftf(self, **context):
        dag_run_id = ADMIdvToAml.get_dag_run_id(context)
        task = AdmToAmlIdvFTFTask(dag_run_id)
        task.execute()

    def task_idv_nftf_pav(self, **context):
        dag_run_id = ADMIdvToAml.get_dag_run_id(context)
        task = AdmToAmlIdvNFTFTPavTask(dag_run_id)
        task.execute()

    def task_idv_nftf_single_source(self, **context):
        dag_run_id = ADMIdvToAml.get_dag_run_id(context)
        task = AdmToAmlIdvNFTFTSingleSourceTask(dag_run_id)
        task.execute()

    def task_idv_nftf_dual_source(self, **context):
        dag_run_id = ADMIdvToAml.get_dag_run_id(context)
        task = AdmToAmlIdvNFTFTDualSourceTask(dag_run_id)
        task.execute()

    def task_idv_digital_source(self, **context):
        dag_run_id = ADMIdvToAml.get_dag_run_id(context)
        task = AdmToAmlIdvDigitalTask(dag_run_id)
        task.execute()

    def task_update_ctl_completed(self, **context):
        dag_run_id = ADMIdvToAml.get_dag_run_id(context)
        task = AdmToAmlIdvUpdateCtlCompleteTask(dag_run_id)
        task.execute()

    def task_update_digital_ctl_process_time(self, **context):
        dag_run_id = ADMIdvToAml.get_dag_run_id(context)
        task = AdmToAmlIdvUpdateDigitalCtlTask(dag_run_id)
        task.execute()

    def task_insert_from_all_methods(self, **context):
        dag_run_id = ADMIdvToAml.get_dag_run_id(context)
        task = AdmToAmlIdvInsertionTask(dag_run_id)
        task.execute()
