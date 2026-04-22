from aml_processing.aml_to_cust_idv_info.tasks.aml_to_cust_idv_insert_into_cust_idv_task import AmlToCustIdvInsertIntoCustIdvTask
from aml_processing.aml_to_cust_idv_info.tasks.aml_to_cust_idv_update_ctl_complete_task import AmlToCustIdvUpdateCtlCompleteTask
from aml_processing.aml_to_cust_idv_info.tasks.aml_to_cust_idv_ctl_task import AmlToCustIdvCtlTask
from aml_processing.aml_to_cust_idv_info.tasks.aml_to_cust_idv_check_duplicates_task import AmlToCustIdvCheckDuplicatesTask

# Parent Class for the list of all tasks to be executed


class AmlToCustIdvInfo:
    @staticmethod
    def get_dag_run_id(context) -> str:
        dag_run_id = context["run_id"]
        return dag_run_id

    def task_check_for_duplicates_in_source_tables(self, **context):
        dag_run_id = AmlToCustIdvInfo.get_dag_run_id(context)
        task = AmlToCustIdvCheckDuplicatesTask(dag_run_id)
        task.execute()

    def task_create_ctl_table(self, **context):
        dag_run_id = AmlToCustIdvInfo.get_dag_run_id(context)
        task = AmlToCustIdvCtlTask(dag_run_id)
        task.execute()

    def task_insert_into_cust_idv_info_table(self, **context):
        dag_run_id = AmlToCustIdvInfo.get_dag_run_id(context)
        task = AmlToCustIdvInsertIntoCustIdvTask(dag_run_id)
        task.execute()

    def task_update_ctl_table(self, **context):
        dag_run_id = AmlToCustIdvInfo.get_dag_run_id(context)
        task = AmlToCustIdvUpdateCtlCompleteTask(dag_run_id)
        task.execute()
