# Parent Class for all the tasks that will be executed for data load in the CUST_IDV_INFO table
class AmlToCustIdvTask:
    def __init__(self, dag_run_id: str):
        self.dag_run_id = dag_run_id

    # Method to be implemented by individual task files(child classes inheriting from this class)
    def execute(self):
        pass
