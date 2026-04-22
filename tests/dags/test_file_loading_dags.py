import unittest

import airflow.models


def test_file_loading_dag_configs():
    file_loading_dag_ids = [
        'tsys_pcb_mc_acctmaster_monthly_full_file_loading',
        'tsys_pcb_mc_adm_aceltr_file_loading',
        'tsys_pcb_mc_adm_appmemo_file_loading',
        'tsys_pcb_mc_adm_applog_file_loading',
        'tsys_pcb_mc_adm_disp_file_loading',
        'tsys_pcb_mc_adm_appelement_file_loading',
        'tsys_pcb_mc_adm_operatorlog_file_loading'
    ]

    dag_bag = airflow.models.DagBag(include_examples=False)
    for dag_id in dag_bag.dags:
        if dag_id in file_loading_dag_ids:
            dag = dag_bag.dags[dag_id]
            assert not dag.catchup, f'catchup of {dag_id} is not False'
            assert dag.is_paused_upon_creation, f'is_paused_upon_creation of {dag.dag_id} is not True'
            assert dag.max_active_runs == 1, f'max_active_runs of {dag.dag_id} is not 1'


if __name__ == '__main__':
    unittest.main()
