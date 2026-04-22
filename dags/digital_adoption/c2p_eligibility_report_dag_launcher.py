# for airflow scanning
from etl_framework.default_etl_data_loader import DefaultETLDataLoader

DAG_DEFAULT_ARGS = {
    "owner": "team-digital-adoption-alerts",
    "capability": "account-management",
    "sub_capability": "account-management",
    "severity": "P3",
    "description": "This DAG produces a consolidated list of all existing customers by eligibility/ineligibility reason for the Click To Pay enrolment",
    "customer_impact": "None",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "max_active_runs": 1,
    "retries": 3,
    "tags": ["team-digital-adoption-alerts"],
    "retry_delay": 300,
    "params": {
        "account_open_start_date": None,
        "account_open_end_date": None,
        "report_generation_flag": None
    }
}

globals().update(DefaultETLDataLoader(module_name='digital_adoption',
                                      config_path='config',
                                      config_filename='c2p_eligibility_report_base_config.yaml',
                                      dag_default_args=DAG_DEFAULT_ARGS).generate_dags())  # pragma: no cover
