# for airflow scanning

from datetime import timedelta
from etl_framework.default_etl_data_loader import DefaultETLDataLoader

DAG_DEFAULT_ARGS = {
    "owner": "team-digital-adoption-alerts",
    'capability': 'account-management',
    'severity': 'P3',
    'sub_capability': 'account-management',
    'business_impact': 'Schema migration for BATCH_AUTHORIZATION_LOG parent table and child tables',
    'customer_impact': 'None',
    "depends_on_past": False,
    "wait_for_downstream": False,
    "max_active_runs": 1,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "tags": ["account-management", "schema-migration"],
    "region": "northamerica-northeast1"
}

globals().update(DefaultETLDataLoader(module_name='tsys_processing',
                                      config_path='dags_config',
                                      config_filename='migrate_batch_authorization_log_schema_config.yaml',
                                      dag_default_args=DAG_DEFAULT_ARGS).generate_dags())  # pragma: no cover
