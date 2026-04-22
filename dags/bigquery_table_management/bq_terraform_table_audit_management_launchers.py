# for airflow scanning
from bigquery_table_management.bq_terraform_table_audit_management_dag import (
    BigQueryTableManager
)

globals().update(
    BigQueryTableManager(
        config_filename="bigquery_terraform_table_audit_config.yaml"
    ).create_dags()
)
