# for airflow scanning
from bigquery_loader.bigquery_bulkloader import BigQueryBulkLoader
from bigquery_loader.bigquery_loader_base import LoadingFrequency

globals().update(BigQueryBulkLoader('sas_aml_ods_idl_bulk_config.yaml', LoadingFrequency.Onetime).create_dags())  # pragma: no cover
