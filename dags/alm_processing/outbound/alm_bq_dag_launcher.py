# for airflow scanning
from alm_processing.outbound.alm_bq_data_loader import AlmBQDataLoader

globals().update(AlmBQDataLoader('bq_dag_config.yaml').generate_dags())  # pragma: no cover
