# for airflow scanning
from alm_processing.outbound.alm_shared_data_loader import AlmSharedDataLoader

globals().update(AlmSharedDataLoader('shared_inbound_dag_config.yaml').generate_dags())  # pragma: no cover
