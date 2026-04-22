# for airflow scanning
from alm_processing.outbound.alm_vendor_data_loader import AlmVendorDataLoader

globals().update(AlmVendorDataLoader('vendor_inbound_dag_config.yaml').generate_dags())  # pragma: no cover
