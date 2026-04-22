# for airflow scanning
from alm_processing.inbound.alm_ctera_to_gcs_data_loader import ALMCteraToGcsCopy

globals().update(ALMCteraToGcsCopy('alm_ctera_to_gcs_file_copy_config.yaml', 'smb_config.yaml', 'alm_shared').generate_dags())  # pragma: no cover
