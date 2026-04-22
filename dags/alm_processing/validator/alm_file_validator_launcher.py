# for airflow scanning
from alm_processing.validator.alm_file_validator import AlmFileValidator

globals().update(AlmFileValidator('alm_file_validation_config.yaml').generate_dags())  # pragma: no cover
