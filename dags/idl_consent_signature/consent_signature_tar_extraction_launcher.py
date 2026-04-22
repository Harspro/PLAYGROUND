from airflow import settings
from idl_consent_signature.consent_signature_gcs_tar_extraction import GCSTarExtraction

# Launch the DAGs based on the YAML configuration
globals().update(GCSTarExtraction('consent_signature_tar_extraction_config.yaml',
                                  f'{settings.DAGS_FOLDER}/idl_consent_signature')
                 .create_dags())  # pragma: no cover
