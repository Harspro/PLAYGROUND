from typing import Final

from airflow import settings

DOC_GENERATION_CONFIG_PATH: Final[str] = f'{settings.DAGS_FOLDER}/doc_generation_processing/config/document_generation_config.yaml'
MAIL_PRINT_EXPORT_JSON_SQL_PATH: Final[str] = f'{settings.DAGS_FOLDER}/doc_generation_processing/sql/mail_print_export_json_query.sql'
MAIL_PRINT_STATUS_INSERT_PDF_STATUS_SQL_PATH: Final[str] = f'{settings.DAGS_FOLDER}/doc_generation_processing/sql/mail_print_status_insert_pdf_status_query.sql'
MAIL_PRINT_STATUS_INSERT_AFP_SUCCESS_SQL_PATH: Final[str] = f'{settings.DAGS_FOLDER}/doc_generation_processing/sql/mail_print_status_insert_afp_success_query.sql'
MAIL_PRINT_STATUS_INSERT_AFP_FAILURE_SQL_PATH: Final[str] = f'{settings.DAGS_FOLDER}/doc_generation_processing/sql/mail_print_status_insert_afp_failure_query.sql'
MAIL_PRINT_STATUS_COUNT_SQL_PATH: Final[str] = f'{settings.DAGS_FOLDER}/doc_generation_processing/sql/mail_print_status_count_query.sql'
MAIL_PRINT_STATUS_PARQUET_SQL_PATH: Final[str] = f'{settings.DAGS_FOLDER}/doc_generation_processing/sql/mail_print_status_parquet_query.sql'

EST_ZONE: Final[str] = 'America/Toronto'
BUCKET_NAME_JSON_KEY: Final[str] = 'bucketName'
UNIQUE_FOLDER_PATH_JSON_KEY: Final[str] = 'uniqueFolderPath'
PATH_JSON_KEY: Final[str] = 'path'
PROCESSING_TIME_MS_JSON_KEY: Final[str] = 'processingTimeMs'
ERROR_MESSAGE_JSON_KEY: Final[str] = 'errorMessage'

AFP_PROCESSING_WORKLOAD_TASK_ID: Final[str] = 'afp_processing.generate_and_upload_afp'
AFP_PROCESSING_VALID_ENV_VAR_TASK_ID: Final[str] = 'afp_processing.get_valid_env_variables'

SUCCESS_TASK_INSTANCE_STATE: Final[str] = 'success'

# mail print status - aligned with Kafka Avro Schema for `dp-core-mail-print-status`
PDF_GENERATED: Final[str] = 'PDF_GENERATED'
PDF_GENERATION_FAILED: Final[str] = 'PDF_GENERATION_FAILED'
