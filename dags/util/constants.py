from typing import Final
import os

try:
    from airflow import settings as airflow_settings
    DAGS_FOLDER: Final[str] = airflow_settings.DAGS_FOLDER
except Exception:
    # Fallback for standalone scripts without Airflow installed.
    DAGS_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

CONFIG: Final = 'config'

# zones
CURATED_ZONE_PROJECT_ID: Final = 'curated_zone_project_id'
PROCESSING_ZONE_CONNECTION_ID: Final = 'processing_zone_connection_id'
LANDING_ZONE_PROJECT_ID: Final = 'landing_zone_project_id'
PROCESSING_ZONE_PROJECT_ID: Final = 'processing_zone_project_id'
LANDING_ZONE_CONNECTION_ID: Final = 'landing_zone_connection_id'

# spark
SPARK: Final = 'spark'
SPARK_JOB: Final = 'spark_job'
JAR_FILE_URIS: Final = 'jar_file_uris'
OUTPUT_JAR_URI: Final = 'output_jar_uri'
OUTPUT_URI_PROCESSING: Final = 'output_uri_processing'
OUTPUT_URI_LANDING: Final = 'output_uri_landing'
OUTPUT_MAIN_CLASS: Final = 'output_main_class'
FILE_URIS: Final = 'file_uris'
MAIN_CLASS: Final = 'main_class'
FILTERING_JOB_ARGS: Final = 'filtering_job_args'
PARSING_JOB_ARGS: Final = 'parsing_job_args'
SEGMENT_ARGS: Final = 'segment_args'
OUTPUT_JOB_ARGS: Final = 'output_job_args'
SEGMENT_DEPENDENCY_GROUPS: Final[str] = 'segment_dependency_groups'
FILE_PREFIX: Final = 'file_prefix'
FILE_GROUPS: Final = 'file_groups'
FOLDER_PREFIX: Final = 'folder_prefix'
GROUP_NAME: Final[str] = 'group_name'
SEGMENTS: Final[str] = 'segments'

PROJECT_ID: Final = 'project_id'
REFERENCE: Final = 'reference'
PLACEMENT: Final = 'placement'
LOCATION: Final = 'location'

APPLICATION_CONF: Final = 'application.conf'
CLUSTER_NAME: Final = 'cluster_name'
DATAPROC_CLUSTER_NAME: Final = 'dataproc_cluster_name'
DLAAS_ONETIME_CLUSTER_NAME: Final = 'dlaas-onetime-cluster'
DLAAS_CHUNK_CLUSTER_NAME: Final = 'dlaas-chunk-cluster'
ARGS: Final = 'args'
SPARK_DAG_INFO: Final = 'pcb.data.traceability.dag.info'
PROPERTIES: Final = 'properties'
APPLICATION_CONFIG: Final = 'application_config'

# dataproc batch config
SPARK_BATCH: Final = 'spark_batch'
ENVIRONMENT_CONFIG: Final = 'environment_config'
EXECUTION_CONFIG: Final = 'execution_config'
SUBNETWORK_URI: Final = 'subnetwork_uri'
SERVICE_ACCOUNT: Final = 'service_account'
NETWORK_TAGS: Final = 'network_tags'
RUNTIME_CONFIG: Final = 'runtime_config'
BATCH_ID: Final = 'batch_id'

BATCH: Final = 'batch'
BATCH_SIZE: Final = 'batch_size'
PARENT_TABLE: Final = 'parent_table'

# oracle db name
DBNAME: Final = 'dbname'

# sqlserver config
IS_SQLSERVER: Final = 'is_sqlserver'

# big query configs
BIGQUERY: Final = 'bigquery'
INSERT_QUERY: Final = 'insert_query'
QUERY_FILE: Final = 'query_file'
QUERY_FILE_POST_ETL: Final = 'query_file_post_etl'
USE_LEGACY_SQL: Final = 'use_legacy_sql'
INSERT_FETCH_COLS: Final = 'insert_fetch_cols'
REPLACEMENTS: Final = 'replacements'
BIGQUERY_TASKS: Final = 'bigquery_tasks'
TASKS: Final = 'tasks'
PYTHON_CALLABLE: Final = "python_callable"
MODULE: Final = "module"
METHOD: Final = "method"

# Postgres config
POSTGRES: Final = 'postgres'
POSTGRES_CONN_ID: Final = 'postgres_conn_id'
SELECT_FIELDS: Final = 'select_fields'
UPDATE_FIELDS: Final = 'update_fields'
CONFLICT_FIELDS: Final = 'conflict_fields'
APPEND_FIELDS: Final = 'append_fields'
EXECUTION_METHOD: Final = 'execution_method'
PRE_ETL_SCRIPT: Final = 'pre_etl_script'
POST_ETL_SCRIPT: Final = 'post_etl_script'
PG_QUERY_FILE: Final = 'pg_query_file'
PG_QUERY: Final = 'pg_query'

# Airflow Connection Details
AIRFLOW_CONNECTION_CONFIG: Final = 'airflow_connection_config'
VAULT_PASSWORD_SECRET_PATH: Final = 'vault_password_secret_path'
VAULT_PASSWORD_SECRET_KEY: Final = 'vault_password_secret_key'
CONN_ID: Final = 'conn_id'
CONN_TYPE: Final = 'conn_type'
DESCRIPTION: Final = 'description'
HOST: Final = 'host'
LOGIN: Final = 'login'
PASSWORD: Final = 'password'
SCHEMA: Final = 'schema'
PORT: Final = 'port'

DATASET_ID: Final = 'dataset_id'
TARGET_DATASET_ID: Final = 'target_dataset_id'
TABLE_NAME: Final = 'table_name'
FROM_NAME: Final = 'from_name'
TO_NAME: Final = 'to_name'
TABLE_ID: Final = 'table_id'
TABLES: Final = 'tables'
OUTPUT_COLUMNS: Final = 'output_columns'
LOADING_FREQUENCY: Final = 'loading_frequency'
SCHEMA_QUALIFIED_TABLE_NAME: Final = 'pcb.bigquery.loader.source.schema.qualified.tablename'
PCB_WHERE_CONDITION: Final = 'pcb.bigquery.loader.source.where.condition'
PCB_SELECT_COlUMNS: Final = 'pcb.bigquery.loader.source.select.columns'
PCB_SELECT_COlUMNS_FILE_LOCATION: Final = 'pcb.bigquery.loader.source.select.columns.file'
DB_READ_PARALLELISM: Final = 'pcb.bigquery.loader.source.db.read.parallelism'
PCB_LOWER_BOUND: Final = 'pcb.bigquery.loader.source.range.lowerbound'
PCB_UPPER_BOUND: Final = 'pcb.bigquery.loader.source.range.upperbound'
PCB_PARTITION_COLUMN: Final = 'pcb.bigquery.loader.source.partition.column'
PCB_CUSTOM_QUERY: Final = 'pcb.bigquery.loader.source.query'
DLAAS_JDBC_USER: Final = 'pcb.bigquery.loader.source.jdbc.user'
JOIN_FROM_COLUMN: Final = 'from_column'
JOIN_TO_COLUMN: Final = 'to_column'
JOIN_TYPE: Final = 'join_type'
ADD_FILE_NAME_COLUMN: Final = 'add_file_name_column'
ALIAS: Final = 'alias'
MERGE: Final = 'merge'
MERGE_MATCHED: Final = 'matched'
TRANSFORMATIONS: Final = 'transformations'
MATCHED_SOURCE_COLUMN: Final = 'source_column'
MATCHED_TARGET_COLUMN: Final = 'target_column'
JOIN_COLUMNS: Final = 'join_columns'
ID: Final = 'id'
WRITE_DISPOSITION: Final[str] = 'write_disposition'
EXTERNAL_TABLE_ID: Final = 'ext_table_id'
SOURCE_TABLE_ID: Final = 'source_table_id'
TARGET_TABLE_ID: Final = 'target_table_id'
BACKUP_TABLE_ID: Final = 'backup_table_id'
STAGING_TABLE_ID: Final = 'staging_table_id'
DATA_FILE_LOCATION: Final = 'data_file_location'
COLUMNS: Final = 'columns'
DDL: Final = 'ddl'
JOIN_CLAUSE: Final = 'join_clause'
UPDATE_STATEMENT: Final = 'update_statement'
OVERWRITE_BIGQUERY_TABLE: Final = 'overwrite_bq_table'
JOIN_SPECIFICATION: Final = 'join_spec'
MERGE_SPECIFICATION: Final = 'merge_spec'
PARTITION: Final = 'partition'
PARTITION_FILTER: Final = 'partition_filter'
TIME_PARTITIONING_TYPE: Final = 'time_partitioning_type'
TIME_PARTITIONING_FIELD: Final = 'time_partitioning_field'
CLUSTER_FIELDS: Final = 'cluster_fields'
CLUSTERING: Final = 'clustering'
DESTINATION_DATASET: Final = 'destination_dataset'
DESTINATION_DATASET_ID: Final = 'destination_dataset_id'
DESTINATION_TABLE: Final = 'destination_table'
TRANSFORMATION_SPECIFICATION: Final = 'transformation_spec'
WRITE_TRUNCATE: Final = 'WRITE_TRUNCATE'
WRITE_APPEND: Final = 'WRITE_APPEND'
WRITE_EMPTY: Final = 'WRITE_EMPTY'
ADDITIONAL_COLUMNS: Final = 'additional_columns'
ADD_COLUMNS: Final = 'add_columns'
DROP_COLUMNS: Final = 'drop_columns'
ALTER_COLUMNS = 'alter_columns'
DEDUPLICATION_COLUMNS: Final = 'deduplication_columns'
SKIP_BACKUP_AND_CREATE = 'skip_backup_and_create'
ALTER_TABLE_ADD_COLUMNS = 'alter_table_add_columns'
JOIN: Final = 'join'
DATA_LOAD_TYPE: Final = 'data_load_type'
APPEND_ONLY: Final = 'APPEND_ONLY'
FULL_REFRESH: Final = 'FULL_REFRESH'
FIELD_DELIMITER: Final = 'field_delimiter'
EXECUTION_ID_LOWER_BOUND: Final = 1000000
DLAAS_BQL_ALLOWED_TYPES: Final = ['onetime', 'chunk']
DLAAS_BQL_ONETIME_CONFIG_PATH: Final = f"{DAGS_FOLDER.rsplit('/', 1)[0]}/data/dlaas_bigquery_loader_configs/onetime"
DLAAS_BQL_CHUNK_CONFIG_PATH: Final = f"{DAGS_FOLDER.rsplit('/', 1)[0]}/data/dlaas_bigquery_loader_configs/chunk"
FLAAS_BQL_ALLOWED_WRITE_TYPES = ['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY']
FLAAS_BQL_ALLOWED_SOURCE_FORMATS = ['CSV', 'CSV_DELIMITER', 'CSV_QUOTE', 'CSV_ESCAPE', 'NEWLINE_DELIMITED_JSON', 'JSON',
                                    'JSONL', 'AVRO', 'PARQUET', 'ORC', 'DATASTORE_BACKUP']
TIMESTAMP: Final = 'timestamp'
COLUMN: Final = 'column'
RENAME: Final = 'rename'
DOMAIN_TECHNICAL_DATASET_ID: Final = 'domain_technical'
DOMAIN_DATA_LOGS_DATASET_ID: Final = 'domain_data_logs'
FILTER_ROWS: Final = 'filter_rows'
DUPLICATE_CHECK_COLS: Final = 'duplicate_check_cols'
TARGET_SERVICE_ACCOUNT: Final = 'target_service_account'
TARGET_PROJECT: Final = 'target_project'

# gcs configs
GCS: Final = 'gcs'
BUCKET: Final = 'bucket'
STAGING_FOLDER: Final = 'staging_folder'
STAGING_BUCKET: Final = 'staging_bucket'

# SCOPES
SCOPE_CLOUD_PLATFORM: Final = 'https://www.googleapis.com/auth/cloud-platform'
SCOPE_BIGQUERY: Final = 'https://www.googleapis.com/auth/bigquery'

# airflow
DAG: Final = 'dag'
DAG_ID: Final = 'dag_id'
DAG_OWNER: Final = 'dag_owner'
DAG_RUN: Final = 'dag_run'
TAGS: Final = 'tags'
NONE_FAILED: Final = 'none_failed'
NONE_FAILED_OR_SKIPPED: Final = 'none_failed_or_skipped'
ALWAYS: Final = 'always'
READ_PAUSE_DEPLOY_CONFIG: Final = 'read_pause_deploy_config'
DEFAULT_ARGS: Final = 'default_args'
TAGS: Final = 'tags'
TASK_GROUPS: Final = 'task_groups'
TYPE: Final = 'type'
FIELD: Final = 'field'
CREATE_IF_NEEDED: Final = 'CREATE_IF_NEEDED'
DAGRUN_TIMEOUT: Final = 'dagrun_timeout'
MAX_ACTIVE_RUNS: Final = 'max_active_runs'

START_TASK_ID: Final = 'start'
END_TASK_ID: Final = 'end'

FETCH_SPARK_JOB: Final = 'fetch_spark_job'
CLUSTER_CREATING_TASK_ID: Final = 'create_cluster'
CREATE_AIRFLOW_CONNECTION: Final = 'create_airflow_connection'
TASK_ID: Final = 'task_id'
DAG_TRIGGER_TASK: Final = 'dag_trigger_task'
CREATE_DATE_COLUMN: Final = 'create_date_column'
GCS_TO_GCS: Final = 'gcs_to_gcs'
FETCHING_TASK_ID: Final = 'fetching_task_id'
LOADING_TASK: Final = 'loading_task'
LOADING_TASK_ID: Final = 'loading_task_id'
PADDED_LOADING_TASK_ID: Final = 'padded_loading_task_id'
FUEL_TRANSACTIONS_TASK_ID: Final = 'fuel_transactions_task_id'
UPDATING_TASK_ID: Final = 'updating_task_id'
KAFKA_WRITER_TASK: Final = 'kafka_writer_task'
KAFKA_WRITER_TASK_ID: Final = 'kafka_writer_task_id'
DELETE_GCS_LANDING_OBJ_TASK_ID: Final = 'delete_gcs_landing_obj_task_id'
DELETE_GCS_PROCESSING_OBJ_TASK_ID: Final = 'delete_gcs_processing_obj_task_id'
DELETE_GCS_PROCESSING_QUERY_OBJ_TASK_ID: Final = 'delete_gcs_processing_query_obj_task_id'
DELETE_GCS_LANDING_RESPONSE_OBJ_TASK_ID: Final = 'delete_gcs_landing_response_obj_task_id'
BACKUP_BIGQUERY_TABLE_TASK_ID: Final = 'backup_bigquery_table'
INSERT_BIGQUERY_TABLE_TASK_ID: Final = 'insert_bigquery_table'
QUERY_BIGQUERY_TABLE_TASK_ID: Final = 'query_bigquery_table'
QUERY_BIGQUERY_TABLE_PRE_ETL_TASK_ID: Final = 'query_bigquery_table_pre_etl'
QUERY_BIGQUERY_TABLE_POST_ETL_TASK_ID: Final = 'query_bigquery_table_post_etl'
UPDATE_BIGQUERY_TABLE_TASK_ID: Final = 'update_bigquery_table'
BIGQUERY_TO_POSTGRES_TASK_ID: Final = 'bigquery_to_postgres'
BIGQUERY_TO_POSTGRES_PRE_ETL_TASK_ID: Final = 'bigquery_to_postgres_pre_etl'
BIGQUERY_TO_POSTGRES_POST_ETL_TASK_ID: Final = 'bigquery_to_postgres_post_etl'
POSTGRES_EXECUTE_QUERY_TASK_ID: Final = 'postgres_execute_query'
RESPONSE_FILE_GENERATION_TASK_ID: Final = 'response_file_generation_task_id'
SAVE_JOB_TO_CONTROL_TABLE: Final = 'save_job_to_control_table'
SCHEDULE_INTERVAL: Final = 'schedule_interval'
SUB_DAG_TRIGGER_RULE: Final = 'sub_dag_trigger_rule'
DATAPROC_JOB_SIZE: Final = 'job_size'
DATAPROC_CLUSTER_SIZES: Final = {
    "extra_small": 3,
    "small": 6,
    "medium": 10,
    "large": 16
}

LOADING_DAG_ID: Final = 'loading_dag_id'
PADDED_LOADING_DAG_ID: Final = 'padded_loading_dag_id'
FUEL_TRANSACTIONS_DAG_ID: Final = 'fuel_transactions_dag_id'
FETCHING_DAG_ID: Final = 'fetching_dag_id'
UPDATING_DAG_ID: Final = 'updating_dag_id'

BUCKET_ID: Final = 'bucket_id'
BUCKET_NAME: Final = 'bucket_name'
LANDING_BUCKET: Final = 'landing_bucket'
INBOUND_LANDING_BUCKET: Final = 'inbound_landing_bucket'
OUTBOUND_LANDING_BUCKET: Final = 'outbound_landing_bucket'
PROCESSING_BUCKET: Final = 'processing_bucket'
PROCESSING_BUCKET_EXTRACT: Final = 'processing_bucket_extract'

FILENAME_PREFIX: Final = 'filename_prefix'
OBJ_PREFIX: Final = 'obj_prefix'
LANDING_OBJ_PREFIX: Final = 'landing_obj_prefix'
PROCESSING_OBJ_PREFIX: Final = 'processing_obj_prefix'
PROCESSING_STATUS_PREFIX: Final = 'processing_status_prefix'

WAIT_FOR_COMPLETION: Final = 'wait_for_completion'
POKE_INTERVAL: Final = 'poke_interval'

TORONTO_TIMEZONE_ID: Final = 'America/Toronto'
DEFAULT_SPARK_SETTINGS: Final = {
    "spark.driver.extraJavaOptions": f"-Duser.timezone={TORONTO_TIMEZONE_ID}",
    "spark.executor.extraJavaOptions": f"-Duser.timezone={TORONTO_TIMEZONE_ID}",
    "spark.sql.session.timeZone": f"{TORONTO_TIMEZONE_ID}",
    "spark.sql.legacy.parquet.int96RebaseModeInRead": "CORRECTED",
    "spark.sql.legacy.parquet.int96RebaseModeInWrite": "CORRECTED",
    "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "CORRECTED",
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "CORRECTED"
}

SPARK_DRIVER_EXTRAJAVAOPTIONS: Final = 'spark.driver.extraJavaOptions'
SPARK_EXECUTOR_EXTRAJAVAOPTIONS: Final = 'spark.executor.extraJavaOptions'

DEFAULT_KAFKA_SPARK_DRIVER_EXTRAJAVAOPTIONS: Final = " -Djavax.net.ssl.trustStore='kafka.jks' -Djavax.net.ssl.trustStorePassword='changeit'"
DEFAULT_KAFKA_SPARK_LOG4J_CONFIG: Final = " -Dlog4j.configuration='log4j.properties'"

DEFAULT_KAFKA_SPARK_SETTINGS: Final = {
    JAR_FILE_URIS: ["gs://pcb-{env}-staging-artifacts/jarfiles/file-kafka-connector-2.0.0.jar"],
    FILE_URIS: ["gs://pcb-{env}-staging-artifacts/resources/file-kafka-connector/kafka.jks", "gs://pcb-{env}-staging-artifacts/resources/file-kafka-connector/log4j.properties"],
    MAIN_CLASS: 'com.pcb.batch.kafka.file.FileKafkaConnectorApplication'
}

DEFAULT_KAFKA_SPARK_PROPERTIES: Final = {
    "spark.dynamicAllocation.executorAllocationRatio": "1".encode(),
    "spark.scheduler.mode": "FAIR",
    "spark.executor.memory": "8G"
}

DEFAULT_DLAAS_SPARK_JOB: Final = {
    JAR_FILE_URIS: ["gs://pcb-{env}-staging-artifacts/jarfiles/ods-bigquery-connector-2.0.0.jar"],
    FILE_URIS: ["gs://pcb-{env}-staging-artifacts/resources/ods-bigquery-connector/application-dlaas-default.conf"],
    MAIN_CLASS: 'com.pcb.batch.odsbigqueryconnector.ODSToGCSConnector',
    APPLICATION_CONFIG: "application-dlaas-default.conf"
}
DLAAS_NUM_CHUNKS: Final = 100

# environment variables
GCP_CONFIG: Final = 'gcp_config'
DATAPROC_CONFIG: Final = 'dataproc_config'
DEPLOYMENT_ENVIRONMENT_NAME: Final = 'deployment_environment_name'
DEPLOY_ENV_STORAGE_SUFFIX: Final = 'deploy_env_storage_suffix'
DEPLOY_ENV: Final = 'deploy_env'
NETWORK_TAG: Final = 'network_tag'
ENV_PLACEHOLDER: Final = '{env}'
# pnp refers to prod or nonprod
PNP_ENV_PLACEHOLDER = '{pnp_env}'
CURRENT_DATE_PLACEHOLDER: Final = '{current_date}'

# auth views
TABLE_REF: Final = 'table_ref'
VIEW_REF: Final = 'view_ref'
UPDATE_VIEW: Final = 'update'
VIEW_SQL: Final = 'view_sql'
VIEW_FILE: Final = 'view_file'
SAMPLE_SIZE: Final = 'sample_size'
EXTERNAL_TABLE_SUFFIX: Final = 'DBEXT'
FILTER: Final = 'filter'
BACKUP_ENABLED: Final = 'backup_enabled'
BACK_UP_STR: Final = '_BACK_UP'
MATERIALIZED_VIEW_CONFIG: Final[str] = "materialized_view_config"
MATERIALIZED_VIEW_OPTIONS: Final[str] = "options"

LANDING_ZONE_DATASET: Final = 'Landing_Zone_Dataset'
LANDING_ZONE_TABLE: Final = 'Landing_Zone_Table'
LANDING_ZONE_NAME: Final = 'Landing_Zone_Name'
CURATED_ZONE_DATASET: Final = 'Curated_Zone_Dataset'
CURATED_ZONE_VIEW: Final = 'Curated_Zone_View'
CURATED_ZONE_NAME: Final = 'Curated_Zone_Name'
COMMA_SPACE: Final = ', '
COMMA: Final = ','

# Loyalty (Convergence)
CONVERGENCE: Final = 'Convergence'
LOYALTY: Final = 'Loyalty'
ESSO_MERCHANT_PROCESSING: Final = 'esso_merchant_processing'
LOYALTY_PROCESSING: Final = 'loyalty_processing'
UPDATING_MC_TASK_ID: Final = 'updating_mc_task_id'
UPDATING_MC_DAG_ID: Final = 'updating_mc_dag_id'
UPDATING_CM_TASK_ID: Final = 'updating_cm_task_id'
UPDATING_CM_DAG_ID: Final = 'updating_cm_dag_id'
UPDATING_STORE_LIST_TASK_ID: Final = 'updating_store_list_task'
ENV_SPECIFIC_REPLACEMENTS: Final = 'env_specific_replacements'
STAGING_DATASET: Final = 'staging_dataset'

# BigQuery Table Manager
BIGQUERY_CLIENT: Final = 'bigquery_client'
BACKUP_TABLE_ID: Final = 'backup_table_id'
SOURCE_TABLE_ID: Final = 'source_table_id'
QUERY: Final = 'query'
BQ_QUERY_LOCATION: Final = 'bq_query_location'
BQ_DEFAULT_LOCATION: Final = 'northamerica-northeast1'

# File Kafka Connector Writer
KAFKA_CLUSTER_NAME: Final = 'kafka_cluster_name'
KAFKA_TRIGGER_DAG_ID: Final = 'kafka_trigger_dag_id'
KAFKA_WRITER: Final = 'kafka_writer'
ORACLE_WRITER: Final = 'oracle_writer'
FILE_KAFKA_CONNECTOR_WRITER: Final = 'file_kafka_connector_writer'
KAFKA_CONNECTOR_FILE_PATH: Final = 'pcb.file.kafka.connector.file.path'
FILE_PATH_FROM_PRECEDING_DAG: Final = 'file_path_from_preceding_dag'
FILE_PATH_SUFFIX: Final = 'file_path_suffix'
FILE_SOURCE_BIGQUERY: Final = 'file_source_bigquery'
KAFKA_CONNECTOR_FILTERED_FIELDS: Final = 'pcb.file.kafka.connector.filtered.fields'
KAFKA_CONNECTOR_GROUP_BY_FIELDS: Final = 'pcb.file.kafka.connector.group.by.fields'
KAFKA_CONNECTOR_SOURCE_ARRAY_FIELDS: Final = 'pcb.file.kafka.connector.source.array.fields'
KAFKA_CONNECTOR_KEY_SERIALIZER_CLASS_CONFIG: Final = 'pcb.file.kafka.connector.kafka.key.serializer.class.config'
KAFKA_CONNECTOR_VALUE_SERIALIZER_CLASS_CONFIG: Final = 'pcb.file.kafka.connector.kafka.value.serializer.class.config'
KAFKA_CONNECTOR_SERIALIZER_CLASS: Final = 'io.confluent.kafka.serializers.KafkaAvroSerializer'
KAFKA_CONNECTOR_MESSAGE_SCHEMA_TYPE: Final = 'pcb.file.kafka.connector.message.schema.type'
KAFKA_CONNECTOR_AVRO_MESSAGE: Final = 'AVRO'

# Dag Trigger Config
TRIGGER_CONFIG: Final = 'trigger_config'
NAME: Final = 'name'
FOLDER_NAME: Final = 'folder_name'
FILE_NAME: Final = 'file_name'
ADD_EXECUTION_ID_COLUMN: Final = 'AS EXECUTION_ID'

# ADM
TRAILER_SEGMENT_NAME: Final = 'TRLR'
RECORD_COUNT: Final = 'REC_CNT'
EXECUTION_ID: Final = 'execution_id'
REGISTER_EXECUTION_ID: Final = 'register_execution_id'
CLEAN_UP_PLACEHOLDERS: Final = 'clean_up_placeholders'
FILE_CREATE_DT: Final = 'FILE_CREATE_DT'
RECORD_COUNT_COLUMN: Final = 'record_count_column'

# Account Master
STAGING_FOLDER_NAME: Final = 'tsys_pcb_mc_acctmaster_full_file_load'
LEFT_JOIN: Final = 'LEFT JOIN'
SOURCE_BUCKET = 'source_bucket'

# View Transformations
VIEW: Final = 'view'
CURATED_SPECIFICATION: Final = 'curated_specification'
TABLE_REF_LABEL: Final = 'table_ref'
RANK_PARTITION: Final = 'rank_partition'
RANK_ORDER_BY: Final = 'rank_order_by'
COLUMN_TRANSFORMATION_SUFFIX: Final = '_COLUMN_TF'

LANDING_TABLE: Final = 'landing_table'

# DLAAS CHUNK
NUM_CHUNKS: Final = 100

# CLuster
CLUSTER_SUFFIX: Final = '-c'
MAX_LENGTH: Final = 51

# Vendor Data transfer
DESTINATION_BUCKET = 'destination_bucket'
DESTINATION_PATH = 'destination_path'
ARCHIVE_BUCKET = 'archive_bucket'
ARCHIVE_PATH = 'archive_path'

# Validator config
DAYS_DELTA: Final[str] = 'days_delta'
HOURS_DELTA: Final[str] = 'hours_delta'
YYYY_MM_DD: Final[str] = '%Y-%m-%d'
YYYY_MM_DD_HH: Final[str] = '%Y-%m-%d %H'
HOLIDAY_CHECK_DELTA: Final[str] = 'holiday_check_delta'

# dag activator
THROTTLE: Final[str] = 'throttle'
DAG_IDS: Final[str] = 'dag_ids'
IS_PAUSED: Final[str] = 'is_paused'
CONCURRENCY_LIMIT: Final[str] = 'concurrency_limit'
INTERVAL: Final[str] = 'interval'

# account history
IS_REDEFINE: Final[str] = 'is_redefine'

# tokenized consents config
POKE_INTERVAL_MIN: Final = 'poke_interval_min'
POKE_TIMEOUT_MIN: Final = 'poke_timeout_min'
EXTERNAL_SENSOR_DAG_ID: Final = 'external_sensor_dag_id'
SOURCE_PROJECT_ID: Final = 'source_project_id'
SOURCE_DATASET_ID: Final = 'source_dataset_id'

# ETL PARARMETER
MODULE_FOLDER: Final = 'module_folder'
TRANSFORMATION_CONFIG: Final = 'transformation_config'
TRANSFORMATION: Final = 'transformation'
TASK_NAME: Final = 'task_name'
TASK_TYPE: Final = 'task_type'
TASK_PARAM: Final = 'task_param'
SCRIPT_PATH: Final = 'script_path'
CONTEXT_PARAMETER: Final = 'context_parameter'
OVERRIDE_PARAMETERS: Final = 'override_params'
BQ_DESTINATION_PROJECT: Final = 'destination_project_id'
BQ_DESTINATION_DATASET: Final = 'destination_dataset_id'
BQ_DESTINATION_TABLE_NAME: Final = 'destination_table_name'
FILE: Final = 'file'
PREFIX: Final = 'prefix'
PREFIX_NAME: Final = 'prefix_name'
OUTBOUND_BUCKET: Final = 'outbound_bucket'
OUTBOUND_FOLDER: Final = 'outbound_folder'
FORMAT: Final = 'format'
EXTENSION: Final = 'extension'
FREQUENCY: Final = 'frequency'

# pcmc_statement
LANDING_PAUSE: Final = 'landing_pause'
FILE_TYPE: Final = 'file_type'
FILE_TYPE_PLACEHOLDER: Final = '{file_type}'
UNDERSCORE: Final = '_'
POST_PROCESSING_TABLES: Final = 'post_processing_tables'

# Constants are added for CTERA File transfer
CTERA_SUCCESS = 'SUCCESS'
CTERA_FAIL = 'FAIL'
HOSTNAME: Final = 'hostname'
USERNAME: Final = 'username'
SECRET_PATH: Final = 'secret_path'
SECRET_KEY: Final = 'secret_key'
BLOB_CHUNK_SIZE: Final = 200 * 1024 * 1024

# Vault_Roles
VAULT_ROLE: Final = {"alm": "alm-read-only"}
VAULT_DEFAULT_ROLE: Final = "gcp-read-only"
ALM_VAULT_NAME: Final = 'alm'

# SCMS CARD
COLUMN_UUID_SUFFIX: Final = '_COLUMN_UUID'
FILE_UUID: Final = 'FILE_UUID'
KAFKA_PREPROCESSING_TASK: Final = 'kafka_preprocessing_task'
CURATED_VIEW: Final = 'curated_view'
CURATED_VIEW_TRIGGER_DAG_ID: Final = 'curated_view_trigger_dag_id'
TRIGGER_VIEW_CREATE_DAG: Final = 'trigger_view_create_dag'
START_TIME: Final = 'start_time'
JOB_TYPE: Final = 'job_type'
READ_RECORD_COUNT: Final = 'RED_RECORD_CNT'

# Dag activator
FILTER_SUBSTR: Final = 'filter_substr'

# landing_to_business_inboud_file_transfer
NP_SUFFIX: Final[str] = "-np"
TRANSFERRED: Final = 'transferred'

# tsys parser
SPARK_PARSE_JOB: Final[str] = "spark_parse_job"
SPARK_FILTER_JOB: Final[str] = "spark_filter_job"

PRIMARY_MACHINE_TYPE: Final[str] = "primary-machine-type"
SECONDARY_MACHINE_TYPE: Final[str] = "secondary-machine-type"

# Environments
PROD_ENV: Final[str] = 'prod'
NONPROD_ENV: Final[str] = 'nonprod'
NONPROD_ENV_LIST: Final[list] = ['dev', 'uat', 'nonprod']
DEV_ENV: Final[str] = 'dev'

# Draft 5
BANK_NUMBER: Final[str] = 'bank_number'
ESID: Final[str] = 'esid'
EMAIL: Final[str] = 'email'
SUBJECT: Final[str] = 'subject'
APPROVER: Final[str] = 'approver'
OPERATOR_ID: Final[str] = 'operator_id'
RECIPIENTS: Final[str] = 'recipients'
PURPOSE: Final[str] = 'purpose'
CHARACTER_SET: Final[str] = 'character_set'

DECODING_JOB_ARGS: Final = 'decoding_job_args'
OUTPUTFILE_FILE_PREFIX: Final = 'outputfile_file_prefix'

# DAG pause/unpause configuration
DAG_PAUSE_UNPAUSE_CONFIG: Final = 'dag_pause_unpause_config'
PAUSE_DAGS_AT_START: Final = 'pause_dags_at_start'
UNPAUSE_DAGS_AT_START: Final = 'unpause_dags_at_start'
PAUSE_DAGS_AT_END: Final = 'pause_dags_at_end'
UNPAUSE_DAGS_AT_END: Final = 'unpause_dags_at_end'
PAUSE_DAGS_AT_START_TASK_ID: Final = 'pause_dags_at_start_task'
UNPAUSE_DAGS_AT_START_TASK_ID: Final = 'unpause_dags_at_start_task'
