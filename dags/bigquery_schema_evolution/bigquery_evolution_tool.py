from datetime import datetime, timedelta

import logging
import pendulum
from typing import Final
import util.constants as consts
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from util.bq_utils import (
    apply_column_transformation,
    apply_timestamp_transformation,
    create_backup_table
)
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env
)
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)

INITIAL_DEFAULT_ARGS: Final = {
    'owner': 'team-centaurs',
    'capability': 'TBD',
    'severity': 'P3',
    'sub_capability': 'TBD',
    'business_impact': 'TBD',
    'customer_impact': 'TBD',
    'depends_on_past': False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True
}


class BigQuerySchemaEvolutionTool:
    def __init__(self, config_filename: str, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        self.dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        self.config_dir = config_dir
        self.job_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)

        logger.info(f'job_config={self.job_config}')

        self.local_tz = pendulum.timezone('America/Toronto')

    def build_evolution_config(self, dag_config: dict):
        project_id = dag_config.get(consts.PROJECT_ID)
        dataset_id = dag_config.get(consts.DATASET_ID)
        target_dataset_id = dag_config.get(consts.TARGET_DATASET_ID, dataset_id)
        table_name = dag_config.get(consts.TABLE_NAME)

        processing_zone_project_id = self.gcp_config[consts.PROCESSING_ZONE_PROJECT_ID]
        backup_table_ref = f"{processing_zone_project_id}.{dataset_id}.{table_name}_BACK_UP"
        table_ref = f"{project_id}.{dataset_id}.{table_name}"
        target_table_ref = f"{project_id}.{target_dataset_id}.{table_name}"

        partition_field = dag_config.get('partition_field')
        clustering_fields = dag_config.get('clustering_fields')
        partition_filter = dag_config.get(consts.PARTITION_FILTER)

        source_table = bigquery.Client().get_table(table_ref)

        if partition_filter is None:
            partition_filter = source_table.require_partition_filter

        if partition_field is None:
            if source_table.time_partitioning is not None:
                partition_field = source_table.time_partitioning.field
            elif source_table.range_partitioning is not None:
                field = source_table.range_partitioning.field
                partition_range = source_table.range_partitioning.range_
                partition_field = f'RANGE_BUCKET({field}, GENERATE_ARRAY({partition_range.start}, {partition_range.end}, {partition_range.interval}))'

        if clustering_fields is None \
                and (source_table.clustering_fields is not None):
            clustering_fields = source_table.clustering_fields

        logger.info(f'partition: {partition_field}, cluster: {clustering_fields}')
        partition_str = f"PARTITION BY {partition_field}" if partition_field else ''
        clustering_str = "CLUSTER BY " + ", ".join(clustering_fields) if clustering_fields else ''
        logger.info(f'partition: {partition_str}, cluster: {clustering_str}')

        transformations = dag_config.get(consts.TRANSFORMATIONS) or []
        skip_backup_and_create = dag_config.get(consts.SKIP_BACKUP_AND_CREATE) or False
        return {
            consts.SOURCE_TABLE_ID: table_ref,
            consts.TARGET_TABLE_ID: target_table_ref,
            consts.BACKUP_TABLE_ID: backup_table_ref,
            consts.PARTITION: partition_str,
            consts.CLUSTERING: clustering_str,
            consts.PARTITION_FILTER: partition_filter,
            consts.TRANSFORMATIONS: transformations,
            consts.SKIP_BACKUP_AND_CREATE: skip_backup_and_create
        }

    def apply_transformations(self, evolution_config: dict):
        bigquery_client = bigquery.Client()
        backup_table_id = evolution_config.get(consts.BACKUP_TABLE_ID)

        table = bigquery_client.get_table(backup_table_id)
        table_schema = table.schema
        columns = consts.COMMA_SPACE.join([field.name for field in table_schema])

        backup_table = {
            consts.ID: backup_table_id,
            consts.COLUMNS: columns
        }

        if not evolution_config.get(consts.TRANSFORMATIONS):
            return backup_table

        transformation_result = backup_table

        for transformation in evolution_config.get(consts.TRANSFORMATIONS):
            if transformation == consts.TIMESTAMP:
                transformation_result = apply_timestamp_transformation(bigquery_client, transformation_result)
            else:
                if consts.COLUMN in transformation:
                    logger.info(f'transformation_config={transformation}')
                    transformation_result = apply_column_transformation(bigquery_client, transformation_result,
                                                                        transformation.get(consts.COLUMN).get(consts.ADD_COLUMNS),
                                                                        transformation.get(consts.COLUMN).get(consts.DROP_COLUMNS))

        return transformation_result

    def build_back_up_job(self, evolution_config: dict):
        if evolution_config.get(consts.SKIP_BACKUP_AND_CREATE):
            logger.info('Skipping backup and create')
            return
        backup_table_ref = evolution_config.get(consts.BACKUP_TABLE_ID)
        source_table_ref = evolution_config.get(consts.SOURCE_TABLE_ID)
        logger.info(f'Creating backup table {backup_table_ref} using {source_table_ref}')
        create_backup_table(backup_table_ref, source_table_ref)
        logger.info(f'Finished creating backup table {backup_table_ref} using {source_table_ref}')

    def build_new_table_creation_job(self, evolution_config: dict):
        if evolution_config.get(consts.SKIP_BACKUP_AND_CREATE):
            logger.info('Skipping backup and create')
            return
        transformed_table = self.apply_transformations(evolution_config)

        source_table_id = evolution_config.get(consts.SOURCE_TABLE_ID)
        target_table_id = evolution_config.get(consts.TARGET_TABLE_ID)
        transformed_table_id = transformed_table.get(consts.ID)
        transformed_table_columns = transformed_table.get(consts.COLUMNS)
        options = ''
        if evolution_config.get(consts.PARTITION):
            options = f'OPTIONS (require_partition_filter = {bool(evolution_config.get(consts.PARTITION_FILTER))})'
        backup_table_ddl = f"""
                    DROP TABLE `{source_table_id}`;

                    CREATE OR REPLACE TABLE
                        `{target_table_id}`
                    {evolution_config.get(consts.PARTITION)}
                    {evolution_config.get(consts.CLUSTERING)}
                    {options}
                    AS
                        SELECT {transformed_table_columns}
                        FROM {transformed_table_id};
            """

        logger.info(backup_table_ddl)
        bigquery.Client().query(backup_table_ddl).result()
        logger.info(f'Finished creating {source_table_id}')

    def alter_columns(self, evolution_config: dict):
        bigquery_client = bigquery.Client()
        target_table_id = evolution_config.get(consts.TARGET_TABLE_ID)
        for transformation in evolution_config.get(consts.TRANSFORMATIONS):
            if consts.COLUMN in transformation:
                alter_columns = transformation.get(consts.COLUMN).get(consts.ALTER_COLUMNS)
                if alter_columns is not None:
                    for alt_col in alter_columns:
                        alter_query = ('ALTER TABLE ' + target_table_id
                                       + ' ALTER COLUMN ' + alt_col)
                        logger.info(alter_query)
                        bigquery_client.query(alter_query).result()

                add_columns = transformation.get(consts.COLUMN).get(consts.ALTER_TABLE_ADD_COLUMNS)
                if add_columns is not None:
                    for add_col in add_columns:
                        alter_query = ('ALTER TABLE ' + target_table_id
                                       + ' ADD COLUMN IF NOT EXISTS ' + add_col)
                        logger.info(alter_query)
                        bigquery_client.query(alter_query).result()

    def create_dag(self, dag_id: str, dag_config: dict) -> DAG:
        default_args = dag_config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS)
        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            schedule=None,
            start_date=datetime(2023, 1, 1, tzinfo=self.local_tz),
            catchup=False,
            max_active_runs=1,
            dagrun_timeout=timedelta(hours=5),
            render_template_as_native_obj=True,
            is_paused_upon_creation=True,
            tags=dag_config.get(consts.TAGS, [])
        )

        with dag:
            start = EmptyOperator(task_id='start')
            end = EmptyOperator(task_id='end')

            load_config = PythonOperator(
                task_id="load_config",
                python_callable=self.build_evolution_config,
                op_kwargs={'dag_config': dag_config}
            )

            back_up_original_table = PythonOperator(
                task_id="back_up_original_table",
                python_callable=self.build_back_up_job,
                op_kwargs={'evolution_config': "{{ ti.xcom_pull(task_ids='load_config') }}"}
            )

            create_updated_table = PythonOperator(
                task_id="create_updated_table",
                python_callable=self.build_new_table_creation_job,
                op_kwargs={'evolution_config': "{{ ti.xcom_pull(task_ids='load_config') }}"}
            )

            alter_table_columns = PythonOperator(
                task_id="alter_table_columns",
                python_callable=self.alter_columns,
                op_kwargs={'evolution_config': "{{ ti.xcom_pull(task_ids='load_config') }}"}
            )

            start >> load_config >> back_up_original_table >> create_updated_table >> alter_table_columns >> end

        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if self.job_config:
            for job_id, config in self.job_config.items():
                dags[job_id] = self.create_dag(job_id, config)

        return dags


globals().update(BigQuerySchemaEvolutionTool('bigquery_schema_evolution_config.yaml').create_dags())
