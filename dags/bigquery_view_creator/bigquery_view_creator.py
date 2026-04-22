import logging
from datetime import timedelta, datetime
from typing import Final, Optional, Dict, Any

import pandas as pd
import pendulum
from copy import deepcopy
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery

import util.constants as consts
from util.bq_utils import (map_names, convert_timestamp_to_datetime)
from util.constants import COMMA_SPACE
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
    read_file_env
)
from dag_factory.terminus_dag_factory import add_tags

BIGQUERY_VIEW_CREATOR: Final = 'bigquery_view_creator'
VIEW_CREATION_TASK_ID: Final = 'create_views'
COLUMN_NAME_MAPPINGS_TABLE_ID: Final = 'JobControlDataset.COLUMN_NAME_MAPPINGS'

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
    "execution_timeout": timedelta(minutes=10),
    "retries": 3,
    'retry_delay': timedelta(hours=1)
}


class BigQueryViewCreator:
    def __init__(self, config_dir: str = None, config_filename: str = None, column_maps_filename: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]

        if config_dir is None:
            config_dir = f'{settings.DAGS_FOLDER}/config'

        if config_filename is None:
            config_filename = 'bigquery_view_creator_config.yaml'

        if column_maps_filename is None:
            column_maps_filename = 'column_name_mappings.csv'

        self.views_config = read_yamlfile_env(f'{config_dir}/{config_filename}', self.deploy_env)
        self.mapping_file_location = f'{config_dir}/{column_maps_filename}'
        self.column_name_mappings = pd.read_csv(self.mapping_file_location)
        self.grouped_column_name_mappings = self.column_name_mappings.groupby(
            [consts.LANDING_ZONE_DATASET, consts.LANDING_ZONE_TABLE, consts.CURATED_ZONE_DATASET,
             consts.CURATED_ZONE_VIEW],
            as_index=False,
        ).agg(list)
        self.view_files_dir = config_dir + '/bigquery_view_sql'

        self.local_tz = pendulum.timezone(consts.TORONTO_TIMEZONE_ID)

        self.default_args = deepcopy(INITIAL_DEFAULT_ARGS)

    def load_mappings_to_bigquery(self):
        mappings = pd.read_csv(self.mapping_file_location) \
            .sort_values(by=['Landing_Zone_Dataset', 'Landing_Zone_Table', 'Landing_Zone_Name'], ignore_index=True)

        curated_zone_project = self.gcp_config[consts.CURATED_ZONE_PROJECT_ID]

        mapping_table_ref = f'{curated_zone_project}.{COLUMN_NAME_MAPPINGS_TABLE_ID}'
        saved_mappings = pd.read_gbq(f'SELECT * FROM `{mapping_table_ref}`', project_id=curated_zone_project) \
            .sort_values(by=['Landing_Zone_Dataset', 'Landing_Zone_Table', 'Landing_Zone_Name'], ignore_index=True)

        if not saved_mappings.equals(mappings):
            logger.info('updating mappings table')
            mappings.to_gbq(COLUMN_NAME_MAPPINGS_TABLE_ID, project_id=curated_zone_project, if_exists='replace')
        else:
            logger.info('no update in mappings')

    def generate_sql(self, bigquery_client, table_ref: str, column_mappings: list):
        columns = convert_timestamp_to_datetime(bigquery_client, table_ref, column_mappings)

        return f"SELECT {columns} FROM `{table_ref}`"

    def generate_description(self, bigquery_client, table_ref: str, column_mappings: list):
        bq_table = bigquery_client.get_table(table_ref)  # Make an API request.
        logger.info(f"Table {table_ref} is found.")

        description = ''

        if bq_table.time_partitioning is not None:
            type = bq_table.time_partitioning.type_
            field = bq_table.time_partitioning.field
            expiration = '' if bq_table.time_partitioning.expiration_ms is None \
                else datetime.fromtimestamp(bq_table.time_partitioning.expiration_ms / 1000, self.local_tz)
            filter = 'Required' if bq_table.time_partitioning.require_partition_filter else 'Not Required'
            partition_description = f"\\nPartitioned by: {type}\\nPartitioned on field: {field}\\n" \
                                    f"Partition expiration: {expiration}\\nPartition filter: {filter}\\n"
            description = f"{partition_description}"

        if bq_table.clustering_fields is not None:
            mapped_fields = map_names(bq_table.clustering_fields, column_mappings)
            clustering_description = "\\n".join(mapped_fields)
            description += f"Clustered by:\\n{clustering_description}"

        return description

    def create_view(self, table_ref: str, view_ref: str, view_sql: str, view_file: str):
        dataset_table_id = table_ref.split('.', 1)[1]
        table_id = dataset_table_id.split('.', 1)[1].lower()

        mappings = self.grouped_column_name_mappings[self.grouped_column_name_mappings.apply(
            lambda x: f"{x[consts.LANDING_ZONE_DATASET]}.{x[consts.LANDING_ZONE_TABLE]}" == dataset_table_id,
            axis=1, result_type='reduce')]

        column_mappings = {}
        if not mappings.empty:
            row = mappings.iloc[0]
            column_mappings = dict(zip(row[consts.LANDING_ZONE_NAME], row[consts.CURATED_ZONE_NAME]))

        bq_client = bigquery.Client()
        if view_file is not None and len(view_file) > 0:
            view_sql = read_file_env(f'{self.view_files_dir}/{view_file}', self.deploy_env)
        view_query = view_sql or self.generate_sql(bq_client, table_ref, column_mappings)
        description = self.generate_description(bq_client, table_ref, column_mappings)
        # ddl updates view schema. 'bigquery.Client().update_table()' does not
        view_ddl = f"""
                        CREATE OR REPLACE VIEW
                            `{view_ref}`
                        OPTIONS (
                          description = "{description}",
                          labels= [('table_ref','{table_id}')]
                        )
                        AS
                            {view_query}
                """
        logger.info(view_ddl)
        query_job = bq_client.query(view_ddl)
        query_job.result()

    def create_materialized_view(self, table_ref: str, view_ref: str, view_sql: str, view_file: str,
                                 materialized_view_config: Optional[Dict[str, Any]]):
        dataset_table_id = table_ref.split('.', 1)[1]
        table_id = dataset_table_id.split('.', 1)[1].lower()

        mappings = self.grouped_column_name_mappings[self.grouped_column_name_mappings.apply(
            lambda x: f"{x[consts.LANDING_ZONE_DATASET]}.{x[consts.LANDING_ZONE_TABLE]}" == dataset_table_id,
            axis=1, result_type='reduce')]

        column_mappings = {}
        if not mappings.empty:
            row = mappings.iloc[0]
            column_mappings = dict(zip(row[consts.LANDING_ZONE_NAME], row[consts.CURATED_ZONE_NAME]))

        bq_client = bigquery.Client()
        if view_file is not None and len(view_file) > 0:
            view_sql = read_file_env(f'{self.view_files_dir}/{view_file}', self.deploy_env)
        view_query = view_sql or self.generate_sql(bq_client, table_ref, column_mappings)
        description = self.generate_description(bq_client, table_ref, column_mappings)

        partition_field = materialized_view_config.get('partition_field')
        clustering_fields = materialized_view_config.get('clustering_fields')
        logger.info(f'partition: {partition_field}, cluster: {clustering_fields}')
        partition_str = f"PARTITION BY {partition_field}" if partition_field else ''
        clustering_str = "CLUSTER BY " + COMMA_SPACE.join(clustering_fields) if clustering_fields else ''
        logger.info(f'partition: {partition_str}, cluster: {clustering_str}')

        materialized_view_options_str = ''
        if options_dict := materialized_view_config.get(consts.MATERIALIZED_VIEW_OPTIONS):
            materialized_view_options_str = ', '.join([f'{k} = {v}' for k, v in options_dict.items()])

        logger.info(f'Materialized View Options: {materialized_view_options_str}')

        # ddl updates view schema. 'bigquery.Client().update_table()' does not
        view_ddl = f"""CREATE OR REPLACE MATERIALIZED VIEW `{view_ref}`
                        {partition_str}
                        {clustering_str}
                        OPTIONS (
                          description = "{description}",
                          labels= [('table_ref','{table_id}')],
                          {materialized_view_options_str}
                        )
                        AS
                            {view_query}
                """
        logger.info(view_ddl)
        query_job = bq_client.query(view_ddl)
        query_job.result()

    def create_sample_table(self, view_ref: str, sample_size: int, view):
        # using suffix `-SAMPLE-TABLE` so that sample tables are kept together with views
        sample_table_ref = f'{view.project}.{view.dataset_id}.{view.table_id}-SAMPLE-TABLE'

        query = f"""
            CREATE OR REPLACE TABLE
                `{sample_table_ref}`
            OPTIONS (
              description = "Sample data of {view.dataset_id}.{view.table_id}. Sample Size: {sample_size}"
            )
            AS
                SELECT *
                FROM `{view_ref}`
                LIMIT {sample_size};
        """

        logger.info(query)
        query_job = bigquery.Client().query(query)
        query_job.result()

    def create_views(self, view_configs: dict):
        task_groups = []

        for view_config in view_configs['tables']:
            table_ref = view_config.get(consts.TABLE_REF)
            view_ref = view_config.get(consts.VIEW_REF)
            view = bigquery.Table(view_ref)
            table = bigquery.Table(table_ref)
            view_sql = view_config.get(consts.VIEW_SQL)
            view_file = view_config.get(consts.VIEW_FILE)
            sample_size = view_config.get(consts.SAMPLE_SIZE) or 1000
            materialized_view_config = view_config.get(consts.MATERIALIZED_VIEW_CONFIG)

            with TaskGroup(group_id=view.table_id) as tgrp:
                check_table = BigQueryTableExistenceSensor(task_id='check_table',
                                                           project_id=table.project,
                                                           dataset_id=table.dataset_id,
                                                           table_id=table.table_id,
                                                           timeout=300,
                                                           mode='reschedule')

                if not materialized_view_config:
                    create_view = PythonOperator(
                        task_id="create_view",
                        python_callable=self.create_view,
                        op_kwargs={'table_ref': table_ref, 'view_ref': view_ref, 'view_sql': view_sql,
                                   'view_file': view_file}
                    )

                    create_sample_table = PythonOperator(
                        task_id="create_sample_table",
                        python_callable=self.create_sample_table,
                        op_kwargs={'view_ref': view_ref, 'sample_size': sample_size, 'view': view},
                        trigger_rule='none_failed'
                    )

                    check_table >> create_view >> create_sample_table

                else:
                    create_materialized_view = PythonOperator(
                        task_id="create_materialized_view",
                        python_callable=self.create_materialized_view,
                        op_kwargs={'table_ref': table_ref, 'view_ref': view_ref, 'view_sql': view_sql,
                                   'view_file': view_file, 'materialized_view_config': materialized_view_config}
                    )

                    check_table >> create_materialized_view

                task_groups.append(tgrp)

        return task_groups

    def build_dag(self, dag_id: str, view_configs: dict) -> DAG:
        default_args = view_configs.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS)
        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            schedule=None,
            start_date=pendulum.datetime(2024, 1, 1, tz=consts.TORONTO_TIMEZONE_ID),
            catchup=False,
            dagrun_timeout=timedelta(minutes=60),
            is_paused_upon_creation=True,
            max_active_tasks=5,
            tags=view_configs.get(consts.TAGS, [])
        )
        with dag:
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            config_loading_task = PythonOperator(
                task_id='load_column_name_mappings_to_bq',
                python_callable=self.load_mappings_to_bigquery,
                trigger_rule='none_failed'
            )

            task_groups = self.create_views(view_configs)

            start_point >> task_groups >> config_loading_task >> end_point
        return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if not self.views_config:
            logger.info('view config is empty.')
            return dags

        for job_id, view_configs in self.views_config.items():
            dags[job_id] = self.build_dag(job_id, view_configs)

        return dags


globals().update(BigQueryViewCreator().create_dags())
