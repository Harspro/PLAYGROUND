import logging
import pendulum
import pprint
from datetime import datetime, timedelta

from airflow import DAG
from airflow import configuration as conf
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

import util.constants as consts
from bigquery_loader.bigquery_loader_base import BigQueryLoader, LoadingFrequency, INITIAL_DEFAULT_ARGS
from util.deploy_utils import read_pause_unpause_setting, pause_unpause_dag
from dag_factory.terminus_dag_factory import add_tags

logger = logging.getLogger(__name__)


class CsvToBigQueryLoader(BigQueryLoader):
    '''
    A class to load CSV files in Google Cloud Storage to BigQuery.
    Supports custom schema, add/drop/rename columns, simple transformations of columns
    Future: Add support for data validation
    '''

    def __init__(self, config_filename: str, loading_frequency: LoadingFrequency, config_dir: str = None):
        super().__init__(config_filename, loading_frequency, config_dir)

    def get_ext_table_ref(self, config):
        ext_project = f"{self.gcp_config.get('processing_zone_project_id')}"
        ext_dataset = config['bigquery']['dataset_id']
        ext_table = config['bigquery']['table_name'] + '_EXT'
        return f"{ext_project}.{ext_dataset}.{ext_table}"

    def load_file_to_bq_external(self, config, **context):
        # Get the context first then config. Useful for triggered dags
        pprint.pprint(config)
        if context.get('dag_run') and context['dag_run'].conf.get('name'):
            filename = context['dag_run'].conf.get('name')
            source_bucket = context['dag_run'].conf.get('bucket')
            source_file_path = f"gs://{source_bucket}/{filename}"
        else:
            filename = config['source_file']['name']
            if config['source_file'].get('bucket') == 'COMPOSER_BUCKET':
                source_bucket = conf.get('logging', 'remote_base_log_folder')
                if source_bucket.endswith('/logs'):
                    source_bucket = source_bucket[:-5]
                    source_file_path = f"{source_bucket}/{filename}"
            else:
                source_bucket = config['source_file'].get('bucket')
                source_file_path = f"gs://{source_bucket}/{filename}"

        print(source_bucket, filename)
        if not filename or not source_bucket:
            raise AirflowFailException("Source filename or bucket not specified")

        ext_table_ref = self.get_ext_table_ref(config)
        client = bigquery.Client()
        client.delete_table(ext_table_ref, not_found_ok=True)

        external_config = bigquery.ExternalConfig("CSV")
        if config['source_file'].get('source_format'):
            external_config = bigquery.ExternalConfig(config['source_file']['source_format'])

        external_config.source_uris = [source_file_path]

        external_config.csv_options.skip_leading_rows = 1
        if config['source_file']['csv_options'].get('skip_leading_rows') is not None:
            external_config.csv_options.skip_leading_rows = config['source_file']['csv_options']['skip_leading_rows']

        external_config.csv_options.field_delimiter = ','
        if config['source_file']['csv_options'].get('field_delimiter'):
            external_config.csv_options.field_delimiter = config['source_file']['csv_options']['field_delimiter']

        external_config.ignore_unknown_values = False
        if config['source_file'].get('ignore_unknown_values'):
            external_config.ignore_unknown_values = config['source_file']['ignore_unknown_values']

        external_config.max_bad_records = 0
        if config['source_file'].get('max_bad_records'):
            external_config.max_bad_records = config['source_file']['max_bad_records']

        if config['source_file'].get('schema_external'):
            bq_table_schema = [bigquery.SchemaField.from_api_repr(s) for s in config['source_file']['schema_external']]
            external_config.autodetect = False
            external_table = bigquery.Table(ext_table_ref, schema=bq_table_schema)
        else:
            external_table = bigquery.Table(ext_table_ref)

        print(external_config)
        external_table.expires = datetime.now() + timedelta(days=1)
        external_table.external_data_configuration = external_config
        print(f'ext_schema: {external_config.schema}')
        client.create_table(external_table)

        target_table = client.get_table(ext_table_ref)
        logging.info(
            f"Loaded {source_file_path} to BQ table {ext_table_ref}, which now has {target_table.num_rows} rows")

    def load_bq_dest_table(self, config, dag_id, **context):
        bq_config = config['bigquery']

        src_table = self.get_ext_table_ref(config)

        dest_table_ref = (f"{bq_config.get('project_id')}."
                          f"{bq_config.get('dataset_id')}."
                          f"{bq_config.get('table_name')}")

        drop_columns = bq_config.get('drop_columns')
        drop_columns_str = ""
        if drop_columns:
            drop_columns_str = "EXCEPT(" + ", ".join(drop_columns) + ")"
        print(f'drop_columns_str: {drop_columns_str}')

        client = bigquery.Client()

        add_columns = bq_config.get('add_columns', "")
        add_columns_str = ""
        if add_columns:
            add_columns_str = ",\n".join(add_columns)
        print(f'add_columns_str = {add_columns_str}')

        target_columns = bq_config.get('target_columns')
        target_columns_str = ""
        if target_columns:
            target_columns_str = ",\n".join(target_columns)
        elif bq_config['data_load_type'] == "MERGE_INSERT":
            raise AirflowFailException("Target Columns must be specified for MERGE_INSERT option")
        print(f'target_columns_str = {target_columns_str}')

        source_columns = bq_config.get('source_columns')
        source_columns_str = ""
        if source_columns:
            source_columns_str = ",\n".join(source_columns)
        elif bq_config['data_load_type'] == "MERGE_INSERT":
            raise AirflowFailException("Source Columns must be specified for MERGE_INSERT option")
        print(f'source_columns_str = {source_columns_str}')

        insert_values = bq_config.get('insert_values', "")
        insert_str = ""
        if insert_values:
            insert_str = ",\n".join(insert_values)
        else:
            insert_str = source_columns_str
        print(f'insert_str = {insert_str}')

        partition_field = bq_config.get('partition_field')
        clustering_fields = bq_config.get('clustering_fields')

        partition_str = ""
        if partition_field:
            partition_str = f"PARTITION BY {partition_field}"

        clustering_str = ""
        if clustering_fields:
            clustering_str = "CLUSTER BY " + ", ".join(clustering_fields)

        add_file_name_str = ""
        add_file_name_column = ""
        add_file_name_config = bq_config.get(consts.ADD_FILE_NAME_COLUMN)
        if add_file_name_config:
            add_file_name_str = f", '{context['dag_run'].conf.get('bucket')}/{context['dag_run'].conf.get('name')}'"
            add_file_name_column = f"{add_file_name_str} as {add_file_name_config.get(consts.ALIAS)}"
        print(f'add_file_name_column = {add_file_name_column}')

        var_dict = Variable.get(dag_id, deserialize_json=True, default_var={})
        override_table = var_dict and var_dict.get('overwrite_bq_table')

        print(f'override_table={override_table}')

        if override_table or bq_config['data_load_type'] == "FULL_REFRESH":
            CREATE_DEST_TABLE_DDL = f"""
                CREATE OR REPLACE TABLE
                    `{dest_table_ref}`
                    {partition_str}
                    {clustering_str}
                AS
                    SELECT * {drop_columns_str},
                        {add_columns_str}
                        {add_file_name_column}
                    FROM `{src_table}` t
            """
        elif bq_config['data_load_type'] == "APPEND_ONLY":
            CREATE_DEST_TABLE_DDL = f"""\
                CREATE TABLE IF NOT EXISTS
                    `{dest_table_ref}`
                    {partition_str}
                    {clustering_str}
                AS
                    SELECT * {drop_columns_str},
                        {add_columns_str}
                        {add_file_name_column}
                    FROM `{src_table}`
                    LIMIT 0;

                INSERT INTO `{dest_table_ref}`
                SELECT *
                    {drop_columns_str},
                    {add_columns_str}
                    {add_file_name_str}
                FROM `{src_table}`;
            """
        elif bq_config['data_load_type'] == "MERGE_INSERT":
            merge_config = config['merge']

            merge_join_columns = merge_config.get(consts.JOIN_COLUMNS)
            print(f'merge_join_columns = {merge_join_columns}')
            if merge_join_columns:
                merge_join_conditions = [f'{x.get(consts.JOIN_FROM_COLUMN)}={x.get(consts.JOIN_TO_COLUMN)}'
                                         for x in merge_join_columns]
                merge_join_columns_str = " AND ".join(merge_join_conditions)
            else:
                raise AirflowFailException("Please provide merge join condition for MERGE_INSERT option")
            print(f'merge_join_columns_str = {merge_join_columns_str}')

            src_merge_table = f"`{src_table}`"
            if config['merge'].get('distinct_source'):
                src_merge_table = f"(Select distinct {source_columns_str} from `{src_table}`)"

            CREATE_DEST_TABLE_DDL = f"""\
                CREATE TABLE IF NOT EXISTS
                    `{dest_table_ref}`
                    {partition_str}
                    {clustering_str}
                AS
                    SELECT * {drop_columns_str},
                        {add_columns_str}
                    FROM `{src_table}`
                    LIMIT 0;

                MERGE `{dest_table_ref}` T
                USING {src_merge_table} S
                ON {merge_join_columns_str}
                WHEN NOT MATCHED THEN
                    INSERT ({target_columns_str})
                    VALUES ({insert_str}{add_file_name_str})
            """
        else:
            print('bigquery data_load_type not understood')
            raise AirflowFailException("config: bigquery data_load_type not understood")

        print('\n'.join([m.lstrip() for m in CREATE_DEST_TABLE_DDL.split('\n')]))
        query_job = client.query(CREATE_DEST_TABLE_DDL)
        query_job.result()

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        self.default_args.update(config.get(consts.DEFAULT_ARGS, INITIAL_DEFAULT_ARGS))

        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            description='CSV to BigQuery Loader.',
            schedule=None,
            start_date=pendulum.today('America/Toronto').add(days=-1),
            max_active_runs=1,
            catchup=False,
            dagrun_timeout=timedelta(hours=5),
            is_paused_upon_creation=True,
            tags=config.get(consts.TAGS)
        )

        with dag:
            if config.get('dag') and config['dag'].get('read_pause_deploy_config'):
                is_paused = read_pause_unpause_setting(dag_id, self.deploy_env)
                pause_unpause_dag(dag, is_paused)

            start_point = EmptyOperator(task_id='start')
            end_point = EmptyOperator(task_id='end')

            table_name = config['bigquery']['table_name']
            load_ext_table_task = PythonOperator(
                task_id=f"load_bq_external_{table_name}",
                python_callable=self.load_file_to_bq_external,
                op_kwargs={consts.CONFIG: config}
            )

            load_bq_task_id = "bigquery_load_dest_table"
            if config['bigquery']['data_load_type'] == "FULL_REFRESH":
                load_bq_task_id = f"bigquery_full_refresh_{table_name}"
            elif config['bigquery']['data_load_type'] == "APPEND_ONLY":
                load_bq_task_id = f"bigquery_append_table_{table_name}"
            elif config['bigquery']['data_load_type'] == "MERGE_INSERT":
                load_bq_task_id = f"bigquery_merge_insert_table_{table_name}"

            load_dest_table_task = PythonOperator(
                task_id=load_bq_task_id,
                python_callable=self.load_bq_dest_table,
                op_kwargs={'dag_id': dag_id, consts.CONFIG: config}
            )

            start_point >> load_ext_table_task >> load_dest_table_task >> end_point
            return add_tags(dag)

    def create_dags(self) -> dict:
        dags = {}

        if not self.job_config:
            logger.info(f'Config file {self.config_file_path} is empty.')
            return dags

        for job_id, config in self.job_config.items():
            dags[job_id] = self.create_dag(job_id, config)

        return dags


csv_to_bq_loader = CsvToBigQueryLoader('csv_to_bigquery_config.yaml', LoadingFrequency.Onetime)
globals().update(csv_to_bq_loader.create_dags())
