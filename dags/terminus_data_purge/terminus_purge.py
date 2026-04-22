import logging

import pendulum
import util.constants as consts
from airflow import DAG, settings
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
from typing import Final
from util.miscutils import (
    read_variable_or_file,
    read_yamlfile_env,
)
from dag_factory.terminus_dag_factory import add_tags

GS_TAG: Final[str] = "team-growth-and-sales"


def get_cuttoff_date(years_ago):
    return (
        "{{ dag_run.conf.get('cutoff_date', '"
        + pendulum.now(consts.TORONTO_TIMEZONE_ID).subtract(years=years_ago).strftime("%Y%m%d")
        + "') }}"
    )


def get_purge_cycle():
    purge_date = pendulum.now(consts.TORONTO_TIMEZONE_ID).strftime("%Y%m%d")
    return 'purge_cycle_' + purge_date


def automatic_purge_fuse(automatic_purge_flag):
    if automatic_purge_flag != 'Y':
        raise AirflowFailException("Automatic purge criteria is not met!")


class TerminusDataPurge:
    def __init__(self):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config['deployment_environment_name']
        self.config_dir = f'{settings.DAGS_FOLDER}/config'
        self.dags_config = read_yamlfile_env(f'{self.config_dir}/terminus_purging_config.yaml', self.deploy_env)
        self.litigation_employees = None
        self.exclusion_rules = None
        self.ENV_PLACEHOLDER = '{env}'
        self.ADM_SRC = 'ADM'
        self.EXPERIAN_SRC = 'EXPERIAN'
        self.EAPPS_SRC = 'EAPPS'
        self.default_args = {"owner": "team-growth-and-sales-alerts",
                             'capability': 'CustomerAcquisition',
                             'severity': 'P3',
                             'sub_capability': 'Applications',
                             'business_impact': 'Compliance Issue if the data is not purged',
                             'customer_impact': 'None',
                             "start_date": pendulum.today(consts.TORONTO_TIMEZONE_ID).add(days=-1),
                             "email": [],
                             "email_on_failure": False,
                             "email_on_retry": False,
                             "depends_on_past": False,
                             "retries": 1,
                             "retry_delay": timedelta(seconds=20),
                             "retry_exponential_backoff": True}

    def create_source_purge_stat_dag(self, source, source_config: dict, dags: dict, exclusion_query) -> None:
        source_dag_id = f'terminus_{source.lower()}_data_purging_app_number_generator'
        qualified_table_id = source_config.get('purgelist_stat_table').get('name')
        application_id_column = source_config.get('purgelist_stat_table').get('application_number_column_name')
        purge_cycle = get_purge_cycle()
        cutoff_date = get_cuttoff_date(5)
        base_query = self.get_source_purge_query(source_config.get('base_query_sql'), cutoff_date, purge_cycle)
        if exclusion_query is None:
            insert_query = f'INSERT INTO {qualified_table_id} (SELECT * FROM ({base_query}))'
        else:

            # Get exclusion query for source
            if source.upper() == self.ADM_SRC:
                exclusion_query = exclusion_query[0].replace(self.ENV_PLACEHOLDER, self.deploy_env)
            elif source.upper() == self.EXPERIAN_SRC:
                exclusion_query = exclusion_query[1].replace(self.ENV_PLACEHOLDER, self.deploy_env)
            elif source.upper() == self.EAPPS_SRC:
                exclusion_query = exclusion_query[2].replace(self.ENV_PLACEHOLDER, self.deploy_env)

            insert_query = f"""INSERT INTO {qualified_table_id} (SELECT * FROM ({base_query})
                                WHERE {application_id_column} NOT IN ({exclusion_query}))"""

        dag = DAG(
            dag_id=source_dag_id,
            default_args=self.default_args,
            schedule=None,
            catchup=False,
            is_paused_upon_creation=True,
            max_active_runs=1,
            tags=[GS_TAG]
        )
        with dag:
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)
            task_id = f'populate_{source}_stat_table'
            populate_initial_purge_list = BigQueryInsertJobOperator(
                task_id=task_id,
                configuration={
                    "query": {
                        "query": insert_query,
                        "useLegacySql": False,
                    }
                },
                location=self.gcp_config.get('bq_query_location'),
            )
            start_point >> populate_initial_purge_list >> end_point
            dags[source_dag_id] = add_tags(dag)

    def create_source_automatic_purge_dag(self, source, source_config: dict, dags: dict,
                                          environment_specific_config: dict) -> None:
        source_dag_id = f'terminus_{source.lower()}_data_purging_task_runner'
        qualified_table_id = source_config.get('purgelist_stat_table').get('name')
        application_id_column = source_config.get('purgelist_stat_table').get('application_number_column_name')
        identify_open_purge_cycle_query = f"""
                                          SELECT PURGE_CYCLE FROM (
                                                                    SELECT
                                                                    DISTINCT PURGE_LIST_DATE,
                                                                    PURGE_CYCLE
                                                                    FROM {qualified_table_id} WHERE IS_TERMINUS='Y'
                                                                    AND PURGE_IND = 0
                                                                    ORDER BY
                                                                    PURGE_LIST_DATE DESC LIMIT 1 )
                                          """
        dag = DAG(
            dag_id=source_dag_id,
            default_args=self.default_args,
            schedule=None,
            catchup=False,
            is_paused_upon_creation=True,
            max_active_runs=1,
            tags=[GS_TAG]
        )
        with dag:
            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)
            if environment_specific_config.get('automatic_purge').get(self.deploy_env):
                automatic_purge_flag = environment_specific_config.get('automatic_purge').get(self.deploy_env)
            break_glass_automatic_purge_verification_task = PythonOperator(
                task_id='check_delete_criteria',
                python_callable=automatic_purge_fuse,
                op_kwargs={"automatic_purge_flag": automatic_purge_flag},
                retries=0,
                trigger_rule=TriggerRule.ALL_DONE
            )
            purge_tasks = []
            purge_tasks_dict = {}  # Dictionary to store tasks by table name for dependency management
            bq_tables_configs = source_config.get('bq_tables')
            for table_config in bq_tables_configs:
                table_name = table_config.get("name")
                task_id = f'purge_{table_name}'
                table_deletion_batch_query = self.get_deletion_batch_query(table_config, qualified_table_id,
                                                                           application_id_column,
                                                                           identify_open_purge_cycle_query)
                purge_table_task = BigQueryInsertJobOperator(
                    task_id=task_id,
                    configuration={
                        "query": {
                            "query": table_deletion_batch_query,
                            "useLegacySql": False,
                        }
                    },
                    location=self.gcp_config.get('bq_query_location'),
                )
                purge_tasks.append(purge_table_task)
                purge_tasks_dict[table_name] = purge_table_task  # Store task object for dependency management

            # Add explicit dependency: APP_CUST_INFO must run BEFORE ADM_RT_RESPONSE_UNS
            # This ensures APP_CUST_INFO can read from ADM_RT_RESPONSE_UNS before it's deleted
            app_cust_info_task = None
            adm_rt_response_uns_task = None

            for table_name, task in purge_tasks_dict.items():
                if 'APP_CUST_INFO' in table_name:
                    app_cust_info_task = task
                if 'ADM_RT_RESPONSE_UNS' in table_name:
                    adm_rt_response_uns_task = task

            if app_cust_info_task and adm_rt_response_uns_task:
                app_cust_info_task >> adm_rt_response_uns_task

            update_intermediate_table_query = f"""
                                                  UPDATE {qualified_table_id} SET PURGE_IND=1 WHERE IS_TERMINUS='Y'
                                                  AND PURGE_CYCLE=({identify_open_purge_cycle_query})
                                               """
            task_id = f'update_{source.lower()}_stat_table'
            update_stat_table_task = BigQueryInsertJobOperator(
                task_id=task_id,
                configuration={
                    "query": {
                        "query": update_intermediate_table_query,
                        "useLegacySql": False,
                    }
                },
                location=self.gcp_config.get('bq_query_location'),
                trigger_rule=TriggerRule.ALL_SUCCESS,
            )
            start_point >> break_glass_automatic_purge_verification_task >> purge_tasks >> update_stat_table_task >> end_point
            dags[source_dag_id] = add_tags(dag)

    def create_purge_dags(self) -> dict:
        dags = {}
        terminus_purge_config = self.dags_config.get('terminus.purging')
        environment_specific_config = terminus_purge_config.get('environment_specific_purge_attributes')
        exclusion_query = terminus_purge_config.get('exclusion.config.sql')
        source_configs = terminus_purge_config.get('sources')
        for source, source_config in source_configs.items():
            self.create_source_purge_stat_dag(source, source_config, dags, exclusion_query)
            self.create_source_automatic_purge_dag(source, source_config, dags, environment_specific_config)
        return dags

    def get_source_purge_query(self, sql_file_name, cutoff_date, purge_cycle) -> str:
        file_path = f'{settings.DAGS_FOLDER}/terminus_data_purge/sql/{sql_file_name}'
        with open(file_path) as file:
            sql = file.read()
        return sql.replace(self.ENV_PLACEHOLDER, self.deploy_env).replace("{cutoff_date}", cutoff_date). \
            replace("{purge_cycle}", purge_cycle)

    def get_deletion_batch_query(self, bq_table_config: dict, source_table, application_id_column, identify_open_purge_cycle_query) -> str:
        table_name = bq_table_config.get("name")
        id_column = bq_table_config.get("id.column")

        if bq_table_config.get("join.table") is not None:
            qualified_join_table_name = bq_table_config.get("join.table")
            join_column_from = bq_table_config.get("join.column.from")
            join_column_to = bq_table_config.get("join.column.to")
            deletion_batch_query = f"""
                                   DELETE FROM {table_name} WHERE {join_column_from} IN
                                    (
                                        SELECT {join_column_to} FROM {qualified_join_table_name} WHERE {id_column} IN
                                            (
                                                SELECT {application_id_column} FROM {source_table} WHERE
                                                PURGE_CYCLE = ({identify_open_purge_cycle_query})
                                                AND IS_TERMINUS='Y'
                                            )
                                    )
                                   """
        else:
            deletion_batch_query = f"""
                                   DELETE FROM {table_name} WHERE {id_column} IN
                                    (
                                        SELECT {application_id_column} FROM {source_table} WHERE
                                        PURGE_CYCLE = ({identify_open_purge_cycle_query})
                                        AND IS_TERMINUS='Y'
                                    )
                                    """
        return deletion_batch_query


globals().update(TerminusDataPurge().create_purge_dags())
