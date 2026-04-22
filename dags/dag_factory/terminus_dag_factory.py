import logging
from dag_factory.sre_parameters import SREParameter
from dag_factory.environment_config import EnvironmentConfig
from dag_factory.abc import BaseDagBuilder
from airflow import DAG, settings
from util.miscutils import read_yamlfile_env

logger = logging.getLogger(__name__)


def add_tags(dag: DAG) -> DAG:
    default_args = dag.default_args
    additional_tags = [default_args.get(SREParameter.OWNER.value), default_args.get(SREParameter.SEVERITY.value)]
    enriched_tags = []
    dag_tags = dag.tags or []
    for tag in additional_tags:
        if tag and (tag not in dag_tags):
            enriched_tags.append(tag)
    dag.tags = enriched_tags + dag_tags
    return dag


class DAGFactory:
    def __init__(self, environment_config: EnvironmentConfig = None):
        self.environment_config = environment_config if environment_config else EnvironmentConfig.load()

    def create_dynamic_dags(self, dag_class: type[BaseDagBuilder], config_filename: str = None, config_dir: str = None) -> dict:
        dag_config = {}
        if config_filename:
            config_dir = config_dir if config_dir else f'{settings.DAGS_FOLDER}/config'
            config_file_path = f'{config_dir}/{config_filename}'
            dag_config = read_yamlfile_env(config_file_path, self.environment_config.deploy_env)

        dags = {}
        if dag_config:
            for dag_id, config in dag_config.items():
                dag = dag_class(self.environment_config).build(dag_id, config)
                enriched_dag = add_tags(dag)
                dags[dag_id] = enriched_dag

        return dags

    def create_dag(self, dag_class: type[BaseDagBuilder], dag_id: str) -> dict:
        dags = {}
        if dag_id:
            dag = dag_class(self.environment_config).build(dag_id, {})
            enriched_dag = add_tags(dag)
            dags[dag_id] = enriched_dag

        return dags
