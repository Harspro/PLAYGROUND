from airflow import settings
import logging
from airflow.models import DagModel, DAG
from util.miscutils import (
    read_yamlfile_env
)


######################################################################
# Pause / Unpause Util Functions
######################################################################

logger = logging.getLogger(__name__)


def read_pause_unpause_setting(dag_name, deploy_env):
    """
    The function reads the deploy config settings for the given dag
    and given environment
    """

    # read the yaml file
    deploy_settings = read_yamlfile_env(fname=f"{settings.DAGS_FOLDER}/config/deploy_config.yaml")

    if dag_name in deploy_settings:
        is_paused = deploy_settings[dag_name][deploy_env]
    else:
        is_paused = deploy_settings['default'][deploy_env]

    return is_paused


def read_feature_flag(feature_flag: str, deploy_env):
    """
    The function reads feature flag for the given environment.
    Feature flags must starts with 'feature.flag'
    """

    if not feature_flag or not feature_flag.startswith('feature.flag'):
        raise Exception(f'{feature_flag} is not a feature flag')

    # read the yaml file
    deploy_settings = read_yamlfile_env(fname=f"{settings.DAGS_FOLDER}/config/deploy_config.yaml")

    if feature_flag in deploy_settings:
        return deploy_settings[feature_flag].get(deploy_env)

    return False


def pause_unpause_dag(dag, paused_bool):
    """
    Takes the dag_id and the boolean whether to pause the dag or not
    Returns nothing but pauses / unpauses the dag.
    """
    if isinstance(dag, DAG):
        dag_id = dag.dag_id
    else:
        dag_id = dag
    dagmodel = DagModel.get_dagmodel(dag_id)
    if dagmodel is not None:
        dagmodel.set_is_paused(is_paused=paused_bool)


def pause_unpause_dags(dag_ids: list, is_paused: bool):
    """
    Pauses or unpauses a list of DAGs using the deploy_utils.pause_unpause_dag method.

    :param dag_ids: List of DAG IDs to pause/unpause.
    :type dag_ids: list

    :param is_paused: Whether to pause (True) or unpause (False) the DAGs.
    :type is_paused: bool
    """
    if dag_ids:
        for dag_id in dag_ids:
            try:
                logger.info(f"{'Pausing' if is_paused else 'Unpausing'} DAG: {dag_id}")
                pause_unpause_dag(dag_id, is_paused)
            except Exception as e:
                logger.error(f"Failed to {'pause' if is_paused else 'unpause'} DAG {dag_id}: {e}")
                # Continue with other DAGs even if one fails
    else:
        logger.info("No DAG IDs provided for pause/unpause operation.")
