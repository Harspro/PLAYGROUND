from dag_factory.environment_config import EnvironmentConfig


def test_env_config_load():
    env_config = EnvironmentConfig.load()
    assert env_config.gcp_config
    assert env_config.dataproc_config
    assert env_config.local_tz
    assert env_config.deploy_env == 'dev'
    assert env_config.storage_suffix == '-dev'
