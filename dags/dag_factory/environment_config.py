import util.constants as consts

from dataclasses import dataclass
from pendulum import timezone
from pendulum.tz.timezone import Timezone
from typing import Self
from util.miscutils import read_variable_or_file


@dataclass(frozen=True)
class EnvironmentConfig:
    deploy_env: str
    storage_suffix: str
    gcp_config: dict
    dataproc_config: dict
    local_tz: Timezone

    @staticmethod
    def load() -> Self:
        gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        dataproc_config = read_variable_or_file(consts.DATAPROC_CONFIG)

        return EnvironmentConfig(
            gcp_config=gcp_config,
            dataproc_config=dataproc_config,
            deploy_env=gcp_config.get(consts.DEPLOYMENT_ENVIRONMENT_NAME),
            storage_suffix=gcp_config.get(consts.DEPLOY_ENV_STORAGE_SUFFIX),
            local_tz=timezone('America/Toronto')
        )
