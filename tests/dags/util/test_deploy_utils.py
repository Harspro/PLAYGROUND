from typing import Final, List, Tuple
from unittest.mock import patch

import pytest

from util.deploy_utils import read_feature_flag


class TestDeployUtils:
    TEST_FEATURE_FLAG_NAME: Final[str] = "feature.flag.test"
    READ_FEATURE_FLAG_TEST_CASES: Final[List[Tuple[str, str]]] = [
        (TEST_FEATURE_FLAG_NAME, 'dev'),
        (TEST_FEATURE_FLAG_NAME, 'uat'),
        (TEST_FEATURE_FLAG_NAME, 'prod')
    ]
    TEST_DEPLOY_CONFIG: Final[dict] = {
        TEST_FEATURE_FLAG_NAME: {
            "dev": True,
            "uat": True,
            "prod": True
        }
    }

    @patch("util.deploy_utils.read_yamlfile_env", return_value=TEST_DEPLOY_CONFIG)
    @pytest.mark.parametrize("feature_flag, deploy_env", READ_FEATURE_FLAG_TEST_CASES)
    def test_read_feature_flag(
            self,
            mock_read_yaml,
            feature_flag: str,
            deploy_env: str
    ):
        assert read_feature_flag(feature_flag, deploy_env)
