from pcmc_estatement_processing.pcb_dst_zip_extractor_file_extract import get_output_file_name_env_based
import pytest
from unittest.mock import patch

JOB_DATE_TIME = "20240402"
CYCLE_NUM_PLACEHOLDER = "{cycle}"


def test_get_output_file_name():
    dag_config = {
        "output_file_extension": {'dev': 'uatv'},
        "output_file_name": {'dev': "psf_{env_var}_pcb_dst_stmtdnp_{cycle}"},
        "output_file_env_vars": {'dev': 't'},
        "processing_output_dir": "/tmp/output"
    }
    deploy_env = "dev"
    cycle_no = 3
    dag_id = 'psf_t_pcb_dst_test_zip'

    expected_output = "/tmp/output/psf_t_pcb_dst_stmtdnp_03_20240402.uatv"

    with patch("pcmc_estatement_processing.pcb_dst_zip_extractor_file_extract.JOB_DATE_TIME", JOB_DATE_TIME), \
         patch("pcmc_estatement_processing.pcb_dst_zip_extractor_file_extract.CYCLE_NUM_PLACEHOLDER", CYCLE_NUM_PLACEHOLDER):
        result = get_output_file_name_env_based(dag_config, deploy_env, cycle_no, dag_id)
        assert result == expected_output
