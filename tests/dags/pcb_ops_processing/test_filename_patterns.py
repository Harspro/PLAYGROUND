import re
import yaml
from pathlib import Path

test_case_config = Path(__file__).parent / 'filename_testcases.yaml'


def test_file_name_patterns() -> None:
    test_cases = {}
    with open(test_case_config, 'r') as test_case_file:
        test_cases = yaml.safe_load(test_case_file)

    valid_test_cases = test_cases.get('valid_file_names')
    assert valid_test_cases

    for test_case in valid_test_cases:
        pattern = test_case.get('filename_pattern')
        filenames = test_case.get('filenames')
        for filename in filenames:
            assert re.fullmatch(pattern, filename), f'{filename} does not match pattern {pattern}'

    invalid_test_cases = test_cases.get('invalid_file_names')
    assert invalid_test_cases

    for test_case in invalid_test_cases:
        pattern = test_case.get('filename_pattern')
        filenames = test_case.get('filenames')
        for filename in filenames:
            assert not re.fullmatch(pattern, filename), f'{filename} should not match pattern {pattern}'
