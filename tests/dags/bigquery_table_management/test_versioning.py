import pytest
from airflow.exceptions import AirflowException

from bigquery_table_management.shared.versioning import (
    extract_version_from_filename,
    validate_sql_naming_convention,
    validate_repo_version_sequence,
    validate_runtime_version_increment,
)

# ==========================================================
# extract_version_from_filename
# ==========================================================


@pytest.mark.parametrize(
    "filename, expected",
    [
        ("V1__CREATE_TABLE.sql", 1),
        ("V12__ALTER_ADD_COLUMN.sql", 12),
        ("/some/path/V3__DROP_TABLE.sql", 3),
        ("V10__CREATE.sql", 10),
        ("V1__CREATE_TABLE.yaml", 1),
    ],
)
def test_extract_version_valid(filename, expected):
    assert extract_version_from_filename(filename) == expected


@pytest.mark.parametrize(
    "filename",
    [
        "v1__CREATE_TABLE.sql",  # lowercase V
        "V0__CREATE_TABLE.sql",  # zero version
        "V-1__CREATE_TABLE.sql",  # negative version
        "V1_CREATE_TABLE.sql",  # single underscore
        "V1__create_table.sql",  # lowercase operation
        "V1__CREATE_TABLE.txt",  # wrong extension
        "RANDOM.sql",  # completely invalid
        "V__CREATE.sql",  # missing version number
    ],
)
def test_extract_version_invalid(filename):
    with pytest.raises(AirflowException):
        extract_version_from_filename(filename)


# ==========================================================
# validate_sql_naming_convention
# ==========================================================


def test_validate_sql_naming_valid():
    validate_sql_naming_convention("V1__CREATE_TABLE.sql")
    validate_sql_naming_convention("V2__ALTER_ADD_COLUMN.SQL")
    validate_sql_naming_convention("V3__DROP_TABLE.sql")
    validate_sql_naming_convention("V1__CREATE_TABLE.yaml")


def test_validate_sql_naming_invalid_operation():
    with pytest.raises(AirflowException):
        validate_sql_naming_convention("V1__RENAME_TABLE.sql")


def test_validate_sql_naming_lowercase_v():
    with pytest.raises(AirflowException):
        validate_sql_naming_convention("v1__CREATE_TABLE.sql")


def test_validate_sql_naming_missing_double_underscore():
    with pytest.raises(AirflowException):
        validate_sql_naming_convention("V1_CREATE_TABLE.sql")


# ==========================================================
# validate_repo_version_sequence
# ==========================================================


def test_validate_repo_sequence_valid():
    files = [
        "V1__CREATE.sql",
        "V2__ALTER.sql",
        "V3__DROP.sql",
    ]
    validate_repo_version_sequence(files)


def test_validate_repo_sequence_duplicate_versions():
    files = [
        "V1__CREATE.sql",
        "V2__ALTER.sql",
        "V2__DROP.sql",
    ]
    with pytest.raises(AirflowException):
        validate_repo_version_sequence(files)


def test_validate_repo_sequence_gap_in_versions():
    files = [
        "V1__CREATE.sql",
        "V3__ALTER.sql",
    ]
    with pytest.raises(AirflowException):
        validate_repo_version_sequence(files)


def test_validate_repo_sequence_not_starting_from_one():
    files = [
        "V2__CREATE.sql",
        "V3__ALTER.sql",
    ]
    with pytest.raises(AirflowException):
        validate_repo_version_sequence(files)


# ==========================================================
# validate_runtime_version_increment
# ==========================================================


def test_runtime_version_first_deploy_valid():
    validate_runtime_version_increment(
        new_version=1,
        last_applied_version=None,
    )


def test_runtime_version_first_deploy_invalid():
    with pytest.raises(AirflowException):
        validate_runtime_version_increment(
            new_version=2,
            last_applied_version=None,
        )


def test_runtime_version_increment_valid():
    validate_runtime_version_increment(
        new_version=3,
        last_applied_version=2,
    )


def test_runtime_version_already_applied():
    with pytest.raises(AirflowException):
        validate_runtime_version_increment(
            new_version=2,
            last_applied_version=2,
        )


def test_runtime_version_jump_detected():
    with pytest.raises(AirflowException):
        validate_runtime_version_increment(
            new_version=4,
            last_applied_version=2,
        )
