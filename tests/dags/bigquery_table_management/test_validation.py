import pytest
from airflow.exceptions import AirflowException

from bigquery_table_management.shared.validation import (
    _strip_sql_comments,
    extract_table_ref,
    validate_table_ref_structure,
    validate_table_ref_match,
    validate_is_safe_ddl,
)

# ==========================================================
# _strip_sql_comments
# ==========================================================


def test_strip_sql_comments_single_line():
    sql = "CREATE TABLE project.dataset.table -- comment"
    cleaned = _strip_sql_comments(sql)
    assert "--" not in cleaned


def test_strip_sql_comments_multi_line():
    sql = """
   /* multi
      line
      comment */
   CREATE TABLE project.dataset.table
   """
    cleaned = _strip_sql_comments(sql)
    assert "multi" not in cleaned
    assert "CREATE TABLE" in cleaned


# ==========================================================
# extract_table_ref
# ==========================================================


def test_extract_table_ref_create():
    sql = "CREATE TABLE project.dataset.table (id INT64)"
    assert extract_table_ref(sql) == "project.dataset.table"


def test_extract_table_ref_alter():
    sql = "ALTER TABLE project.dataset.table ADD COLUMN name STRING"
    assert extract_table_ref(sql) == "project.dataset.table"


def test_extract_table_ref_drop_if_exists():
    sql = "DROP TABLE IF EXISTS project.dataset.table"
    assert extract_table_ref(sql) == "project.dataset.table"


def test_extract_table_ref_with_backticks():
    sql = "CREATE TABLE `project.dataset.table` (id INT64)"
    assert extract_table_ref(sql) == "project.dataset.table"


def test_extract_table_ref_invalid():
    sql = "CREATE VIEW project.dataset.table AS SELECT 1"
    with pytest.raises(AirflowException):
        extract_table_ref(sql)


# ==========================================================
# validate_table_ref_structure
# ==========================================================


def test_validate_table_ref_structure_valid():
    sql = "CREATE TABLE project.dataset.table (id INT64)"
    validate_table_ref_structure(sql)


def test_validate_table_ref_structure_missing_parts():
    sql = "CREATE TABLE dataset.table (id INT64)"
    with pytest.raises(AirflowException):
        validate_table_ref_structure(sql)


def test_validate_table_ref_structure_empty_part():
    sql = "CREATE TABLE project..table (id INT64)"
    with pytest.raises(AirflowException):
        validate_table_ref_structure(sql)


# ==========================================================
# validate_table_ref_match
# ==========================================================


def test_validate_table_ref_match_valid():
    sql = "CREATE TABLE project.dataset.table (id INT64)"
    validate_table_ref_match(sql, "project.dataset.table")


def test_validate_table_ref_match_case_insensitive():
    sql = "CREATE TABLE PROJECT.DATASET.TABLE (id INT64)"
    validate_table_ref_match(sql, "project.dataset.table")


def test_validate_table_ref_match_mismatch():
    sql = "CREATE TABLE project.dataset.table (id INT64)"
    with pytest.raises(AirflowException):
        validate_table_ref_match(sql, "project.dataset.other_table")


# ==========================================================
# validate_is_safe_ddl
# ==========================================================

# ---- VALID CASES ----


def test_safe_create_table():
    sql = "CREATE TABLE project.dataset.table (id INT64)"
    validate_is_safe_ddl(sql)


def test_safe_drop_table_if_exists():
    sql = "DROP TABLE IF EXISTS project.dataset.table"
    validate_is_safe_ddl(sql)


def test_safe_alter_add_column():
    sql = "ALTER TABLE project.dataset.table ADD COLUMN age INT64"
    validate_is_safe_ddl(sql)


def test_safe_alter_drop_column():
    sql = "ALTER TABLE project.dataset.table DROP COLUMN age"
    validate_is_safe_ddl(sql)


# ---- BLOCK MULTIPLE STATEMENTS ----


def test_block_multiple_statements():
    sql = """
   CREATE TABLE project.dataset.table (id INT64);
   DROP TABLE project.dataset.table;
   """
    with pytest.raises(AirflowException):
        validate_is_safe_ddl(sql)


# ---- BLOCK DML ----


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO project.dataset.table VALUES (1)",
        "UPDATE project.dataset.table SET id = 2",
        "DELETE FROM project.dataset.table WHERE id = 1",
        "MERGE project.dataset.table USING other",
        "SELECT * FROM project.dataset.table",
    ],
)
def test_block_dml(sql):
    with pytest.raises(AirflowException):
        validate_is_safe_ddl(sql)


# ---- BLOCK TRUNCATE ----


def test_block_truncate():
    sql = "TRUNCATE TABLE project.dataset.table"
    with pytest.raises(AirflowException):
        validate_is_safe_ddl(sql)


# ---- BLOCK DROP WITHOUT IF EXISTS ----


def test_block_drop_without_if_exists():
    sql = "DROP TABLE project.dataset.table"
    with pytest.raises(AirflowException):
        validate_is_safe_ddl(sql)


# ---- BLOCK RISKY ALTER ----


@pytest.mark.parametrize(
    "sql",
    [
        "ALTER TABLE project.dataset.table ALTER COLUMN age SET DATA TYPE STRING",
        "ALTER TABLE project.dataset.table ALTER COLUMN age SET NOT NULL",
        "ALTER TABLE project.dataset.table DROP NOT NULL",
        "ALTER TABLE project.dataset.table SET OPTIONS (...)",
        "ALTER TABLE project.dataset.table RENAME COLUMN age TO age2",
    ],
)
def test_block_risky_alter(sql):
    with pytest.raises(AirflowException):
        validate_is_safe_ddl(sql)


# ---- BLOCK PARTITION / CLUSTER IN ALTER ----


def test_block_partition_in_alter():
    sql = "ALTER TABLE project.dataset.table ADD COLUMN age INT64 PARTITION BY age"
    with pytest.raises(AirflowException):
        validate_is_safe_ddl(sql)


def test_block_cluster_in_alter():
    sql = "ALTER TABLE project.dataset.table ADD COLUMN age INT64 CLUSTER BY age"
    with pytest.raises(AirflowException):
        validate_is_safe_ddl(sql)
