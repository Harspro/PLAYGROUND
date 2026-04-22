"""
PR SQL Validation Test for BigQuery Table Management.

Does NOT use git. Scans all files under dags/bigquery_table_management/schemas/
and validates each one. Works locally and in CI (no git dependency).

Validates:
   1. Schema file naming (V<n>__OPERATION.sql or .yaml)
   2. Safe DDL rules (SQL only)
   3. Table reference structure (SQL only)
   4. Full version sequence integrity per table (SQL + YAML)
"""

import logging
from pathlib import Path
from collections import defaultdict
from typing import Dict, List

from airflow.exceptions import AirflowException

from bigquery_table_management.shared.validation import (
    validate_is_safe_ddl,
    validate_table_ref_structure,
)

from bigquery_table_management.shared.versioning import (
    ALLOWED_SCHEMA_EXTENSIONS,
    validate_repo_version_sequence,
    validate_sql_naming_convention,
)

# ==========================================================
# LOGGING CONFIG
# ==========================================================

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setLevel(logging.INFO)
    logger.addHandler(_handler)


# ==========================================================
# REPOSITORY PATH RESOLUTION (Works locally + CI)
# ==========================================================

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_ROOT = REPO_ROOT / "dags/bigquery_table_management/schemas"


# ==========================================================
# SCHEMA FILE DISCOVERY
# ==========================================================

# Only these files are ignored; all others must pass naming validation
IGNORED_FILES = (".gitkeep", "README.md")


def get_all_schema_files() -> List[Path]:
    """
    Returns all files under SCHEMA_ROOT.
    """
    schema_files: List[Path] = [p for p in SCHEMA_ROOT.rglob("*") if p.is_file()]
    return sorted(schema_files)


# ==========================================================
# GROUPING LOGIC
# ==========================================================


def group_files_by_table(schema_files: List[Path]) -> Dict[Path, List[Path]]:
    """
    Groups schema files by their table directory.
    """
    grouped = defaultdict(list)
    for file in schema_files:
        grouped[file.parent].append(file)
    return dict(grouped)


# ==========================================================
# MAIN PR VALIDATION TEST
# ==========================================================


def test_pr_sql_validations():
    """
    Main PR validation test entry point.

    Scans all files under dags/bigquery_table_management/schemas/ (no git).
    Only .gitkeep and README.md are ignored. All other files must pass naming
    validation (V<n>__OPERATION.sql or .yaml) or validation fails.
    Runs in CI and locally.
    """

    if not SCHEMA_ROOT.exists():
        logger.warning(
            "Schema root does not exist: %s. Skipping validation.", SCHEMA_ROOT
        )
        return

    all_files = get_all_schema_files()
    # Only .gitkeep and README.md are ignored; all other files must pass naming validation
    schema_files = [p for p in all_files if p.name not in IGNORED_FILES]
    if not schema_files:
        logger.info("No schema files found. Skipping validation.")
        return

    logger.info("\n Starting PR SQL validation...\n")

    try:
        grouped = group_files_by_table(schema_files)

        for table_dir, table_sql_files in sorted(
            grouped.items(), key=lambda x: str(x[0])
        ):
            logger.info(f"\n Validating table directory: {table_dir}")

            # ==================================================
            # 1 Validate schema files
            # ==================================================
            for file in table_sql_files:
                logger.info(f"\n Validating file: {file.name}")

                # Naming convention (rejects non-.sql/.yaml)
                validate_sql_naming_convention(str(file))
                logger.info(" Naming validation passed")

                if file.suffix.lower() == ".sql":
                    sql_text = file.read_text()
                    validate_is_safe_ddl(sql_text)
                    logger.info(" Safe DDL validation passed")
                    validate_table_ref_structure(sql_text)
                    logger.info(" Table reference validation passed")
                elif file.suffix.lower() == ".yaml":
                    # Future: validate_yaml_schema(file)
                    logger.info(" YAML validation (placeholder)")

            # ==================================================
            # 2 Validate full version sequence
            # ==================================================
            # Only versioned schema files (.sql, .yaml) participate in sequence
            versioned_files = [
                p
                for p in table_sql_files
                if p.suffix.lower() in ALLOWED_SCHEMA_EXTENSIONS
            ]
            versioned_file_paths = sorted(str(p) for p in versioned_files)

            logger.info(
                f"\n Validating full version sequence in: {table_dir}\n"
                f"Files: {versioned_file_paths}"
            )

            # Extract dataset/table folder from path: .../schemas/{dataset}/{table}/
            try:
                rel = table_dir.relative_to(SCHEMA_ROOT)
                dataset_table = str(rel) if rel.parts else None
            except ValueError:
                dataset_table = None
            validate_repo_version_sequence(
                versioned_file_paths, dataset_table=dataset_table
            )
            logger.info(" Version sequence validation passed")

    except Exception as e:
        wrapped_error = (
            "\n====================================================\n"
            " PR SQL VALIDATION FAILED\n"
            "====================================================\n"
            f"{str(e)}\n"
            "===================================================="
        )

        logger.error(wrapped_error)
        raise AirflowException(wrapped_error) from e

    logger.info("\n PR SQL validation completed successfully.\n")
