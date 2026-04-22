"""
Versioning utilities for BigQuery Table Management framework.

This module enforces strict schema file naming and version control rules
for schema management operations.

Naming Convention:
   V<version>__<OPERATION>.sql or .yaml

Examples:
   V1__CREATE_TABLE.sql
   V2__ALTER_ADD_COLUMN.sql
   V3__DROP_TABLE.sql
   V4__CLUSTER_BY_ID.yaml (continues version sequence from SQL files)

Rules:
   - 'V' must be uppercase.
   - Version must be a positive integer.
   - Double underscore '__' required.
   - Operation must be uppercase.
   - Extension '.sql' or '.yaml' (case-insensitive).
"""

import re
from pathlib import Path
from airflow.exceptions import AirflowException

# ============================================================
# CONFIGURATION
# ============================================================

# Extensions allowed for versioned schema files (future-proof: add .yaml, etc.)
ALLOWED_SCHEMA_EXTENSIONS = (".sql", ".yaml")

# Strict pattern:
#   V<number>__OPERATION.sql or .yaml
# Extension cases are explicit (sql|SQL|yaml|YAML)
VERSION_PATTERN = r"^V(\d+)__([A-Z0-9_]+)\.(sql|SQL|yaml|YAML)$"

# Allowed primary operations
ALLOWED_OPERATIONS = {"CREATE", "ALTER", "DROP"}


# ============================================================
# VERSION EXTRACTION (RUNTIME AND PR LEVEL)
# ============================================================


def extract_version_from_filename(filename: str) -> int:
    """
    Extract version number from schema or YAML config filename.

    Expected format:
        V<version>__OPERATION.sql
        V<version>__OPERATION.yaml

    Examples:
        V1__CREATE_TABLE.sql -> 1
        V12__ALTER_ADD_COLUMN.sql -> 12
        V5__CLUSTER_BY_ID.yaml -> 5

    Args:
        filename (str):
            Schema file name or full path (SQL or YAML).

    Returns:
        int:
            Extracted version number.

    Raises:
        AirflowException:
            If filename does not follow strict naming convention
            or version is invalid.
    """
    name = Path(filename).name

    match = re.match(VERSION_PATTERN, name)

    if not match:
        raise AirflowException(
            f"Invalid schema filename '{name}'. "
            "Expected format: V<version>__OPERATION.sql or .yaml "
            "(uppercase V and operation required)."
        )

    version = int(match.group(1))

    if version <= 0:
        raise AirflowException(
            f"Invalid version '{version}'. " "Version must be a positive integer."
        )

    return version


# ============================================================
# NAMING VALIDATION (PR LEVEL)
# ============================================================


def validate_sql_naming_convention(filename: str) -> None:
    """
    Validate strict schema file naming convention.

    Rules:
        - Must start with uppercase 'V'
        - Must contain double underscore '__'
        - Operation must be uppercase
        - Operation must begin with allowed keyword (for SQL files only)
        - Extension must be '.sql' or '.yaml' (explicit cases: sql|SQL|yaml|YAML)
        - YAML files skip operation validation (they use content-based routing)

    Args:
        filename (str):
            Schema file name (SQL or YAML).

    Raises:
        AirflowException:
            If naming convention invalid.
    """
    name = Path(filename).name

    match = re.match(VERSION_PATTERN, name)

    if not match:
        raise AirflowException(
            f"Invalid schema filename '{name}'. "
            "Expected format: V<version>__OPERATION.sql or .yaml "
            "(uppercase V and operation required)."
        )

    operation_block = match.group(2)
    extension = match.group(3)

    # Skip operation validation for YAML files (they use content-based routing)
    if extension.lower() != "yaml":
        # Extract main operation for SQL files
        main_operation = operation_block.split("_")[0]

        if main_operation not in ALLOWED_OPERATIONS:
            raise AirflowException(
                f"Unsupported operation '{main_operation}' in '{name}'. "
                f"Allowed operations: {ALLOWED_OPERATIONS}"
            )


# ============================================================
# REPOSITORY VERSION SEQUENCE VALIDATION (PR LEVEL)
# ============================================================


def validate_repo_version_sequence(
    file_paths: list[str], dataset_table: str | None = None
) -> None:
    """
    Validate schema file version sequence across repository.

    Ensures:
        - No duplicate versions
        - Versions start from 1
        - No gaps in sequence
        - Strict incremental ordering

    Example:
        V1, V2, V3 -> Valid
        V1, V3     -> Invalid (gap)
        V1, V2, V2 -> Invalid (duplicate)

    Args:
        file_paths (list[str]):
            List of schema file paths (.sql or .yaml).
        dataset_table (str | None):
            Optional "dataset/table" folder path for error context.

    Raises:
        AirflowException:
            If version sequence invalid.
    """
    version_to_files: dict[int, list[str]] = {}

    for path in file_paths:
        version = extract_version_from_filename(path)
        name = Path(path).name
        version_to_files.setdefault(version, []).append(name)

    versions_sorted = sorted(version_to_files.keys())

    prefix = f"[{dataset_table}] " if dataset_table else ""

    # Check duplicates
    duplicates = {v: files for v, files in version_to_files.items() if len(files) > 1}
    if duplicates:
        v, files = next(iter(sorted(duplicates.items())))
        raise AirflowException(f"{prefix}Duplicate version V{v}: {', '.join(files)}")

    # Check strict incremental sequence
    for expected_version, actual_version in enumerate(versions_sorted, start=1):
        if actual_version != expected_version:
            raise AirflowException(
                f"{prefix}Version sequence invalid: expected V{expected_version}, found V{actual_version}"
            )


# ============================================================
# RUNTIME VERSION ENFORCEMENT (AUDIT LEVEL)
# ============================================================


def validate_runtime_version_increment(
    new_version: int,
    last_applied_version: int | None,
) -> None:
    """
    Validate runtime version increment against audit table.

    Rules:
        - If no previous version exists, new_version must be 1.
        - Otherwise, new_version must equal last_applied_version + 1.

    Args:
        new_version (int):
            Version extracted from SQL file.
        last_applied_version (int | None):
            Latest version recorded in audit table.
            None if table has no history.

    Raises:
        AirflowException:
            If version increment invalid.
    """
    if last_applied_version is None:
        if new_version != 1:
            raise AirflowException(
                f"Invalid initial version V{new_version}. "
                "First deployment must start with V1."
            )
        return

    if new_version <= last_applied_version:
        raise AirflowException(
            f"Version {new_version} already applied. "
            f"Latest version is {last_applied_version}."
        )

    expected_version = last_applied_version + 1

    if new_version != expected_version:
        raise AirflowException(
            f"Version mismatch detected. "
            f"Expected V{expected_version}, found V{new_version}."
        )
