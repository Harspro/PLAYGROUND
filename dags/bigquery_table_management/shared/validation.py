import re
from airflow.exceptions import AirflowException


# ==========================================================
# HELPERS
# ==========================================================
def normalize_table_ref(table_ref: str) -> str:
    """
    Normalize a table reference string.

    Strips surrounding backticks and whitespace, and converts the value to
    lowercase to ensure consistent comparisons.

    Args:
        table_ref (str): Fully qualified table reference (e.g., `project.dataset.table`).

    Returns:
        str:
            Normalized table reference.
    """
    return table_ref.strip("`").strip().lower()


def _strip_sql_comments(sql: str) -> str:
    """
    Remove SQL comments from a query string.

    Removes:
        - Multi-line comments (/* ... */)
        - Single-line comments (-- ...)

    Args:
        sql (str): Raw SQL statement.

    Returns:
        str:
            SQL string with comments removed.
    """
    # Remove multi-line comments first
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)

    # Remove single-line comments
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)

    return sql


# ==========================================================
# TABLE REFERENCE EXTRACTION AND VALIDATION
# ==========================================================


def extract_table_ref(sql: str) -> str:
    """
    Extract fully qualified table reference from DDL SQL.

    Supports:
        - CREATE TABLE
        - ALTER TABLE
        - DROP TABLE

    Args:
        sql: SQL statement

    Returns:
        str: Extracted table reference (project.dataset.table)

    Raises:
        AirflowException: If table reference cannot be extracted.
    """
    sql = _strip_sql_comments(sql)

    pattern = (
        r"(CREATE|ALTER|DROP)\s+TABLE\s+"
        r"(?:IF\s+(?:NOT\s+)?EXISTS\s+)?"
        r"`?([^`\s]+)`?"
    )

    match = re.search(pattern, sql, re.IGNORECASE)

    if not match:
        raise AirflowException(
            "Could not extract fully qualified table reference from SQL. "
            "Check fully qualified table used in the sql script"
        )

    return match.group(2)


# ==========================================================
# PR SAFE VALIDATION
# ==========================================================


def validate_table_ref_structure(sql: str) -> None:
    """
    PR-safe validation.

    Ensures SQL contains exactly one valid fully-qualified table reference:
        project.dataset.table

    This does NOT validate environment-specific values.
    Safe to run in unit tests.

    Note: This validates SQL file table references (which must be fully qualified).
    Runtime table_ref values are always project.dataset.table after environment
    placeholder replacement and project extraction.

    Args:
        sql: SQL statement.

    Raises:
        AirflowException if structure invalid.
    """
    extracted = extract_table_ref(sql)

    parts = extracted.split(".")

    if len(parts) != 3:
        raise AirflowException(
            f"Invalid table reference structure: {extracted}. "
            "Expected format: project.dataset.table"
        )

    project, dataset, table = parts

    if not project or not dataset or not table:
        raise AirflowException(f"Incomplete table reference detected: {extracted}")


# ==========================================================
# STRICT RUNTIME VALIDATION
# ==========================================================


def validate_table_ref_match(sql: str, expected_table_ref: str) -> None:
    """
    Ensure SQL references only the expected table.

    Extracts table reference from DDL and compares it against the provided expected table reference.

    Args:
        sql (str): SQL statement.
        expected_table_ref (str): Expected fully qualified table reference.

    Raises:
        AirflowException:
            If table reference cannot be extracted or
            if the extracted table does not match the expected table.
    """
    extracted = extract_table_ref(sql)

    if normalize_table_ref(extracted) != normalize_table_ref(expected_table_ref):
        raise AirflowException(
            f"Table mismatch. SQL references {extracted}, "
            f"expected {expected_table_ref}"
        )


# ==========================================================
# STRICT DDL VALIDATION
# ==========================================================
def validate_is_safe_ddl(sql: str) -> None:
    """
    Validate that SQL statement is a safe DDL operation.

    Enforces strict schema-management rules.

    Allows:
        - CREATE TABLE
        - ALTER TABLE (ADD COLUMN, DROP COLUMN only)
        - DROP TABLE IF EXISTS

    Disallows:
        - INSERT
        - DELETE
        - UPDATE
        - TRUNCATE
        - MERGE
        - SELECT
        - Multiple SQL statements
        - Risky ALTER operations (e.g., SET DATA TYPE, SET NOT NULL)
        - PARTITION BY in ALTER
        - CLUSTER BY in ALTER
        - Any non-DDL operation

    Args:
        sql (str): SQL statement to validate.

    Returns:
        None

    Raises:
        AirflowException:
            If unsafe SQL detected.
    """

    sql = _strip_sql_comments(sql)
    sql_clean = sql.strip()
    sql_upper = sql_clean.upper()

    # --------------------------------------------------
    # Block multiple statements
    # --------------------------------------------------
    if ";" in sql_clean.rstrip(";"):
        raise AirflowException("Multiple SQL statements are not allowed.")

    # --------------------------------------------------
    # Block DML
    # --------------------------------------------------
    if re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|SELECT)\b", sql_upper):
        raise AirflowException("DML statements are not allowed in schema management.")

    # --------------------------------------------------
    # Block TRUNCATE
    # --------------------------------------------------
    if re.search(r"\bTRUNCATE\s+TABLE\b", sql_upper):
        raise AirflowException("TRUNCATE TABLE is not allowed in schema management.")

    # --------------------------------------------------
    # Must start with CREATE / ALTER / DROP TABLE
    # --------------------------------------------------
    match = re.search(r"^\s*(CREATE|ALTER|DROP)\s+TABLE", sql_upper)

    if not match:
        raise AirflowException("Only CREATE/ALTER/DROP TABLE statements are allowed.")

    statement_type = match.group(1)

    # --------------------------------------------------
    # CREATE → fully allowed
    # --------------------------------------------------
    if statement_type == "CREATE":
        return

    # --------------------------------------------------
    # DROP → only IF EXISTS allowed
    # --------------------------------------------------
    if statement_type == "DROP":
        if not re.search(r"\bDROP\s+TABLE\s+IF\s+EXISTS\b", sql_upper):
            raise AirflowException("DROP TABLE without IF EXISTS is not allowed.")
        return

    # --------------------------------------------------
    # ALTER → strictly controlled
    # --------------------------------------------------
    if statement_type == "ALTER":

        # Only ADD COLUMN / DROP COLUMN allowed
        if not re.search(r"\bADD\s+COLUMN\b", sql_upper) and not re.search(
            r"\bDROP\s+COLUMN\b", sql_upper
        ):
            raise AirflowException(
                "Only ALTER TABLE ADD COLUMN or DROP COLUMN supported. "
                "Other ALTER operations require YAML recreation."
            )

        # Risky operations
        risky_patterns = [
            r"\bALTER\s+COLUMN\b",
            r"\bSET\s+DATA\s+TYPE\b",
            r"\bSET\s+NOT\s+NULL\b",
            r"\bDROP\s+NOT\s+NULL\b",
            r"\bSET\s+OPTIONS\b",
            r"\bSET\s+DEFAULT\b",
            r"\bDROP\s+DEFAULT\b",
            r"\bRENAME\s+COLUMN\b",
            r"\bRENAME\s+TO\b",
        ]

        for pattern in risky_patterns:
            if re.search(pattern, sql_upper):
                raise AirflowException(
                    "This ALTER operation requires table recreation. "
                    "Use YAML-based recreation."
                )

        # Partition/Cluster not allowed in ALTER
        if re.search(r"\bPARTITION\s+BY\b", sql_upper):
            raise AirflowException("PARTITION BY allowed only in CREATE TABLE.")

        if re.search(r"\bCLUSTER\s+BY\b", sql_upper):
            raise AirflowException("CLUSTER BY allowed only in CREATE TABLE.")

        return
