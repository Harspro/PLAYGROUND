"""
Handler classes for Reversal Search Tool.

This module implements the registry pattern for handling different input types.
Each input type (email, name, address, combined) has a dedicated handler class
that provides validation, PII masking, and SQL query building capabilities.
"""

from typing import Dict, Any, Final
from reversal_search_tool.utils import mask_email, mask_name, mask_address


# ============================================================================
# SQL QUERY TEMPLATES
# ============================================================================

# Common SELECT clause - returns 6 identifiers used by TechOps for troubleshooting
# All queries return the same set of identifiers regardless of input type
SELECT_CLAUSE: Final[str] = """
SELECT DISTINCT
  a.ACCOUNT_UID as AccountUID,
  a.MAST_ACCOUNT_ID as AccountID,
  c.CUSTOMER_UID as CustomerUID,
  ci.CUSTOMER_IDENTIFIER_NO as CustomerID,
  pu.USER_NO as userID,
  adm.PC_MC_CUST_APPL_ID as ApplicationID
"""

# Common JOINs used across all queries
# These joins link customer to account and retrieve additional identifiers
# LEFT JOINs are used for optional fields (CustomerID, userID, ApplicationID)
COMMON_JOINS: Final[str] = """
INNER JOIN `{project_id}.domain_account_management.ACCOUNT_CUSTOMER` ac
  ON c.CUSTOMER_UID = ac.CUSTOMER_UID
INNER JOIN `{project_id}.domain_account_management.ACCOUNT` a
  ON ac.ACCOUNT_UID = a.ACCOUNT_UID
LEFT JOIN `{project_id}.domain_customer_management.CUSTOMER_IDENTIFIER` ci
  ON c.CUSTOMER_UID = ci.CUSTOMER_UID
  AND ci.TYPE = 'PCF-CUSTOMER-ID'
  AND ci.DISABLED_IND = 'N'
LEFT JOIN `{project_id}.domain_customer_management.PLATFORM_USER` pu
  ON c.PLATFORM_USER_UID = pu.PLATFORM_USER_UID
LEFT JOIN `{project_id}.domain_account_management.CIF_ACCOUNT_CURR` acc
  ON SAFE_CAST(a.MAST_ACCOUNT_ID AS NUMERIC) = acc.CIFP_ACCOUNT_ID5
LEFT JOIN `{project_id}.domain_customer_acquisition.ADM_RT_RESPONSE_UNS` adm
  ON SAFE_CAST(adm.APA_APP_NUM AS STRING) = acc.CIFP_CUSTOM_DATA_81
"""

# QUALIFY clause deduplicates records when a customer has multiple account relationships
# Orders by UPDATE_DT DESC to get the most recent account-customer relationship
# ORDER BY ensures consistent result ordering for readability
QUALIFY_ORDER_CLAUSE: Final[str] = """
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY a.ACCOUNT_UID, c.CUSTOMER_UID
  ORDER BY ac.UPDATE_DT DESC
) = 1
ORDER BY AccountID
"""

# Email query: Searches by email address (case-insensitive)
# Join path: EMAIL_CONTACT -> CONTACT -> CUSTOMER -> ACCOUNT
EMAIL_QUERY: Final[str] = SELECT_CLAUSE + """
FROM `{project_id}.domain_customer_management.EMAIL_CONTACT` ec
INNER JOIN `{project_id}.domain_customer_management.CONTACT` ct
  ON ec.CONTACT_UID = ct.CONTACT_UID
INNER JOIN `{project_id}.domain_customer_management.CUSTOMER` c
  ON ct.CUSTOMER_UID = c.CUSTOMER_UID
""" + COMMON_JOINS + """
WHERE LOWER(ec.EMAIL_ADDRESS) = LOWER('{email}')
""" + QUALIFY_ORDER_CLAUSE

# Name query: Searches by given name and surname (case-insensitive)
# Join path: CUSTOMER -> ACCOUNT (no contact table needed for name search)
NAME_QUERY: Final[str] = SELECT_CLAUSE + """
FROM `{project_id}.domain_customer_management.CUSTOMER` c
""" + COMMON_JOINS + """
WHERE LOWER(c.GIVEN_NAME) = LOWER('{given_name}')
  AND LOWER(c.SURNAME) = LOWER('{surname}')
""" + QUALIFY_ORDER_CLAUSE

# Address query: Searches by address line 1, city, and postal code
# Join path: ADDRESS_CONTACT -> CONTACT -> CUSTOMER -> ACCOUNT
# Postal code is exact match (case-sensitive), address and city are case-insensitive
ADDRESS_QUERY: Final[str] = SELECT_CLAUSE + """
FROM `{project_id}.domain_customer_management.ADDRESS_CONTACT` adc
INNER JOIN `{project_id}.domain_customer_management.CONTACT` ct
  ON adc.CONTACT_UID = ct.CONTACT_UID
INNER JOIN `{project_id}.domain_customer_management.CUSTOMER` c
  ON ct.CUSTOMER_UID = c.CUSTOMER_UID
""" + COMMON_JOINS + """
WHERE LOWER(adc.ADDRESS_LINE_1) = LOWER('{address_line_1}')
  AND LOWER(adc.CITY) = LOWER('{city}')
  AND adc.POSTAL_ZIP_CODE = '{postal_code}'
""" + QUALIFY_ORDER_CLAUSE


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def escape_sql_string(value: str) -> str:
    """
    Escape single quotes in SQL string values to prevent SQL injection.

    Args:
        value: String value to escape

    Returns:
        Escaped string (single quotes doubled) or empty string if value is None/empty
    """
    return value.replace("'", "''") if value else ""


def build_email_where_condition(email: str) -> str:
    """
    Build WHERE condition for email search (case-insensitive).

    Args:
        email: Email address string (will be escaped and lowercased)

    Returns:
        SQL WHERE condition string for email matching
    """
    email_escaped = escape_sql_string(email)
    return f"LOWER(ec.EMAIL_ADDRESS) = LOWER('{email_escaped}')"


# ============================================================================
# HANDLER CLASSES
# ============================================================================

class EmailHandler:
    """
    Handler for email input type.

    Validates email input, masks PII for logging, and builds SQL query
    to search for customer/account identifiers by email address.
    """

    @staticmethod
    def validate_input(dag_run_conf: Dict[str, Any]) -> None:
        """
        Validate that email field is present and non-empty.

        Args:
            dag_run_conf: DAG run configuration dictionary

        Raises:
            ValueError: If email field is missing, not a string, or empty
        """
        if 'email' not in dag_run_conf:
            raise ValueError("Missing required field: 'email'")

        email = dag_run_conf.get('email')
        if not isinstance(email, str):
            raise ValueError("Email must be a string")
        if not email.strip():
            raise ValueError("Email cannot be empty")

    @staticmethod
    def mask_input(dag_run_conf: Dict[str, Any]) -> str:
        """
        Mask email address for secure logging (PII protection).

        Args:
            dag_run_conf: DAG run configuration dictionary

        Returns:
            Masked email string (e.g., "cu***@ex***.com")
        """
        email = dag_run_conf.get('email', '')
        return mask_email(email)

    @staticmethod
    def build_query(dag_run_conf: Dict[str, Any], project_id: str) -> str:
        """
        Build SQL query to search by email address.

        Args:
            dag_run_conf: DAG run configuration dictionary
            project_id: BigQuery project ID (environment-specific)

        Returns:
            Complete SQL query string with email parameter substituted
        """
        email = dag_run_conf.get('email')
        email_escaped = escape_sql_string(email)
        return EMAIL_QUERY.format(project_id=project_id, email=email_escaped)

    @staticmethod
    def get_log_message(dag_run_conf: Dict[str, Any]) -> str:
        """
        Generate log message with masked email for secure logging.

        Args:
            dag_run_conf: DAG run configuration dictionary

        Returns:
            Log message string with masked PII
        """
        masked = EmailHandler.mask_input(dag_run_conf)
        return f"Searching by email: {masked}"


class NameHandler:
    """
    Handler for name input type.

    Validates given name and surname input, masks PII for logging,
    and builds SQL query to search for customer/account identifiers by name.
    """

    @staticmethod
    def validate_input(dag_run_conf: Dict[str, Any]) -> None:
        """
        Validate that both given_name and surname fields are present and non-empty.

        Args:
            dag_run_conf: DAG run configuration dictionary

        Raises:
            ValueError: If given_name or surname is missing or empty
        """
        if 'given_name' not in dag_run_conf:
            raise ValueError("Missing required field: 'given_name'")
        if 'surname' not in dag_run_conf:
            raise ValueError("Missing required field: 'surname'")
        if not dag_run_conf.get('given_name') or not dag_run_conf.get('surname'):
            raise ValueError("Given name and surname cannot be empty")

    @staticmethod
    def mask_input(dag_run_conf: Dict[str, Any]) -> str:
        """
        Mask name for secure logging (PII protection).

        Args:
            dag_run_conf: DAG run configuration dictionary

        Returns:
            Masked name string (e.g., "Jo*** Do***")
        """
        given_name = dag_run_conf.get('given_name', '')
        surname = dag_run_conf.get('surname', '')
        masked_given = mask_name(given_name)
        masked_surname = mask_name(surname)
        return f"{masked_given} {masked_surname}"

    @staticmethod
    def build_query(dag_run_conf: Dict[str, Any], project_id: str) -> str:
        """
        Build SQL query to search by given name and surname.

        Args:
            dag_run_conf: DAG run configuration dictionary
            project_id: BigQuery project ID (environment-specific)

        Returns:
            Complete SQL query string with name parameters substituted
        """
        given_name = escape_sql_string(dag_run_conf.get('given_name', ''))
        surname = escape_sql_string(dag_run_conf.get('surname', ''))
        return NAME_QUERY.format(
            project_id=project_id,
            given_name=given_name,
            surname=surname
        )

    @staticmethod
    def get_log_message(dag_run_conf: Dict[str, Any]) -> str:
        """
        Generate log message with masked name for secure logging.

        Args:
            dag_run_conf: DAG run configuration dictionary

        Returns:
            Log message string with masked PII
        """
        masked = NameHandler.mask_input(dag_run_conf)
        return f"Searching by name: {masked}"


class AddressHandler:
    """
    Handler for address input type.

    Validates address components (address_line_1, city, postal_code),
    masks PII for logging, and builds SQL query to search by address.
    """

    @staticmethod
    def validate_input(dag_run_conf: Dict[str, Any]) -> None:
        """
        Validate that all required address fields are present and non-empty.

        Args:
            dag_run_conf: DAG run configuration dictionary

        Raises:
            ValueError: If any required address field is missing or empty
        """
        required_fields = ['address_line_1', 'city', 'postal_code']
        for field in required_fields:
            if field not in dag_run_conf:
                raise ValueError(f"Missing required field: '{field}'")
            if not dag_run_conf.get(field):
                raise ValueError(f"'{field}' cannot be empty")

    @staticmethod
    def mask_input(dag_run_conf: Dict[str, Any]) -> str:
        """
        Mask address for secure logging (PII protection).

        Args:
            dag_run_conf: DAG run configuration dictionary

        Returns:
            Masked address string (e.g., "12*** Ma*** St, To***, M5***")
        """
        address_line_1 = mask_address(dag_run_conf.get('address_line_1', ''))
        city = mask_address(dag_run_conf.get('city', ''))
        postal_code = dag_run_conf.get('postal_code', '')
        masked_postal = postal_code[:2] + "***" if len(postal_code) > 2 else "***"
        return f"{address_line_1}, {city}, {masked_postal}"

    @staticmethod
    def build_query(dag_run_conf: Dict[str, Any], project_id: str) -> str:
        """
        Build SQL query to search by address (address_line_1, city, postal_code).

        Args:
            dag_run_conf: DAG run configuration dictionary
            project_id: BigQuery project ID (environment-specific)

        Returns:
            Complete SQL query string with address parameters substituted
        """
        address_line_1 = escape_sql_string(dag_run_conf.get('address_line_1', ''))
        city = escape_sql_string(dag_run_conf.get('city', ''))
        postal_code = escape_sql_string(dag_run_conf.get('postal_code', ''))
        return ADDRESS_QUERY.format(
            project_id=project_id,
            address_line_1=address_line_1,
            city=city,
            postal_code=postal_code
        )

    @staticmethod
    def get_log_message(dag_run_conf: Dict[str, Any]) -> str:
        """
        Generate log message with masked address for secure logging.

        Args:
            dag_run_conf: DAG run configuration dictionary

        Returns:
            Log message string with masked PII
        """
        masked = AddressHandler.mask_input(dag_run_conf)
        return f"Searching by address: {masked}"


class CombinedHandler:
    """
    Handler for combined input types.

    Supports searching with multiple input types simultaneously:
    - Email + Name
    - Email + Address
    - Name + Address
    - Email + Name + Address

    All provided inputs are combined with AND logic (all must match).
    """

    @staticmethod
    def validate_input(dag_run_conf: Dict[str, Any]) -> None:
        """
        Validate that at least one input type is provided with all required fields.

        Args:
            dag_run_conf: DAG run configuration dictionary

        Raises:
            ValueError: If no input types provided, or if provided inputs are invalid
        """
        has_email = 'email' in dag_run_conf and dag_run_conf.get('email')
        has_name = 'given_name' in dag_run_conf and 'surname' in dag_run_conf
        has_address = all(field in dag_run_conf for field in ['address_line_1', 'city', 'postal_code'])

        # Check for incomplete name (given_name without surname or vice versa)
        has_partial_name = ('given_name' in dag_run_conf) != ('surname' in dag_run_conf)
        if has_partial_name:
            raise ValueError("Given name and surname cannot be empty")

        # Check for incomplete address (some but not all address fields)
        address_fields = ['address_line_1', 'city', 'postal_code']
        has_partial_address = any(field in dag_run_conf for field in address_fields) and not has_address
        if has_partial_address:
            missing_fields = [field for field in address_fields if field not in dag_run_conf or not dag_run_conf.get(field)]
            if missing_fields:
                raise ValueError(f"'{missing_fields[0]}' cannot be empty")

        if not (has_email or has_name or has_address):
            raise ValueError("At least one input type (email, name, or address) must be provided")

        # Validate email if provided
        if has_email:
            email = dag_run_conf.get('email')
            if not isinstance(email, str):
                raise ValueError("Email must be a string")
            if not email.strip():
                raise ValueError("Email cannot be empty")

        # Validate name if provided
        if has_name:
            if not dag_run_conf.get('given_name') or not dag_run_conf.get('surname'):
                raise ValueError("Given name and surname cannot be empty")

        # Validate address if provided
        if has_address:
            for field in ['address_line_1', 'city', 'postal_code']:
                if not dag_run_conf.get(field):
                    raise ValueError(f"'{field}' cannot be empty")

    @staticmethod
    def mask_input(dag_run_conf: Dict[str, Any]) -> str:
        """
        Mask all provided inputs for secure logging (PII protection).

        Args:
            dag_run_conf: DAG run configuration dictionary

        Returns:
            Comma-separated string of masked inputs (e.g., "email: cu***@ex***.com, name: Jo*** Do***")
        """
        masked_parts = []

        if 'email' in dag_run_conf and dag_run_conf.get('email'):
            masked_parts.append(f"email: {EmailHandler.mask_input(dag_run_conf)}")

        if 'given_name' in dag_run_conf and 'surname' in dag_run_conf:
            masked_parts.append(f"name: {NameHandler.mask_input(dag_run_conf)}")

        if all(field in dag_run_conf for field in ['address_line_1', 'city', 'postal_code']):
            masked_parts.append(f"address: {AddressHandler.mask_input(dag_run_conf)}")

        return ", ".join(masked_parts)

    @staticmethod
    def build_query(dag_run_conf: Dict[str, Any], project_id: str) -> str:
        """
        Build SQL query combining multiple input types with AND logic.

        The FROM clause is determined by which inputs are provided:
        - Email + Address: Start from CUSTOMER, join email and address through
          separate CONTACT records (allows email and address on different contacts)
        - Email only: Start from EMAIL_CONTACT
        - Address only: Start from CUSTOMER, join to ADDRESS_CONTACT
        - Name only: Start from CUSTOMER (no contact table needed)

        Args:
            dag_run_conf: DAG run configuration dictionary
            project_id: BigQuery project ID (environment-specific)

        Returns:
            Complete SQL query string with all provided input parameters substituted
        """
        has_email = 'email' in dag_run_conf and dag_run_conf.get('email')
        has_name = 'given_name' in dag_run_conf and 'surname' in dag_run_conf
        has_address = all(field in dag_run_conf for field in ['address_line_1', 'city', 'postal_code'])

        # Build FROM clause based on which inputs are provided
        if has_email and has_address:
            # Email and address can be on different contact records for the same customer
            # Join through CUSTOMER to allow this flexibility
            from_clause = f"""
FROM `{project_id}.domain_customer_management.CUSTOMER` c
INNER JOIN `{project_id}.domain_customer_management.CONTACT` ct_email
  ON c.CUSTOMER_UID = ct_email.CUSTOMER_UID
INNER JOIN `{project_id}.domain_customer_management.EMAIL_CONTACT` ec
  ON ec.CONTACT_UID = ct_email.CONTACT_UID
INNER JOIN `{project_id}.domain_customer_management.CONTACT` ct_address
  ON c.CUSTOMER_UID = ct_address.CUSTOMER_UID
INNER JOIN `{project_id}.domain_customer_management.ADDRESS_CONTACT` adc
  ON adc.CONTACT_UID = ct_address.CONTACT_UID
"""
        elif has_email:
            from_clause = f"""
FROM `{project_id}.domain_customer_management.EMAIL_CONTACT` ec
INNER JOIN `{project_id}.domain_customer_management.CONTACT` ct
  ON ec.CONTACT_UID = ct.CONTACT_UID
INNER JOIN `{project_id}.domain_customer_management.CUSTOMER` c
  ON ct.CUSTOMER_UID = c.CUSTOMER_UID
"""
        elif has_address:
            from_clause = f"""
FROM `{project_id}.domain_customer_management.CUSTOMER` c
INNER JOIN `{project_id}.domain_customer_management.CONTACT` ct
  ON c.CUSTOMER_UID = ct.CUSTOMER_UID
INNER JOIN `{project_id}.domain_customer_management.ADDRESS_CONTACT` adc
  ON adc.CONTACT_UID = ct.CONTACT_UID
"""
        else:
            # Name only - no contact table joins needed
            from_clause = f"""
FROM `{project_id}.domain_customer_management.CUSTOMER` c
"""

        # Build WHERE conditions - combine all provided inputs with AND logic
        where_conditions = []

        if has_email:
            email_condition = build_email_where_condition(dag_run_conf.get('email'))
            if email_condition:
                where_conditions.append(email_condition)

        if has_name:
            given_name = escape_sql_string(dag_run_conf.get('given_name', ''))
            surname = escape_sql_string(dag_run_conf.get('surname', ''))
            where_conditions.append(f"LOWER(c.GIVEN_NAME) = LOWER('{given_name}')")
            where_conditions.append(f"LOWER(c.SURNAME) = LOWER('{surname}')")

        if has_address:
            address_line_1 = escape_sql_string(dag_run_conf.get('address_line_1', ''))
            city = escape_sql_string(dag_run_conf.get('city', ''))
            postal_code = escape_sql_string(dag_run_conf.get('postal_code', ''))
            where_conditions.append(f"LOWER(adc.ADDRESS_LINE_1) = LOWER('{address_line_1}')")
            where_conditions.append(f"LOWER(adc.CITY) = LOWER('{city}')")
            where_conditions.append(f"adc.POSTAL_ZIP_CODE = '{postal_code}'")

        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

        # Build complete query
        common_joins_formatted = COMMON_JOINS.format(project_id=project_id)
        query = SELECT_CLAUSE + from_clause + common_joins_formatted + where_clause + QUALIFY_ORDER_CLAUSE
        return query

    @staticmethod
    def get_log_message(dag_run_conf: Dict[str, Any]) -> str:
        """
        Generate log message with all masked inputs for secure logging.

        Args:
            dag_run_conf: DAG run configuration dictionary

        Returns:
            Log message string with all masked PII
        """
        masked = CombinedHandler.mask_input(dag_run_conf)
        return f"Searching by combined inputs: {masked}"


# ============================================================================
# REGISTRY
# ============================================================================

# Registry pattern: Maps input_type string to handler class
# This allows dynamic handler selection without modifying core DAG logic
# To add new input types: create handler class and add entry here
INPUT_TYPE_REGISTRY = {
    'email': EmailHandler,
    'name': NameHandler,
    'address': AddressHandler,
    'combined': CombinedHandler,
}
