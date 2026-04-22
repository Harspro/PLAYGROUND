"""
Unit tests for reversal_search_tool.handlers module.

Tests handler classes (EmailHandler, NameHandler, AddressHandler, CombinedHandler)
and helper functions (escape_sql_string, build_email_where_condition).
"""

import pytest
from reversal_search_tool.handlers import (
    EmailHandler,
    NameHandler,
    AddressHandler,
    CombinedHandler,
    INPUT_TYPE_REGISTRY,
    escape_sql_string,
    build_email_where_condition
)


class TestEscapeSqlString:
    """Tests for escape_sql_string helper function."""

    def test_escape_sql_string_normal(self):
        """Test escaping normal string."""
        result = escape_sql_string("test")
        assert result == "test"

    def test_escape_sql_string_with_single_quote(self):
        """Test escaping string with single quote."""
        result = escape_sql_string("test'value")
        assert result == "test''value"

    def test_escape_sql_string_multiple_quotes(self):
        """Test escaping string with multiple single quotes."""
        result = escape_sql_string("test'value'another")
        assert result == "test''value''another"

    def test_escape_sql_string_empty(self):
        """Test escaping empty string."""
        result = escape_sql_string("")
        assert result == ""

    def test_escape_sql_string_none(self):
        """Test escaping None value."""
        result = escape_sql_string(None)
        assert result == ""


class TestBuildEmailWhereCondition:
    """Tests for build_email_where_condition helper function."""

    def test_build_email_where_condition_normal(self):
        """Test building WHERE condition for normal email."""
        result = build_email_where_condition("test@example.com")
        assert "LOWER(ec.EMAIL_ADDRESS) = LOWER" in result
        assert "test@example.com" in result

    def test_build_email_where_condition_with_quote(self):
        """Test building WHERE condition for email with quote."""
        result = build_email_where_condition("test'@example.com")
        assert "test''@example.com" in result


class TestEmailHandler:
    """Tests for EmailHandler class."""

    def test_validate_input_success(self):
        """Test successful validation."""
        dag_run_conf = {'email': 'test@example.com'}
        # Should not raise
        EmailHandler.validate_input(dag_run_conf)

    def test_validate_input_missing_email(self):
        """Test validation fails when email is missing."""
        dag_run_conf = {}
        with pytest.raises(ValueError, match="Missing required field: 'email'"):
            EmailHandler.validate_input(dag_run_conf)

    def test_validate_input_email_not_string(self):
        """Test validation fails when email is not a string."""
        dag_run_conf = {'email': 123}
        with pytest.raises(ValueError, match="Email must be a string"):
            EmailHandler.validate_input(dag_run_conf)

    def test_validate_input_empty_email(self):
        """Test validation fails when email is empty."""
        dag_run_conf = {'email': ''}
        with pytest.raises(ValueError, match="Email cannot be empty"):
            EmailHandler.validate_input(dag_run_conf)

    def test_validate_input_whitespace_only_email(self):
        """Test validation fails when email is whitespace only."""
        dag_run_conf = {'email': '   '}
        with pytest.raises(ValueError, match="Email cannot be empty"):
            EmailHandler.validate_input(dag_run_conf)

    def test_mask_input(self):
        """Test masking email input."""
        dag_run_conf = {'email': 'customer@example.com'}
        result = EmailHandler.mask_input(dag_run_conf)
        assert result == "cu***@ex***.com"

    def test_build_query(self):
        """Test building SQL query."""
        dag_run_conf = {'email': 'test@example.com'}
        project_id = 'test-project'
        query = EmailHandler.build_query(dag_run_conf, project_id)

        assert 'test-project' in query
        assert 'EMAIL_CONTACT' in query
        assert 'test@example.com' in query
        assert 'SELECT DISTINCT' in query
        assert 'AccountUID' in query

    def test_build_query_escapes_sql(self):
        """Test that query building escapes SQL injection attempts."""
        dag_run_conf = {'email': "test'@example.com"}
        project_id = 'test-project'
        query = EmailHandler.build_query(dag_run_conf, project_id)

        # Should escape the single quote
        assert "test''@example.com" in query
        assert "test'@example.com" not in query or "test''@example.com" in query

    def test_get_log_message(self):
        """Test getting log message."""
        dag_run_conf = {'email': 'customer@example.com'}
        result = EmailHandler.get_log_message(dag_run_conf)
        assert "Searching by email:" in result
        assert "cu***@ex***.com" in result


class TestNameHandler:
    """Tests for NameHandler class."""

    def test_validate_input_success(self):
        """Test successful validation."""
        dag_run_conf = {'given_name': 'FirstName', 'surname': 'LastName'}
        # Should not raise
        NameHandler.validate_input(dag_run_conf)

    def test_validate_input_missing_given_name(self):
        """Test validation fails when given_name is missing."""
        dag_run_conf = {'surname': 'LastName'}
        with pytest.raises(ValueError, match="Missing required field: 'given_name'"):
            NameHandler.validate_input(dag_run_conf)

    def test_validate_input_missing_surname(self):
        """Test validation fails when surname is missing."""
        dag_run_conf = {'given_name': 'FirstName'}
        with pytest.raises(ValueError, match="Missing required field: 'surname'"):
            NameHandler.validate_input(dag_run_conf)

    def test_validate_input_empty_given_name(self):
        """Test validation fails when given_name is empty."""
        dag_run_conf = {'given_name': '', 'surname': 'LastName'}
        with pytest.raises(ValueError, match="Given name and surname cannot be empty"):
            NameHandler.validate_input(dag_run_conf)

    def test_validate_input_empty_surname(self):
        """Test validation fails when surname is empty."""
        dag_run_conf = {'given_name': 'FirstName', 'surname': ''}
        with pytest.raises(ValueError, match="Given name and surname cannot be empty"):
            NameHandler.validate_input(dag_run_conf)

    def test_mask_input(self):
        """Test masking name input."""
        dag_run_conf = {'given_name': 'FirstName', 'surname': 'LastName'}
        result = NameHandler.mask_input(dag_run_conf)
        assert result == "Fi*** La***"

    def test_build_query(self):
        """Test building SQL query."""
        dag_run_conf = {'given_name': 'FirstName', 'surname': 'LastName'}
        project_id = 'test-project'
        query = NameHandler.build_query(dag_run_conf, project_id)

        assert 'test-project' in query
        assert 'CUSTOMER' in query
        assert 'GIVEN_NAME' in query
        assert 'SURNAME' in query
        assert 'FirstName' in query
        assert 'LastName' in query

    def test_get_log_message(self):
        """Test getting log message."""
        dag_run_conf = {'given_name': 'FirstName', 'surname': 'LastName'}
        result = NameHandler.get_log_message(dag_run_conf)
        assert "Searching by name:" in result
        assert "Fi*** La***" in result


class TestAddressHandler:
    """Tests for AddressHandler class."""

    def test_validate_input_success(self):
        """Test successful validation."""
        dag_run_conf = {
            'address_line_1': '123 Main St',
            'city': 'Toronto',
            'postal_code': 'M5H 2N2'
        }
        # Should not raise
        AddressHandler.validate_input(dag_run_conf)

    def test_validate_input_missing_address_line_1(self):
        """Test validation fails when address_line_1 is missing."""
        dag_run_conf = {'city': 'Toronto', 'postal_code': 'M5H 2N2'}
        with pytest.raises(ValueError, match="Missing required field: 'address_line_1'"):
            AddressHandler.validate_input(dag_run_conf)

    def test_validate_input_missing_city(self):
        """Test validation fails when city is missing."""
        dag_run_conf = {'address_line_1': '123 Main St', 'postal_code': 'M5H 2N2'}
        with pytest.raises(ValueError, match="Missing required field: 'city'"):
            AddressHandler.validate_input(dag_run_conf)

    def test_validate_input_missing_postal_code(self):
        """Test validation fails when postal_code is missing."""
        dag_run_conf = {'address_line_1': '123 Main St', 'city': 'Toronto'}
        with pytest.raises(ValueError, match="Missing required field: 'postal_code'"):
            AddressHandler.validate_input(dag_run_conf)

    def test_validate_input_empty_address_line_1(self):
        """Test validation fails when address_line_1 is empty."""
        dag_run_conf = {'address_line_1': '', 'city': 'Toronto', 'postal_code': 'M5H 2N2'}
        with pytest.raises(ValueError, match="'address_line_1' cannot be empty"):
            AddressHandler.validate_input(dag_run_conf)

    def test_mask_input(self):
        """Test masking address input."""
        dag_run_conf = {
            'address_line_1': '123 Main Street',
            'city': 'Toronto',
            'postal_code': 'M5H 2N2'
        }
        result = AddressHandler.mask_input(dag_run_conf)
        assert "12*** Ma*** St***" in result
        assert "To***" in result
        assert "M5***" in result

    def test_build_query(self):
        """Test building SQL query."""
        dag_run_conf = {
            'address_line_1': '123 Main St',
            'city': 'Toronto',
            'postal_code': 'M5H 2N2'
        }
        project_id = 'test-project'
        query = AddressHandler.build_query(dag_run_conf, project_id)

        assert 'test-project' in query
        assert 'ADDRESS_CONTACT' in query
        assert 'ADDRESS_LINE_1' in query
        assert 'CITY' in query
        assert 'POSTAL_ZIP_CODE' in query

    def test_get_log_message(self):
        """Test getting log message."""
        dag_run_conf = {
            'address_line_1': '123 Main St',
            'city': 'Toronto',
            'postal_code': 'M5H 2N2'
        }
        result = AddressHandler.get_log_message(dag_run_conf)
        assert "Searching by address:" in result


class TestCombinedHandler:
    """Tests for CombinedHandler class."""

    def test_validate_input_email_only(self):
        """Test validation with email only."""
        dag_run_conf = {'email': 'test@example.com'}
        # Should not raise
        CombinedHandler.validate_input(dag_run_conf)

    def test_validate_input_name_only(self):
        """Test validation with name only."""
        dag_run_conf = {'given_name': 'FirstName', 'surname': 'LastName'}
        # Should not raise
        CombinedHandler.validate_input(dag_run_conf)

    def test_validate_input_address_only(self):
        """Test validation with address only."""
        dag_run_conf = {
            'address_line_1': '123 Main St',
            'city': 'Toronto',
            'postal_code': 'M5H 2N2'
        }
        # Should not raise
        CombinedHandler.validate_input(dag_run_conf)

    def test_validate_input_all_three(self):
        """Test validation with all three input types."""
        dag_run_conf = {
            'email': 'test@example.com',
            'given_name': 'FirstName',
            'surname': 'LastName',
            'address_line_1': '123 Main St',
            'city': 'Toronto',
            'postal_code': 'M5H 2N2'
        }
        # Should not raise
        CombinedHandler.validate_input(dag_run_conf)

    def test_validate_input_none_provided(self):
        """Test validation fails when no input types provided."""
        dag_run_conf = {}
        with pytest.raises(ValueError, match="At least one input type"):
            CombinedHandler.validate_input(dag_run_conf)

    def test_validate_input_invalid_email(self):
        """Test validation fails when email is invalid."""
        dag_run_conf = {'email': 123, 'given_name': 'FirstName', 'surname': 'LastName'}
        with pytest.raises(ValueError, match="Email must be a string"):
            CombinedHandler.validate_input(dag_run_conf)

    def test_validate_input_incomplete_name(self):
        """Test validation fails when name is incomplete."""
        dag_run_conf = {'email': 'test@example.com', 'given_name': 'FirstName'}
        with pytest.raises(ValueError, match="Given name and surname cannot be empty"):
            CombinedHandler.validate_input(dag_run_conf)

    def test_validate_input_incomplete_address(self):
        """Test validation fails when address is incomplete."""
        dag_run_conf = {'email': 'test@example.com', 'address_line_1': '123 Main St'}
        with pytest.raises(ValueError, match="'city' cannot be empty"):
            CombinedHandler.validate_input(dag_run_conf)

    def test_mask_input_email_only(self):
        """Test masking with email only."""
        dag_run_conf = {'email': 'customer@example.com'}
        result = CombinedHandler.mask_input(dag_run_conf)
        assert "email:" in result
        assert "cu***@ex***.com" in result

    def test_mask_input_all_three(self):
        """Test masking with all three input types."""
        dag_run_conf = {
            'email': 'customer@example.com',
            'given_name': 'FirstName',
            'surname': 'LastName',
            'address_line_1': '123 Main St',
            'city': 'Toronto',
            'postal_code': 'M5H 2N2'
        }
        result = CombinedHandler.mask_input(dag_run_conf)
        assert "email:" in result
        assert "name:" in result
        assert "address:" in result

    def test_build_query_email_only(self):
        """Test building query with email only."""
        dag_run_conf = {'email': 'test@example.com'}
        project_id = 'test-project'
        query = CombinedHandler.build_query(dag_run_conf, project_id)

        assert 'EMAIL_CONTACT' in query
        assert 'test@example.com' in query

    def test_build_query_name_only(self):
        """Test building query with name only."""
        dag_run_conf = {'given_name': 'FirstName', 'surname': 'LastName'}
        project_id = 'test-project'
        query = CombinedHandler.build_query(dag_run_conf, project_id)

        assert 'CUSTOMER' in query
        assert 'GIVEN_NAME' in query
        assert 'SURNAME' in query
        # Should not have contact table joins
        assert 'CONTACT' not in query

    def test_build_query_address_only(self):
        """Test building query with address only."""
        dag_run_conf = {
            'address_line_1': '123 Main St',
            'city': 'Toronto',
            'postal_code': 'M5H 2N2'
        }
        project_id = 'test-project'
        query = CombinedHandler.build_query(dag_run_conf, project_id)

        assert 'ADDRESS_CONTACT' in query
        assert 'ADDRESS_LINE_1' in query

    def test_build_query_email_and_address(self):
        """Test building query with email and address."""
        dag_run_conf = {
            'email': 'test@example.com',
            'address_line_1': '123 Main St',
            'city': 'Toronto',
            'postal_code': 'M5H 2N2'
        }
        project_id = 'test-project'
        query = CombinedHandler.build_query(dag_run_conf, project_id)

        assert 'EMAIL_CONTACT' in query
        assert 'ADDRESS_CONTACT' in query
        assert 'test@example.com' in query
        assert 'ADDRESS_LINE_1' in query

    def test_build_query_all_three(self):
        """Test building query with all three input types."""
        dag_run_conf = {
            'email': 'test@example.com',
            'given_name': 'FirstName',
            'surname': 'LastName',
            'address_line_1': '123 Main St',
            'city': 'Toronto',
            'postal_code': 'M5H 2N2'
        }
        project_id = 'test-project'
        query = CombinedHandler.build_query(dag_run_conf, project_id)

        assert 'EMAIL_CONTACT' in query
        assert 'ADDRESS_CONTACT' in query
        assert 'GIVEN_NAME' in query
        assert 'SURNAME' in query

    def test_get_log_message(self):
        """Test getting log message."""
        dag_run_conf = {
            'email': 'customer@example.com',
            'given_name': 'FirstName',
            'surname': 'LastName'
        }
        result = CombinedHandler.get_log_message(dag_run_conf)
        assert "Searching by combined inputs:" in result
        assert "email:" in result
        assert "name:" in result


class TestInputTypeRegistry:
    """Tests for INPUT_TYPE_REGISTRY."""

    def test_registry_contains_all_handlers(self):
        """Test that registry contains all expected handlers."""
        assert 'email' in INPUT_TYPE_REGISTRY
        assert 'name' in INPUT_TYPE_REGISTRY
        assert 'address' in INPUT_TYPE_REGISTRY
        assert 'combined' in INPUT_TYPE_REGISTRY

    def test_registry_handler_types(self):
        """Test that registry contains correct handler classes."""
        assert INPUT_TYPE_REGISTRY['email'] == EmailHandler
        assert INPUT_TYPE_REGISTRY['name'] == NameHandler
        assert INPUT_TYPE_REGISTRY['address'] == AddressHandler
        assert INPUT_TYPE_REGISTRY['combined'] == CombinedHandler
