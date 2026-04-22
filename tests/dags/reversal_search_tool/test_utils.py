"""
Unit tests for reversal_search_tool.utils module.

Tests PII masking functions and result formatting functions.
"""

import pytest
from reversal_search_tool.utils import (
    mask_email,
    mask_name,
    mask_address,
    format_results,
    MAX_RESULTS_TO_DISPLAY
)


class TestMaskEmail:
    """Tests for mask_email function."""

    def test_mask_email_normal(self):
        """Test masking normal email address."""
        result = mask_email("customer@example.com")
        assert result == "cu***@ex***.com"

    def test_mask_email_short_local(self):
        """Test masking email with short local part."""
        result = mask_email("ab@example.com")
        assert result == "***@ex***.com"

    def test_mask_email_single_char_local(self):
        """Test masking email with single character local part."""
        result = mask_email("a@example.com")
        assert result == "***@ex***.com"

    def test_mask_email_no_domain_dot(self):
        """Test masking email without domain dot."""
        result = mask_email("user@localhost")
        assert result == "us***@lo***"

    def test_mask_email_empty(self):
        """Test masking empty email."""
        result = mask_email("")
        assert result == "***"

    def test_mask_email_none(self):
        """Test masking None email."""
        result = mask_email(None)
        assert result == "***"

    def test_mask_email_no_at_symbol(self):
        """Test masking invalid email without @ symbol."""
        result = mask_email("notanemail")
        assert result == "***"

    def test_mask_email_complex_domain(self):
        """Test masking email with complex domain."""
        result = mask_email("user@subdomain.example.co.uk")
        assert result == "us***@su***.uk"


class TestMaskName:
    """Tests for mask_name function."""

    def test_mask_name_full_name(self):
        """Test masking full name."""
        result = mask_name("FirstName LastName")
        assert result == "Fi*** La***"

    def test_mask_name_single_name(self):
        """Test masking single name."""
        result = mask_name("FirstName")
        assert result == "Fi***"

    def test_mask_name_short_name(self):
        """Test masking short name."""
        result = mask_name("Jo")
        assert result == "***"

    def test_mask_name_single_char(self):
        """Test masking single character name."""
        result = mask_name("J")
        assert result == "***"

    def test_mask_name_multiple_words(self):
        """Test masking name with multiple words."""
        result = mask_name("FirstName MiddleName LastName")
        assert result == "Fi*** Mi*** La***"

    def test_mask_name_empty(self):
        """Test masking empty name."""
        result = mask_name("")
        assert result == "***"

    def test_mask_name_none(self):
        """Test masking None name."""
        result = mask_name(None)
        assert result == "***"

    def test_mask_name_with_extra_spaces(self):
        """Test masking name with extra spaces (split() normalizes whitespace)."""
        result = mask_name("FirstName  LastName")
        # split() removes extra spaces, so result has single space
        assert result == "Fi*** La***"


class TestMaskAddress:
    """Tests for mask_address function."""

    def test_mask_address_normal(self):
        """Test masking normal address."""
        result = mask_address("123 Main Street")
        assert result == "12*** Ma*** St***"

    def test_mask_address_short_words(self):
        """Test masking address with short words (both words <= 2 chars get masked)."""
        result = mask_address("1 St")
        # Both "1" (1 char) and "St" (2 chars) are <= 2, so both become "***"
        assert result == "*** ***"

    def test_mask_address_empty(self):
        """Test masking empty address."""
        result = mask_address("")
        assert result == "***"

    def test_mask_address_none(self):
        """Test masking None address."""
        result = mask_address(None)
        assert result == "***"

    def test_mask_address_single_word(self):
        """Test masking single word address."""
        result = mask_address("Main")
        assert result == "Ma***"


class TestFormatResults:
    """Tests for format_results function."""

    def test_format_results_empty(self):
        """Test formatting empty results."""
        result = format_results([])
        assert "No results found" in result
        assert "Input may be incorrect" in result

    def test_format_results_single_row(self):
        """Test formatting single result row."""
        results = [{
            'AccountUID': 1234567,
            'AccountID': 'ACC123456',
            'CustomerUID': 7654321,
            'CustomerID': 'CUST123456',
            'userID': 'USER123456',
            'ApplicationID': 9876543210
        }]
        output = format_results(results)
        assert "AccountUID" in output
        assert "1234567" in output
        assert "ACC123456" in output
        assert "|" in output  # Table format

    def test_format_results_multiple_rows(self):
        """Test formatting multiple result rows."""
        results = [
            {
                'AccountUID': 1234567,
                'AccountID': 'ACC123456',
                'CustomerUID': 7654321,
                'CustomerID': 'CUST123456',
                'userID': 'USER123456',
                'ApplicationID': 9876543210
            },
            {
                'AccountUID': 1234568,
                'AccountID': 'ACC123457',
                'CustomerUID': 7654322,
                'CustomerID': 'CUST123457',
                'userID': 'USER123457',
                'ApplicationID': 9876543211
            }
        ]
        output = format_results(results)
        assert "1234567" in output
        assert "1234568" in output
        assert "ACC123456" in output
        assert "ACC123457" in output

    def test_format_results_with_none_values(self):
        """Test formatting results with None values."""
        results = [{
            'AccountUID': 1234567,
            'AccountID': None,
            'CustomerUID': 7654321,
            'CustomerID': None,
            'userID': None,
            'ApplicationID': None
        }]
        output = format_results(results)
        assert "N/A" in output
        assert "1234567" in output

    def test_format_results_truncation(self):
        """Test that results are truncated at MAX_RESULTS_TO_DISPLAY."""
        # Create more results than MAX_RESULTS_TO_DISPLAY
        results = []
        for i in range(MAX_RESULTS_TO_DISPLAY + 10):
            results.append({
                'AccountUID': i,
                'AccountID': f'ACC{i}',
                'CustomerUID': i + 1000,
                'CustomerID': f'CUST{i}',
                'userID': f'USER{i}',
                'ApplicationID': i + 2000
            })

        output = format_results(results)
        # Should show truncation message
        assert "more result(s) not displayed" in output
        assert f"Total: {len(results)} results" in output
        assert f"first {MAX_RESULTS_TO_DISPLAY} results" in output

    def test_format_results_custom_max_display(self):
        """Test formatting with custom max_display parameter."""
        results = [
            {'AccountUID': i, 'AccountID': f'ACC{i}', 'CustomerUID': i,
             'CustomerID': f'CUST{i}', 'userID': f'USER{i}', 'ApplicationID': i}
            for i in range(5)
        ]
        output = format_results(results, max_display=2)
        assert "more result(s) not displayed" in output
        assert "Total: 5 results" in output

    def test_format_results_table_structure(self):
        """Test that formatted output has proper table structure."""
        results = [{
            'AccountUID': 1234567,
            'AccountID': 'ACC123456',
            'CustomerUID': 7654321,
            'CustomerID': 'CUST123456',
            'userID': 'USER123456',
            'ApplicationID': 9876543210
        }]
        output = format_results(results)

        # Check for table structure elements
        assert "|" in output  # Column separators
        assert "AccountUID" in output  # Header
        assert "AccountID" in output
        assert "-" in output  # Separator row
