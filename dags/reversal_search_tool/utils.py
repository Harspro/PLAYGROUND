"""
Utility functions for Reversal Search Tool.

This module provides:
- PII masking functions (email, name, address) for secure logging
- Result formatting functions to display query results as readable tables
"""

from typing import List, Dict, Any


# ============================================================================
# PII MASKING FUNCTIONS
# ============================================================================

def mask_email(email: str) -> str:
    """
    Mask email address for secure logging (PII protection).

    Masks local part (first 2 chars) and domain (first 2 chars + TLD visible).

    Args:
        email: Email address string

    Returns:
        Masked email string (e.g., "customer@example.com" -> "cu***@ex***.com")
        Returns "***" if email is invalid or empty
    """
    if not email or "@" not in email:
        return "***"

    local, domain = email.split("@", 1)
    masked_local = local[:2] + "***" if len(local) > 2 else "***"

    if "." in domain:
        domain_parts = domain.split(".")
        masked_domain = domain_parts[0][:2] + "***." + domain_parts[-1]
    else:
        masked_domain = domain[:2] + "***"

    return f"{masked_local}@{masked_domain}"


def mask_name(name: str) -> str:
    """
    Mask name for secure logging (PII protection).

    Masks each word in the name (first 2 chars visible, rest masked).

    Args:
        name: Name string (can be first name, last name, or full name)

    Returns:
        Masked name string (e.g., "FirstName LastName" -> "Fi*** La***")
        Returns "***" if name is empty
    """
    if not name:
        return "***"

    parts = name.split()
    masked = " ".join([p[:2] + "***" if len(p) > 2 else "***" for p in parts])
    return masked


def mask_address(address: str) -> str:
    """
    Mask address for secure logging (PII protection).

    Masks each word in the address (first 2 chars visible, rest masked).

    Args:
        address: Address string

    Returns:
        Masked address string (e.g., "123 Main St" -> "12*** Ma*** St")
        Returns "***" if address is empty
    """
    if not address:
        return "***"

    parts = address.split()
    masked = " ".join([p[:2] + "***" if len(p) > 2 else "***" for p in parts])
    return masked


# ============================================================================
# RESULT FORMATTING FUNCTIONS
# ============================================================================

# Maximum number of results to display in logs
# Prevents log bloat when query returns large result sets
MAX_RESULTS_TO_DISPLAY = 100


def format_results(results: List[Dict[str, Any]], max_display: int = MAX_RESULTS_TO_DISPLAY) -> str:
    """
    Format query results as a readable table for logging.
    Limits the number of rows displayed to prevent log bloat.

    Args:
        results: List of dictionaries containing query results
        max_display: Maximum number of results to display (default: 100)

    Returns:
        Formatted string table representation of results
    """
    if not results:
        return (
            "No results found for the provided input.\n\n"
            "Possible reasons:\n"
            "- Input may be incorrect or incomplete\n"
            "- Customer may not exist in system"
        )

    total_results = len(results)
    results_to_display = results[:max_display]
    has_more = total_results > max_display

    # Define column configuration: (header_name, width)
    # Column widths accommodate both header names and expected data value lengths
    columns = [
        ('AccountUID', 12),      # 7-digit UID + padding
        ('AccountID', 15),      # 11-char account ID + padding
        ('CustomerUID', 13),     # 7-digit UID + padding
        ('CustomerID', 16),      # 13-char customer ID + padding
        ('userID', 18),          # 15-char user ID + padding
        ('ApplicationID', 15)     # 10-digit application ID + padding
    ]

    # Build header row
    header_parts = []
    for col_name, col_width in columns:
        header_parts.append(f"{col_name:<{col_width}}")
    header = "| " + " | ".join(header_parts) + " |"

    # Build separator row (matching header format exactly)
    separator_parts = []
    for col_name, col_width in columns:
        separator_parts.append("-" * col_width)
    separator = "| " + " | ".join(separator_parts) + " |"

    # Build data rows
    rows = []
    for row in results_to_display:
        # Convert values to strings, handling None and Decimal types
        # Use 'N/A' if value is None (explicit check to avoid treating 0 or '' as missing)
        field_names = ['AccountUID', 'AccountID', 'CustomerUID', 'CustomerID', 'userID', 'ApplicationID']
        values = []
        for field in field_names:
            value = row.get(field)
            values.append('N/A' if value is None else str(value))

        # Format each value with its column width
        row_parts = []
        for i, (col_name, col_width) in enumerate(columns):
            # Truncate if value is too long
            value = values[i][:col_width] if len(values[i]) > col_width else values[i]
            row_parts.append(f"{value:<{col_width}}")

        rows.append("| " + " | ".join(row_parts) + " |")

    # Build table output (header, separator, rows, and closing separator)
    table_lines = [header, separator] + rows + [separator]
    table_output = "\n" + "\n".join(table_lines)

    # Add message if there are more results
    if has_more:
        remaining = total_results - max_display
        table_output += f"\n\n... ({remaining} more result(s) not displayed. Total: {total_results} results)"
        table_output += f"\nNote: Only first {max_display} results are shown in logs to prevent log bloat."

    return table_output
