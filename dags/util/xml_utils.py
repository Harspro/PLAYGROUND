"""
Generic XML utilities for streaming XML generation and validation.

This module provides reusable utilities for generating and validating XML files
using SAX (Simple API for XML) which writes XML incrementally without loading
the entire structure into memory.

These utilities are generic and can be used across all DAGs.
DAG-specific XML generation logic should remain in the individual DAG files.
"""

import io
from xml.sax.saxutils import XMLGenerator
from typing import Optional, Dict, Any, BinaryIO
from airflow.exceptions import AirflowFailException


class StreamingXMLWriter:
    """
    A streaming XML writer that uses SAX to write XML incrementally.

    This class provides a clean interface for writing XML documents without
    building the entire tree in memory. It handles namespaces, attributes,
    and ensures proper UTF-8 encoding.

    Supports any writable binary stream including BytesIO, file objects,
    and GCS blob streams (blob.open("wb")).

    Example usage:
        # Write to memory
        xml_stream = io.BytesIO()
        writer = StreamingXMLWriter(xml_stream)

        # Or stream directly to GCS
        with blob.open("wb") as gcs_file:
            writer = StreamingXMLWriter(gcs_file)
            writer.start_document()
            writer.start_element('root')
            writer.write_element('child', 'value')
            writer.end_element('root')
            writer.end_document()
    """

    def __init__(self, output_stream: BinaryIO, encoding: str = 'UTF-8', indent: str = '  '):
        """
        Initialize the streaming XML writer.

        Args:
            output_stream: Any writable binary stream (BytesIO, file, GCS blob stream, etc.)
            encoding: Character encoding (default: UTF-8)
            indent: Indentation string (default: two spaces)
        """
        self.output_stream = output_stream
        self.encoding = encoding
        self.indent = indent
        self.indent_level = 0
        self.generator = XMLGenerator(output_stream, encoding=encoding, short_empty_elements=False)
        self._element_has_content = []

    def start_document(self):
        """Start the XML document with proper declaration."""
        self.generator.startDocument()

    def end_document(self):
        """End the XML document."""
        self.generator.endDocument()

    def start_element(self, name: str, attrs: Optional[Dict[str, str]] = None, has_text: bool = False):
        """
        Start an XML element.

        Args:
            name: Element name (can include namespace prefix)
            attrs: Dictionary of attributes
            has_text: True if this element will contain text content (suppresses newline)
        """
        if not has_text:
            self._write_indent()

        # Convert attrs to dict if None
        if attrs is None:
            attrs = {}

        self.generator.startElement(name, attrs)
        self._element_has_content.append(has_text)

        if not has_text:
            self._write_newline()
            self.indent_level += 1

    def end_element(self, name: str):
        """
        End an XML element.

        Args:
            name: Element name (must match the corresponding start_element)
        """
        has_text = self._element_has_content.pop()

        if not has_text:
            self.indent_level -= 1
            self._write_indent()

        self.generator.endElement(name)
        self._write_newline()

    def write_element(self, name: str, text: str, attrs: Optional[Dict[str, str]] = None):
        """
        Write a complete element with text content in one call.

        Args:
            name: Element name
            text: Text content
            attrs: Dictionary of attributes (optional)
        """
        self.start_element(name, attrs, has_text=True)
        self.generator.characters(str(text))
        self.end_element(name)

    def write_field(self, element_name: str, row: Any, field_name: str, required: bool = False,
                    context: str = None, transform=None, attrs: Optional[Dict[str, str]] = None):
        """
        Convenience method to get a field value and write it as an XML element.

        Args:
            element_name: Name of the XML element to write
            row: Row object to get field from
            field_name: Name of the field to retrieve from row
            required: If True, raise AirflowFailException if value is null/empty
            context: Additional context for error messages (e.g., "NoReleve=123456789")
            transform: Optional function to transform the value (e.g., str, int, lambda x: str(int(x)))
            attrs: Dictionary of attributes (optional)

        Raises:
            AirflowFailException: If required=True and value is null/empty
        """
        value = get_field_value(row, field_name, required=required, context=context)

        # Apply transformation if provided
        if transform and value is not None:
            value = transform(value)

        # Only write if we have a value
        if value is not None:
            self.write_element(element_name, str(value), attrs)

    def _write_indent(self):
        """Write indentation for current level."""
        if self.indent_level > 0:
            indent_str = self.indent * self.indent_level
            self.output_stream.write(indent_str.encode(self.encoding))

    def _write_newline(self):
        """Write a newline character."""
        self.output_stream.write(b'\n')


def get_field_value(row: Any, field_name: str, default: Any = None, required: bool = False,
                    context: str = None) -> Any:
    """
    Safely get a field value from a row object with optional validation.

    Works with BigQuery Row objects, dicts, or objects with attributes.

    Args:
        row: Row object (BigQuery Row, dict, or object with attributes)
        field_name: Name of the field to retrieve
        default: Default value if field doesn't exist
        required: If True, raise AirflowFailException if value is null/empty
        context: Additional context for error messages (e.g., "NoReleve=123456789")

    Returns:
        Field value or default

    Raises:
        AirflowFailException: If required=True and value is null/empty
    """
    # Try dict-like access first (BigQuery Row, dict)
    if hasattr(row, 'get'):
        value = row.get(field_name, default)
    # Try attribute access
    elif hasattr(row, field_name):
        value = getattr(row, field_name, default)
    # Try dict key access
    else:
        try:
            value = row[field_name]
        except (KeyError, TypeError):
            value = default

    # Validate if required
    if required and is_null_or_empty(value):
        error_msg = f"Field '{field_name}' is required but is null or blank"
        if context:
            error_msg += f" for {context}"
        raise AirflowFailException(error_msg)

    return value


def is_null_or_empty(value: Any) -> bool:
    """
    Check if a value is null, None, or empty string.

    Args:
        value: Value to check

    Returns:
        True if value is None, empty string, or pandas-style null
    """
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == '':
        return True
    # Handle pandas NA/NaT
    try:
        import pandas as pd
        if pd.isna(value):
            return True
    except (ImportError, TypeError):
        pass
    return False


def is_not_null(value: Any) -> bool:
    """
    Check if a value is not null (inverse of is_null_or_empty).

    Args:
        value: Value to check

    Returns:
        True if value is not null/empty
    """
    return not is_null_or_empty(value)


def validate_xml(xml_stream: BinaryIO, xsd_schema) -> None:
    """Validate XML from a stream against an XSD schema without loading into memory.

    The xmlschema library uses iterparse internally when given a file-like object,
    which processes the XML incrementally without loading it all into memory.

    Args:
        xml_stream: File-like binary stream of XML content
        xsd_schema: Loaded XSD schema object (from xmlschema.XMLSchema)

    Raises:
        AirflowFailException: If XML validation fails, with detailed error messages
    """
    # xmlschema.is_valid() and iter_errors() accept file-like objects and use iterparse
    if not xsd_schema.is_valid(xml_stream):
        # Need to re-open stream for error iteration since is_valid() consumed it
        # Reset stream position to beginning
        xml_stream.seek(0)

        # Extract detailed error information
        errors = []
        for i, error in enumerate(xsd_schema.iter_errors(xml_stream), 1):
            # Try to extract the element path and value
            if hasattr(error, 'path'):
                path = error.path
            else:
                path = "Unknown path"

            # Try to get the value that failed
            if hasattr(error, 'elem') and error.elem is not None:
                elem_tag = error.elem.tag if hasattr(error.elem, 'tag') else 'Unknown'
                elem_value = error.elem.text if hasattr(error.elem, 'text') else 'No value'
                errors.append(f"Error {i}: Element={elem_tag}, Value='{elem_value}', Path={path}, Reason={error.reason}")
            else:
                errors.append(f"Error {i}: {error.reason}")

            # Only show first 10 errors to avoid overwhelming output
            if i >= 10:
                # Count remaining errors without iterating through all of them
                xml_stream.seek(0)
                total_errors = sum(1 for _ in xsd_schema.iter_errors(xml_stream))
                errors.append(f"... and {total_errors - 10} more errors")
                break

        raise AirflowFailException("XML Validation Errors:\n" + "\n".join(errors))
