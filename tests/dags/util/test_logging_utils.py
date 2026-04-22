"""
Tests for the logging_utils functions utilized throughout the data foundation repository.

This module tests the log_message function and build_spark_logging_info function
to ensure they work correctly with various inputs and edge cases.
"""

import pytest
import logging
import json
import sys
from unittest.mock import patch, MagicMock, call
from typing import Dict, Any
from datetime import datetime

from util.logging_utils import log_message, build_spark_logging_info


class TestLogMessage:
    """Test cases for the log_message function."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        # Reset logging to ensure clean state
        logging.getLogger().handlers.clear()

    def test_basic_logging_info_level(self):
        """Test basic logging with INFO level (default)."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            log_message("Test message")
            mock_logger.info.assert_called_once_with("Test message", exc_info=None)

    def test_basic_logging_debug_level(self):
        """Test basic logging with DEBUG level."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            log_message("Debug message", log_level="DEBUG")
            mock_logger.debug.assert_called_once_with("Debug message", exc_info=None)

    def test_basic_logging_warning_level(self):
        """Test basic logging with WARNING level."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            log_message("Warning message", log_level="WARNING")
            mock_logger.warning.assert_called_once_with("Warning message", exc_info=None)

    def test_basic_logging_error_level(self):
        """Test basic logging with ERROR level."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            log_message("Error message", log_level="ERROR")
            mock_logger.error.assert_called_once_with("Error message", exc_info=None)

    def test_basic_logging_critical_level(self):
        """Test basic logging with CRITICAL level."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            log_message("Critical message", log_level="CRITICAL")
            mock_logger.critical.assert_called_once_with("Critical message", exc_info=None)

    def test_case_insensitive_log_levels(self):
        """Test that log levels are case insensitive."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            # Test lowercase
            log_message("Test message", log_level="debug")
            mock_logger.debug.assert_called_with("Test message", exc_info=None)

            # Test mixed case
            log_message("Test message", log_level="Error")
            mock_logger.error.assert_called_with("Test message", exc_info=None)

    def test_invalid_log_level_raises_value_error(self):
        """Test that invalid log level raises ValueError."""
        with pytest.raises(ValueError,
                           match="Invalid log level 'INVALID'. Must be one of:.*"):
            log_message("Test message", log_level="INVALID")

    def test_context_logging_with_simple_context(self):
        """Test logging with simple context dictionary."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            context = {"dag_id": "test_dag", "task_id": "test_task"}
            log_message("Test message", context=context)

            expected_message = "Test message | Context: {dag_id: test_dag | task_id: test_task}"
            mock_logger.info.assert_called_once_with(expected_message, exc_info=None)

    def test_context_logging_with_complex_context(self):
        """Test logging with complex context dictionary."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            context = {
                "dag_id": "data_processing_dag",
                "task_id": "extract_data",
                "execution_date": "2024-01-15T10:00:00",
                "run_id": "manual__2024-01-15T10:00:00+00:00"
            }
            log_message("Processing data", context=context)

            expected_message = "Processing data | Context: {dag_id: data_processing_dag | task_id: extract_data | execution_date: 2024-01-15T10:00:00 | run_id: manual__2024-01-15T10:00:00+00:00}"
            mock_logger.info.assert_called_once_with(expected_message, exc_info=None)

    def test_context_logging_filters_none_values(self):
        """Test that context logging filters out None values."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            context = {
                "dag_id": "test_dag",
                "task_id": None,
                "execution_date": "2024-01-15",
                "run_id": None
            }
            log_message("Test message", context=context)

            expected_message = "Test message | Context: {dag_id: test_dag | execution_date: 2024-01-15}"
            mock_logger.info.assert_called_once_with(expected_message, exc_info=None)

    def test_context_logging_with_empty_context(self):
        """Test logging with empty context dictionary."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            log_message("Test message", context={})

            # Should log original message without context
            mock_logger.info.assert_called_once_with("Test message", exc_info=None)

    def test_context_logging_with_all_none_values(self):
        """Test logging with context containing only None values."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            context = {"dag_id": None, "task_id": None}
            log_message("Test message", context=context)

            # Should log original message without context
            mock_logger.info.assert_called_once_with("Test message", exc_info=None)

    def test_context_formatting_failure_handling(self):
        """Test handling of context formatting failures."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            # Create a context that will cause formatting to fail
            class BadContext:
                def items(self):
                    raise Exception("Formatting error")

            context = BadContext()
            log_message("Test message", context=context)

            # Should log warning about formatting failure and original message
            mock_logger.warning.assert_called_once()
            mock_logger.info.assert_called_once_with("Test message", exc_info=None)

    def test_exception_logging_with_exception_object(self):
        """Test logging with exception object."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            test_exception = ValueError("Test error")
            log_message("Error occurred", log_level="ERROR", exc_info=test_exception)

            mock_logger.error.assert_called_once_with("Error occurred", exc_info=test_exception)

    def test_exception_logging_with_boolean_true(self):
        """Test logging with exc_info=True."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            log_message("Error occurred", log_level="ERROR", exc_info=True)

            mock_logger.error.assert_called_once_with("Error occurred", exc_info=True)

    def test_exception_logging_with_boolean_false(self):
        """Test logging with exc_info=False."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            log_message("Error occurred", log_level="ERROR", exc_info=False)

            mock_logger.error.assert_called_once_with("Error occurred", exc_info=False)

    def test_exception_logging_with_none(self):
        """Test logging with exc_info=None."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            log_message("Error occurred", log_level="ERROR", exc_info=None)

            mock_logger.error.assert_called_once_with("Error occurred", exc_info=None)

    def test_logging_with_context_and_exception(self):
        """Test logging with both context and exception information."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            context = {"dag_id": "test_dag", "task_id": "test_task"}
            test_exception = ValueError("Test error")

            log_message("Error occurred", log_level="ERROR", context=context, exc_info=test_exception)

            expected_message = "Error occurred | Context: {dag_id: test_dag | task_id: test_task}"
            mock_logger.error.assert_called_once_with(expected_message, exc_info=test_exception)

    def test_logging_failure_fallback(self):
        """Test fallback behavior when logging fails."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            mock_logger.info.side_effect = Exception("Logging failed")

            with patch('util.logging_utils._fallback_log') as mock_fallback:
                log_message("Test message")

                # Should call fallback logging
                mock_fallback.assert_called_once()

    def test_logging_fallback_also_fails(self):
        """Test behavior when both main logging and fallback logging fail."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            mock_logger.info.side_effect = Exception("All logging failed")

            with patch('util.logging_utils._fallback_log') as mock_fallback:
                mock_fallback.side_effect = Exception("Fallback also failed")
                # The function should handle the fallback failure gracefully
                # by catching the exception in the _fallback_log function itself
                # We expect this to not raise an exception

                try:
                    log_message("Test message")
                except Exception as e:
                    # If it does raise an exception, it should be the fallback exception
                    assert "Fallback also failed" in str(e)

                # Should still attempt fallback
                mock_fallback.assert_called_once()

    def test_all_parameters_combined(self):
        """Test logging with all parameters provided."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            context = {"dag_id": "test_dag", "task_id": "test_task"}
            test_exception = ValueError("Test error")

            log_message(
                message="Processing completed",
                log_level="INFO",
                context=context,
                exc_info=test_exception
            )

            expected_message = "Processing completed | Context: {dag_id: test_dag | task_id: test_task}"
            mock_logger.info.assert_called_once_with(expected_message, exc_info=test_exception)

    @pytest.mark.parametrize("log_level", ["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"])
    def test_all_valid_log_levels(self, log_level):
        """Test all valid log levels work correctly."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            log_message("Test message", log_level=log_level)

            # Verify the appropriate method was called
            method_name = log_level.lower()
            getattr(mock_logger, method_name).assert_called_once_with("Test message", exc_info=None)


class TestStructuredLogging:
    """Test cases for structured logging functionality."""

    def test_structured_logging_basic(self):
        """Test basic structured logging."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            with patch('util.logging_utils.datetime') as mock_datetime:
                mock_datetime.utcnow.return_value.isoformat.return_value = "2024-01-15T10:00:00.000000"
                log_message("Test message", structured=True)

                # Verify JSON was logged
                call_args = mock_logger.info.call_args[0][0]
                log_data = json.loads(call_args)
                assert log_data["message"] == "Test message"
                assert log_data["level"] == "INFO"
                assert log_data["logger"] == "pcb.airflow.logging_utils"
                assert "timestamp" in log_data

    def test_structured_logging_with_context(self):
        """Test structured logging with context."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            with patch('util.logging_utils.datetime') as mock_datetime:
                mock_datetime.utcnow.return_value.isoformat.return_value = "2024-01-15T10:00:00.000000"
                context = {"dag_id": "test_dag", "task_id": "test_task"}
                log_message("Test message", context=context, structured=True)

                call_args = mock_logger.info.call_args[0][0]
                log_data = json.loads(call_args)
                assert log_data["message"] == "Test message"
                assert log_data["context"] == context

    def test_structured_logging_filters_none_context(self):
        """Test that structured logging filters out None values from context."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            with patch('util.logging_utils.datetime') as mock_datetime:
                mock_datetime.utcnow.return_value.isoformat.return_value = "2024-01-15T10:00:00.000000"
                context = {"dag_id": "test_dag", "task_id": None, "run_id": "test_run"}
                log_message("Test message", context=context, structured=True)

                call_args = mock_logger.info.call_args[0][0]
                log_data = json.loads(call_args)
                expected_context = {"dag_id": "test_dag", "run_id": "test_run"}
                assert log_data["context"] == expected_context

    def test_structured_logging_with_exception_object(self):
        """Test structured logging with exception object."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            with patch('util.logging_utils.datetime') as mock_datetime:
                mock_datetime.utcnow.return_value.isoformat.return_value = "2024-01-15T10:00:00.000000"
                test_exception = ValueError("Test error")
                log_message("Error occurred", log_level="ERROR", exc_info=test_exception, structured=True)

                call_args = mock_logger.error.call_args[0][0]
                log_data = json.loads(call_args)
                assert log_data["message"] == "Error occurred"
                assert log_data["level"] == "ERROR"
                assert "exception" in log_data
                assert log_data["exception"]["type"] == "ValueError"
                assert log_data["exception"]["message"] == "Test error"
                assert "traceback" in log_data["exception"]

    def test_structured_logging_with_exc_info_true(self):
        """Test structured logging with exc_info=True."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            with patch('util.logging_utils.datetime') as mock_datetime:
                mock_datetime.utcnow.return_value.isoformat.return_value = "2024-01-15T10:00:00.000000"
                with patch('sys.exc_info') as mock_exc_info:
                    mock_exc_info.return_value = (ValueError, ValueError("Test error"), None)
                    log_message("Error occurred", log_level="ERROR", exc_info=True, structured=True)

                    call_args = mock_logger.error.call_args[0][0]
                    log_data = json.loads(call_args)
                    assert log_data["message"] == "Error occurred"
                    assert "exception" in log_data
                    assert log_data["exception"]["type"] == "ValueError"

    def test_structured_logging_with_exc_info_false(self):
        """Test structured logging with exc_info=False."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            with patch('util.logging_utils.datetime') as mock_datetime:
                mock_datetime.utcnow.return_value.isoformat.return_value = "2024-01-15T10:00:00.000000"
                log_message("Test message", exc_info=False, structured=True)

                call_args = mock_logger.info.call_args[0][0]
                log_data = json.loads(call_args)
                assert log_data["message"] == "Test message"
                assert "exception" not in log_data

    def test_structured_logging_with_all_parameters(self):
        """Test structured logging with all parameters."""
        with patch('util.logging_utils.task_logger') as mock_logger:
            with patch('util.logging_utils.datetime') as mock_datetime:
                mock_datetime.utcnow.return_value.isoformat.return_value = "2024-01-15T10:00:00.000000"
                context = {"dag_id": "test_dag", "task_id": "test_task"}
                test_exception = ValueError("Test error")
                log_message(
                    message="Processing completed",
                    log_level="WARNING",
                    context=context,
                    exc_info=test_exception,
                    structured=True
                )

                call_args = mock_logger.warning.call_args[0][0]
                log_data = json.loads(call_args)
                assert log_data["message"] == "Processing completed"
                assert log_data["level"] == "WARNING"
                assert log_data["context"] == context
                assert "exception" in log_data
                assert log_data["exception"]["type"] == "ValueError"


class TestFallbackLogging:
    """Test cases for the _fallback_log function."""

    def test_fallback_log_success(self):
        """Test successful fallback logging."""
        with patch('builtins.print') as mock_print:
            from util.logging_utils import _fallback_log
            _fallback_log("Test fallback message")
            # Verify print was called with fallback format
            mock_print.assert_called_once()
            call_args = mock_print.call_args[0][0]
            assert "[FALLBACK LOG]" in call_args
            assert "Test fallback message" in call_args

    def test_fallback_log_with_exception(self):
        """Test fallback logging when print also fails."""
        with patch('builtins.print') as mock_print:
            mock_print.side_effect = Exception("Print failed")
            from util.logging_utils import _fallback_log
            # Should not raise exception
            _fallback_log("Test fallback message")
            # Verify print was attempted
            mock_print.assert_called_once()


class TestBuildSparkLoggingInfo:
    """Test cases for the build_spark_logging_info function."""

    def test_build_spark_logging_info_basic(self):
        """Test basic functionality of build_spark_logging_info."""
        dag_id = "test_dag"
        default_args = {"owner": "test_owner"}
        arg_list = ["arg1", "arg2"]
        run_id = "test_run_123"

        with patch('util.logging_utils.consts.SPARK_DAG_INFO', 'SPARK_DAG_INFO'):
            result = build_spark_logging_info(dag_id, default_args, arg_list, run_id)

            expected_dag_info = "[dag_name: test_dag, dag_run_id: test_run_123, dag_owner: test_owner]"
            expected_arg = f"SPARK_DAG_INFO={expected_dag_info}"

            assert result == ["arg1", "arg2", expected_arg]
            assert len(result) == 3

    def test_build_spark_logging_info_with_default_run_id(self):
        """Test build_spark_logging_info with default run_id."""
        dag_id = "test_dag"
        default_args = {"owner": "test_owner"}
        arg_list = ["arg1"]

        with patch('util.logging_utils.consts.SPARK_DAG_INFO', 'SPARK_DAG_INFO'):
            result = build_spark_logging_info(dag_id, default_args, arg_list)

            expected_dag_info = "[dag_name: test_dag, dag_run_id: {{run_id}}, dag_owner: test_owner]"
            expected_arg = f"SPARK_DAG_INFO={expected_dag_info}"

            assert result == ["arg1", expected_arg]

    def test_build_spark_logging_info_with_empty_arg_list(self):
        """Test build_spark_logging_info with empty argument list."""
        dag_id = "test_dag"
        default_args = {"owner": "test_owner"}
        arg_list = []

        with patch('util.logging_utils.consts.SPARK_DAG_INFO', 'SPARK_DAG_INFO'):
            result = build_spark_logging_info(dag_id, default_args, arg_list)

            expected_dag_info = "[dag_name: test_dag, dag_run_id: {{run_id}}, dag_owner: test_owner]"
            expected_arg = f"SPARK_DAG_INFO={expected_dag_info}"

            assert result == [expected_arg]

    def test_build_spark_logging_info_modifies_original_list(self):
        """Test that build_spark_logging_info modifies the original list."""
        dag_id = "test_dag"
        default_args = {"owner": "test_owner"}
        arg_list = ["arg1", "arg2"]

        with patch('util.logging_utils.consts.SPARK_DAG_INFO', 'SPARK_DAG_INFO'):
            result = build_spark_logging_info(dag_id, default_args, arg_list)

            # The original list should be modified
            assert arg_list == result
            assert len(arg_list) == 3

    def test_build_spark_logging_info_with_special_characters(self):
        """Test build_spark_logging_info with special characters in dag_id and owner."""
        dag_id = "test-dag_with.special@chars"
        default_args = {"owner": "test.owner@domain.com"}
        arg_list = ["arg1"]

        with patch('util.logging_utils.consts.SPARK_DAG_INFO', 'SPARK_DAG_INFO'):
            result = build_spark_logging_info(dag_id, default_args, arg_list)

            expected_dag_info = "[dag_name: test-dag_with.special@chars, dag_run_id: {{run_id}}, dag_owner: test.owner@domain.com]"
            expected_arg = f"SPARK_DAG_INFO={expected_dag_info}"

            assert result == ["arg1", expected_arg]

    def test_build_spark_logging_info_returns_same_list_reference(self):
        """Test that build_spark_logging_info returns the same list reference."""
        dag_id = "test_dag"
        default_args = {"owner": "test_owner"}
        arg_list = ["arg1"]

        with patch('util.logging_utils.consts.SPARK_DAG_INFO', 'SPARK_DAG_INFO'):
            result = build_spark_logging_info(dag_id, default_args, arg_list)

            # Should return the same list object
            assert result is arg_list


class TestLoggingUtilsIntegration:
    """Integration tests for logging utilities."""

    def test_log_message_with_real_logger(self):
        """Test log_message with a real logger to ensure it works end-to-end."""
        # Create a real logger for testing
        test_logger = logging.getLogger("test_logger")
        test_logger.setLevel(logging.DEBUG)

        # Create a handler to capture log messages
        from io import StringIO
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        test_logger.addHandler(handler)

        with patch('util.logging_utils.task_logger', test_logger):
            log_message("Integration test message", log_level="INFO")

            log_output = log_capture.getvalue()
            assert "INFO - Integration test message" in log_output

    def test_log_message_context_integration(self):
        """Test log_message context formatting with real logger."""
        test_logger = logging.getLogger("test_logger_context")
        test_logger.setLevel(logging.DEBUG)

        from io import StringIO
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        test_logger.addHandler(handler)

        with patch('util.logging_utils.task_logger', test_logger):
            context = {"dag_id": "integration_test", "task_id": "test_task"}
            log_message("Context integration test", context=context)

            log_output = log_capture.getvalue()
            assert "INFO - Context integration test | Context: {dag_id: integration_test | task_id: test_task}" in log_output

    def test_structured_logging_integration(self):
        """Test structured logging with real logger."""
        test_logger = logging.getLogger("test_structured_logger")
        test_logger.setLevel(logging.DEBUG)

        from io import StringIO
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        test_logger.addHandler(handler)

        with patch('util.logging_utils.task_logger', test_logger):
            with patch('util.logging_utils.datetime') as mock_datetime:
                mock_datetime.utcnow.return_value.isoformat.return_value = "2024-01-15T10:00:00.000000Z"
                context = {"dag_id": "integration_test", "task_id": "test_task"}
                log_message("Structured integration test", context=context, structured=True)

                log_output = log_capture.getvalue()
                assert "INFO - " in log_output
                # Extract JSON part
                json_part = log_output.split("INFO - ")[1].strip()
                log_data = json.loads(json_part)
                assert log_data["message"] == "Structured integration test"
                assert log_data["context"] == context
                assert log_data["level"] == "INFO"
                assert log_data["logger"] == "pcb.airflow.logging_utils"
