import json
import logging
from datetime import datetime
import traceback
from typing import Optional, Union, Dict, Any
import util.constants as consts

# Existing logger for Airflow tasks
task_logger = logging.getLogger('airflow.task')

# Valid log level
_VALID_LOG_LEVELS = frozenset(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])


def log_message(
        message: str,
        log_level: str = "INFO",
        context: Optional[Dict[str, Any]] = None,
        exc_info: Optional[Union[bool, Exception]] = None,
        structured: bool = False
) -> None:
    """
    A reusable logging utility function for Airflow DAG runs that integrates with Airflow's logging system.

    This function provides a centralized way to log messages during Airflow task execution with support
    for different log levels, contextual information, and exception details.

    Args:
        message (str): The content to log. This is a required parameter.

        log_level (str, optional): The severity level of the log. Acceptable values are:
            - "INFO": General information messages (default)
            - "DEBUG": Detailed debug information
            - "WARNING": Warning messages
            - "ERROR": Error messages
            - "CRITICAL": Critical error messages
            Defaults to "INFO".

        context (dict, optional): Dictionary carrying contextual information about the Airflow run.
            Common keys include:
            - dag_id: The ID of the DAG
            - task_id: The ID of the current task
            - execution_date: The execution date of the DAG run
            - run_id: The unique run ID
            If provided, this information will be included in the log output.
            Defaults to None.

        exc_info (bool or Exception, optional): Exception information to be logged.
            - If True: Logs the current exception info
            - If an Exception object: Logs that specific exception
            - If False or None: No exception info is logged
            Defaults to None.

        structured (bool, optional): Whether to output structured (JSON) logs.
            - If True: Output logs in JSON format for better parsing by log aggregation systems.
            Default to False

    Returns:
        None

    Raises:
        ValueError: If an invalid log level is provided.

    Examples:
        # Basic usage
        log_message("Task started successfully")

        # With custom log level
        log_message("Processing data", log_level="DEBUG")

        # With context information
        context = {"dag_id": "my_dag", "task_id": "process_data"}
        log_message("Processing data", context=context)

        # With exception information
        try:
            # Some operation
            pass
        except Exception as e:
            log_message("Error occurred", log_level="ERROR", exc_info=e)

        # With all parameters
        log_message(
            message="Data processing completed",
            log_level="INFO",
            context={"dag_id": "my_dag", "task_id": "process_data"},
            exc_info=False
        )
    """
    log_level_upper = log_level.upper()
    if log_level_upper not in _VALID_LOG_LEVELS:
        raise ValueError(f"Invalid log level '{log_level}'. Must be one of: {', '.join(_VALID_LOG_LEVELS)}")

    # Get the logger for the current module
    logger = task_logger

    # Build the log message with context if provided
    try:
        if structured:
            log_data = _build_structured_log(message, context, log_level_upper, exc_info)
            log_msg = json.dumps(log_data, separators=(',', ':'))
        else:
            log_msg = _build_readable_log(message, context)
    except Exception as e:
        # If context formatting fails, log the original message and the error
        logger.warning(f"Failed to format context for message '{message}': {str(e)}")
        log_msg = message

    # Log the message with the specified level (using getattr for cleaner code)
    try:
        log_method = getattr(logger, log_level_upper.lower())
        log_method(log_msg, exc_info=exc_info)
    except AttributeError:
        # This should not happen due to validation above, but safety first
        logger.info(log_msg, exc_info=exc_info)
    except Exception as e:
        # Fallback logging if the main logging fails
        _fallback_log(f"Logging failed for message '{message}': {str(e)}")


def _build_readable_log(message: str, context: Optional[Dict[str, Any]]) -> str:
    """Build a human-readable log message."""
    if not context:
        return message

    # Format context information in a readable way
    context_str = " | ".join([f"{k}: {v}" for k, v in context.items() if v is not None])
    return f"{message} | Context: {{{context_str}}}" if context_str else message


def _build_structured_log(
        message: str,
        context: Optional[Dict[str, Any]],
        log_level: str,
        exc_info: Optional[Union[bool, Exception]]
) -> Dict[str, Any]:
    """Build a structured log entry as a dictionary."""
    log_data = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "level": log_level,
        "message": message,
        "logger": "pcb.airflow.logging_utils"
    }

    if context:
        # Filter out None values and add context
        log_data["context"] = {k: v for k, v in context.items() if v is not None}

    if exc_info:
        if isinstance(exc_info, Exception):
            log_data["exception"] = {
                "type": type(exc_info).__name__,
                "message": str(exc_info),
                "traceback": traceback.format_exception(type(exc_info), exc_info, exc_info.__traceback__)
            }
        elif exc_info is True:
            # Get current exception info
            import sys
            exc_type, exc_value, exc_traceback = sys.exc_info()
            if exc_type:
                log_data["exception"] = {
                    "type": exc_type.__name__,
                    "message": str(exc_value),
                    "traceback": traceback.format_exception(exc_type, exc_value, exc_traceback)
                }

    return log_data


def _fallback_log(message: str) -> None:
    """Fallback logging mechanism when primary logging fails."""
    try:
        print(f"[FALLBACK LOG] {datetime.utcnow().isoformat()}: {message}")
    except Exception:
        # If even print fails, there's not much we can do
        pass


def build_spark_logging_info(dag_id, default_args, arg_list, run_id='{{run_id}}'):
    """
    Build Spark logging information for DAG execution.

    Args:
        dag_id: The ID of the DAG
        default_args: Default arguments for the DAG
        arg_list: List of arguments to append to
        run_id: The run ID (defaults to Airflow template)

    Returns:
        List with Spark logging information appended
    """
    dag_owner = default_args['owner']
    dag_info = f"[dag_name: {dag_id}, dag_run_id: {run_id}, dag_owner: {dag_owner}]"
    arg_list.append(f'{consts.SPARK_DAG_INFO}={dag_info}')
    return arg_list
