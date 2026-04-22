"""
Reversal Search Tool DAG.

This DAG enables TechOps to find customer and account identifiers (AccountUID,
AccountID, CustomerUID, CustomerID, userID, ApplicationID) using PII inputs
(email, name, address, or combinations thereof).

The tool uses a registry pattern to support multiple input types dynamically.
All PII inputs are masked in logs for security and privacy compliance.
"""

import logging
from datetime import timedelta
from textwrap import dedent

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

import util.constants as consts
from dag_factory.abc import BaseDagBuilder
from dag_factory.terminus_dag_factory import DAGFactory
from util.miscutils import read_variable_or_file

from reversal_search_tool.handlers import INPUT_TYPE_REGISTRY
from reversal_search_tool.utils import format_results, MAX_RESULTS_TO_DISPLAY

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

# DAG Configuration
DAG_ID = 'reversal_search_tool'
DAG_OWNER = 'Team Centaurs'
DAG_TAGS = ['techops', 'reversal-search', 'troubleshooting']

# Execution Configuration
EXECUTION_TIMEOUT = timedelta(minutes=10)
RETRY_DELAY = timedelta(minutes=2)
MAX_RETRIES = 0  # Fail fast - manual troubleshooting tool, user can re-run if needed


# ============================================================================
# TASK FUNCTIONS
# ============================================================================

def validate_and_get_handler(dag_run) -> tuple:
    """
    Validate DAG run configuration and return handler, project ID, and input type.

    Args:
        dag_run: Airflow DAG run object containing configuration

    Returns:
        Tuple of (project_id, handler, input_type, dag_run_conf)

    Raises:
        AirflowException: If validation fails at any step
    """
    # Get environment-based project ID (dev/uat/prod curated zone)
    gcp_config = read_variable_or_file(consts.GCP_CONFIG)
    project_id = gcp_config.get(consts.CURATED_ZONE_PROJECT_ID)
    if not project_id:
        raise AirflowException("Missing curated_zone_project_id in GCP config")

    logger.info(f"Using project: {project_id}")

    # Validate that configuration was provided via DAG Run Config
    dag_run_conf = dag_run.conf or {}
    if not dag_run_conf:
        raise AirflowException(
            "No configuration provided. Please provide configuration via 'Trigger DAG w/ config'.\n"
            "Required fields:\n"
            "- input_type: 'email', 'name', 'address', or 'combined'\n"
            "- Additional fields based on input_type (see documentation)"
        )

    # Get and validate input_type
    input_type = dag_run_conf.get('input_type')
    if not input_type:
        raise AirflowException(
            "Missing required field: 'input_type'.\n"
            "Please provide 'input_type' in configuration: 'email', 'name', 'address', or 'combined'"
        )

    # Get handler from registry (registry pattern for dynamic handler selection)
    if input_type not in INPUT_TYPE_REGISTRY:
        available_types = ", ".join(INPUT_TYPE_REGISTRY.keys())
        raise AirflowException(
            f"Unsupported input_type: '{input_type}'. "
            f"Available types: {available_types}"
        )

    handler = INPUT_TYPE_REGISTRY[input_type]

    # Validate input using handler-specific validation
    try:
        handler.validate_input(dag_run_conf)
    except ValueError as e:
        logger.error(f"Input validation failed: {e}")
        raise AirflowException(f"Input validation failed: {e}")

    return (project_id, handler, input_type, dag_run_conf)


def reversal_search_task(**context):
    """
    Main reversal search task.

    Uses registry pattern to dynamically select handler based on input_type.
    Validates input, builds SQL query, executes BigQuery query, and formats results.
    All PII is masked in logs for security compliance.

    Args:
        **context: Airflow context dictionary containing dag_run and other metadata

    Returns:
        Summary string indicating search completion and result count

    Raises:
        AirflowException: If validation fails, input type is unsupported,
                         project ID missing, or query execution fails
    """
    dag_run = context['dag_run']

    # Validate configuration and get handler
    project_id, handler, input_type, dag_run_conf = validate_and_get_handler(dag_run)

    # Log masked input (PII protection)
    log_message = handler.get_log_message(dag_run_conf)
    logger.info(dedent(f"""
        {'=' * 50}
        Starting Reversal Search Tool
        Input type: {input_type}
        {log_message}
        Query built successfully (PII masked in logs)
    """))

    # Build SQL query with environment-specific project ID
    query = handler.build_query(dag_run_conf, project_id)

    # Execute BigQuery query and wait for results
    try:
        logger.info("Executing BigQuery query...")
        bq_client = bigquery.Client()

        # query_and_wait() submits query, waits for completion, and returns results
        query_result = bq_client.query_and_wait(query)
        logger.info(f"Query completed. Total rows: {query_result.total_rows}")

        # Extract results into list of dictionaries
        results = []
        for row in query_result:
            results.append({
                'AccountUID': row.get('AccountUID'),
                'AccountID': row.get('AccountID'),
                'CustomerUID': row.get('CustomerUID'),
                'CustomerID': row.get('CustomerID'),
                'userID': row.get('userID'),
                'ApplicationID': row.get('ApplicationID'),
            })

        # Format results as readable table and log
        total_records = len(results)
        formatted_output = format_results(results)

        # Log results section header
        logger.info(dedent(f"""
            {'=' * 50}
            REVERSAL SEARCH RESULTS
            {'=' * 50}
        """))

        # Log formatted table (formatted_output includes leading newline)
        logger.info(formatted_output)

        # Log summary and truncation notice if applicable
        logger.info(f"\nTotal Records: {total_records}")
        if total_records > MAX_RESULTS_TO_DISPLAY:
            logger.info(
                f"Note: Only first {MAX_RESULTS_TO_DISPLAY} results displayed in logs. "
                f"Total {total_records} results found."
            )
        logger.info(f"{'=' * 50}\n")
        logger.info("Reversal search completed successfully")

        # Return summary string instead of full results list
        # This prevents Airflow from auto-logging large data structures
        # The formatted table is already logged above in readable format
        return f"Search completed successfully. Found {total_records} record(s). Results displayed in logs above."

    except Exception as e:
        logger.error(f"BigQuery query execution failed: {e}")
        raise AirflowException(f"BigQuery query execution failed: {e}") from e


# ============================================================================
# DAG BUILDER
# ============================================================================

class ReversalSearchToolDagBuilder(BaseDagBuilder):
    """
    DAG Builder for Reversal Search Tool.

    Creates a DAG that enables TechOps to find customer and account identifiers
    using PII inputs (email, name, address, or combinations thereof).
    """

    def build(self, dag_id: str, config: dict) -> DAG:
        """
        Build the Reversal Search Tool DAG.

        Args:
            dag_id: DAG identifier
            config: DAG configuration dictionary (not used for this DAG)

        Returns:
            Configured DAG instance
        """
        # Prepare default_args using BaseDagBuilder method
        default_args = self.prepare_default_args({
            'owner': DAG_OWNER,
            'capability': 'Terminus Data Platform',
            'severity': 'P3',
            'sub_capability': 'Data Operations',
            'business_impact': 'Low',
            'customer_impact': 'Low',
            'depends_on_past': False,
            'email': [],
            'email_on_failure': False,
            'email_on_retry': False,
            'execution_timeout': EXECUTION_TIMEOUT,
            'retries': MAX_RETRIES,
            'retry_delay': RETRY_DELAY
        })

        local_tz = self.environment_config.local_tz

        with DAG(
            dag_id=dag_id,
            default_args=default_args,
            schedule=None,  # Manual trigger only - requires DAG Run Config
            start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
            catchup=False,
            max_active_runs=1,  # Prevent concurrent runs
            is_paused_upon_creation=True,  # DAG paused on creation for security review
            tags=DAG_TAGS
        ) as dag:
            start = EmptyOperator(task_id='start')

            reversal_search = PythonOperator(
                task_id='reversal_search_task',
                python_callable=reversal_search_task,
                provide_context=True
            )

            end = EmptyOperator(task_id='end')

            start >> reversal_search >> end

        return dag


# ============================================================================
# DAG REGISTRATION
# ============================================================================

# Create DAG using the factory pattern
globals().update(DAGFactory().create_dag(ReversalSearchToolDagBuilder, DAG_ID))
