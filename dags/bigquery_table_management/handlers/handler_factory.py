import logging

from airflow.exceptions import AirflowException

from bigquery_table_management.handlers.base_handler import BaseHandler
from bigquery_table_management.handlers.sql_handler import SQLHandler
from bigquery_table_management.handlers.yaml_handler import YamlHandler

logger = logging.getLogger(__name__)


class HandlerFactory:
    """
    Factory responsible for returning the appropriate script execution handler
    based on the script file extension.

    This abstraction allows the framework to support multiple script types
    (e.g., SQL, YAML) without changing DAG or builder logic.

    Currently Supported:
        - SQL scripts (.sql)
        - YAML configs (.yaml) - routes to specialized handlers based on content:
            * Clustering updates (clustering_fields)
            * Partitioning updates (partition_field) - Future
            * Complex schema evolution (transformations) - Future

    Raises:
        AirflowException: if the script type is unsupported.
    """

    @staticmethod
    def get_handler(
        script_path: str, project_id: str, dataset_id: str, deploy_env: str, timezone: str, location: str
    ) -> BaseHandler:
        """
        Return the appropriate handler instance based on the script extension.

        The handler encapsulates execution logic such as validation,
        environment replacement, audit logging , and schema capture.

        Args:
            script_path (str): Path to the script file. Used to determine handler type.
            project_id (str): GCP project ID where operations will be executed.
            dataset_id (str): Dataset ID extracted from table reference.
            deploy_env (str): Current deployment environment (e.g., dev, uat, prod).
            timezone (str): Timezone string for audit logging (e.g., "America/Toronto").
            location (str): BigQuery location/region (e.g., "northamerica-northeast1").

        Returns:
            BaseHandler:
                Concrete handler instance corresponding to the script type.

        Raises:
            AirflowException:
                If the script extension is unsupported or not implemented.
        """

        if script_path.lower().endswith(".sql"):
            return SQLHandler(project_id, dataset_id, deploy_env, timezone, location)

        if script_path.lower().endswith(".yaml"):
            return YamlHandler(project_id, dataset_id, deploy_env, timezone, location)

        raise AirflowException(f"Unsupported script type: {script_path}")
