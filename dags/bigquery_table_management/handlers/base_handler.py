from abc import ABC, abstractmethod
from typing import Dict


class BaseHandler(ABC):
    """
    Abstract base class for script execution handlers.

    This class defines the contract and shared configuration for all execution handlers (e.g., SQLHandler).

    Responsibilities:
        - Store environment context (project, dataset, deploy environment)
        - Enforce implementation of execute() in subclasses
        - Provide a consistent execution interface

    Subclasses:
        Must implement the `execute` method.
    """

    def __init__(self, project_id: str, dataset_id: str, deploy_env: str, timezone: str, location: str):
        """
        Initialize base handler configuration.

        Args:
            project_id (str): GCP project ID where operations will be executed.
            dataset_id (str): Target BigQuery dataset ID.
            deploy_env (str): Deployment environment identifiers (e.g., dev, uat, prod)
            timezone (str): Timezone string for audit logging (e.g., "America/Toronto").
            location (str): BigQuery location/region (e.g., "northamerica-northeast1").
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.deploy_env = deploy_env
        self.timezone = timezone
        self.location = location

    @abstractmethod
    def execute(self, script_path: str, table_ref: str) -> bool:
        """
        Execute script safely using handler-specific logic.

        This method must be implemented by subclasses.

        Workflow:
        1. Check idempotency via audit table
        2. Load script file
        3. Replace environment placeholders
        4. Validate script rules
        5. Validate table reference
        6. Execute operation
        7. Capture updated schema
        8. Log execution in audit table

        Args:
            script_path (str):
                Relative path to script file.
            table_ref (str):
                Expected table reference.

        Returns:
            bool:
                True if script executed.
                False if skipped due to idempotency.

        Raises:
            AirflowException:
                If validation fails or execution errors occur.
        """
        pass
