from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class BaseTables(object):
    # GCP Dataset ID
    gcp_dataset_id: str

    def __getattribute__(self, name) -> str:
        """
        Return the corresponding Table ID.
        """
        # Handle special attributes and avoid infinite recursion
        if name == "gcp_dataset_id":
            return super().__getattribute__(name)

        # Return Table ID
        return f"{self.gcp_dataset_id}.{super().__getattribute__(name).upper()}"


@dataclass(frozen=True)
class LandingTables(BaseTables):
    def __init__(self, gcp_dataset_id):
        super().__init__(gcp_dataset_id)

    # The BigQuery table name for storing PRMI tickets' properties.
    PRMI_TICKET: Final[str] = "PRMI_TICKET"

    # The BigQuery table name for storing DAG's properties.
    DAG_HISTORY: Final[str] = "DAG_HISTORY"
