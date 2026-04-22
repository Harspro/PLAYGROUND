import pytest
import dataclasses
from jira_analytics.config.terminus_table_config import BaseTables, LandingTables, CuratedTables


class TestTerminusTableConfig:
    """
    Class to test Terminus Table Config. This includes testing the base, curated and landing tables.
    """
    def test_base_tables(self):
        dataset_id = "my_dataset"
        base_tables = BaseTables(gcp_dataset_id=dataset_id)

        # Ensure dataset ID is stored correctly
        assert base_tables.gcp_dataset_id == dataset_id

        # Check attribute lookup for a non-existent table
        with pytest.raises(AttributeError):
            _ = base_tables.NON_EXISTENT_TABLE

    def test_landing_tables(self):
        dataset_id = "landing_dataset"
        landing_tables = LandingTables(gcp_dataset_id=dataset_id)

        # Check that the table names are correctly formatted
        assert getattr(landing_tables, "INITIATIVE") == f"{dataset_id}.INITIATIVE"
        assert landing_tables.CHANGELOG_EVENT == f"{dataset_id}.CHANGELOG_EVENT"

    def test_curated_tables(self):
        dataset_id = "curated_dataset"
        curated_tables = CuratedTables(gcp_dataset_id=dataset_id)

        # Check that the table names are correctly formatted
        assert getattr(curated_tables, "UNIQUE_ISSUES") == f"{dataset_id}.UNIQUE_ISSUES"
        assert curated_tables.TARGET_BURNDOWN == f"{dataset_id}.TARGET_BURNDOWN"

    def test_frozen_dataclass(self):
        dataset_id = "test_dataset"
        landing_tables = LandingTables(gcp_dataset_id=dataset_id)

        # Ensure modifying an attribute raises an error
        with pytest.raises(dataclasses.FrozenInstanceError):
            landing_tables.INITIATIVE = "NEW_TABLE"
