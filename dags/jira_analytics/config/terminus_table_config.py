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

    # The BigQuery table name for storing initiatives' properties.
    INITIATIVE: Final[str] = "INITIATIVE"

    # The BigQuery table name for storing epics' properties.
    EPIC: Final[str] = "EPIC"

    # The BigQuery table name for storing stories' properties.
    STORY: Final[str] = "STORY"

    # The BigQuery table name for storing changelog events.
    CHANGELOG_EVENT: Final[str] = "CHANGELOG_EVENT"

    # The BigQuery table name for storing changelog events count.
    CHANGELOG_COUNT: Final[str] = "CHANGELOG_COUNT"

    # The BigQuery table name for storing DAG's properties.
    DAG_HISTORY: Final[str] = "DAG_HISTORY"

    # The BigQuery Table name for storing outcome teams.
    OUTCOME_TEAM: Final[str] = "OUTCOME_TEAM"

    # The BigQuery Table name for storing sprint data.
    SPRINT: Final[str] = "SPRINT"

    # The BigQuery table name for storing active sprint and story relation data.
    ISSUES_IN_ACTIVE_SPRINTS: Final[str] = "ISSUES_IN_ACTIVE_SPRINTS"

    # The BigQuery table name for storing closed sprint and story relation data.
    ISSUES_IN_CLOSED_SPRINTS: Final[str] = "ISSUES_IN_CLOSED_SPRINTS"

    # The BigQuery table name for storing BUG data.
    BUG: Final[str] = "BUG"


@dataclass(frozen=True)
class CuratedTables(BaseTables):
    def __init__(self, gcp_dataset_id):
        super().__init__(gcp_dataset_id)

    # Base Tables
    # The BigQuery table name for storing the unique issue keys and their details.
    UNIQUE_ISSUES: Final[str] = "UNIQUE_ISSUES"

    # The BigQuery table name for storing the status changelog.
    STATUS_CHANGELOG: Final[str] = "STATUS_CHANGELOG"

    # The BigQuery table name for storing the t_shirt_size changelog.
    T_SHIRT_SIZE_CHANGELOG: Final[str] = "T_SHIRT_SIZE_CHANGELOG"

    # The BigQuery table name for storing the story_point changelog.
    STORY_POINT_CHANGELOG: Final[str] = "STORY_POINT_CHANGELOG"

    # The BigQuery table name for storing the Burndown Event Transformation.
    BURNDOWN_EVENT: Final[str] = "BURNDOWN_EVENT"

    # The BigQuery table name for storing the Target Burndown Transformation.
    TARGET_BURNDOWN: Final[str] = "TARGET_BURNDOWN"

    # The BigQuery table name for storing the Forecasted Burndown Transformation.
    FORECASTED_BURNDOWN: Final[str] = "FORECASTED_BURNDOWN"

    # The BigQuery table name for storing the Outcome Team Story Allocation.
    OUTCOME_TEAMS_STORY_ALLOCATION: Final[str] = "OUTCOME_TEAMS_STORY_ALLOCATION"

    # The BigQuery table name for storing the Outcome Team Bugs.
    OUTCOME_TEAMS_BUG: Final[str] = "OUTCOME_TEAMS_BUG"

    # The BigQuery table name for storing the Outcome Team Completed Story Points.
    LOOKER_OUTCOME_TEAMS_COMPLETED_STORY_POINTS: Final[str] = "LOOKER_OUTCOME_TEAMS_COMPLETED_STORY_POINTS"

    # Looker View Tables
    # The BigQuery table name for Looker View initiative_burndown_chart.
    LOOKER_EPIC_BURNDOWN_EVENTS: Final[str] = "LOOKER_EPIC_BURNDOWN_EVENTS"

    # The BigQuery table name for Looker View epic_burndown_events_table.
    LOOKER_INITIATIVE_BURNDOWN_CHART: Final[str] = "LOOKER_INITIATIVE_BURNDOWN_CHART"

    # The BigQuery table name for Looker View cumulative_flow.
    LOOKER_CUMULATIVE_FLOW: Final[str] = "LOOKER_CUMULATIVE_FLOW"

    # The BigQuery table name for Looker View story_point_burndown_chart.
    LOOKER_STORY_POINT_BURNDOWN_CHART: Final[str] = "LOOKER_STORY_POINT_BURNDOWN_CHART"

    # The BigQuery table name for Looker View combined_initiative_allocation.
    LOOKER_COMBINED_INITIATIVE_ALLOCATION: Final[str] = "LOOKER_COMBINED_INITIATIVE_ALLOCATION"

    # The BigQuery table name for Looker View active_sprint_allocation.
    LOOKER_ACTIVE_SPRINT_ALLOCATION: Final[str] = "LOOKER_ACTIVE_SPRINT_ALLOCATION"

    # The BigQuery table name for Tableau view story_allocation
    TABLEAU_STORY_ALLOCATION: Final[str] = "TABLEAU_STORY_ALLOCATION"
