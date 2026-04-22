from aml_processing.sas_aml_migration.tasks.case_feed_from_existing_cases_task import CaseFeedFromExistingCases
from aml_processing.sas_aml_migration.tasks.note_feed_from_existing_cases_task import NoteFeedFromExistingCases
from aml_processing.sas_aml_migration.tasks.person_feed_from_existing_cases_task import PersonFeedFromExistingCases
from aml_processing.sas_aml_migration.tasks.create_derived_cases_task import CaseFeedFromDerivedCases


class SASAMLDataTransform:

    def task_case_feed_from_existing_cases(self):
        task = CaseFeedFromExistingCases()
        task.execute()

    def task_note_feed_from_existing_cases(self):
        task = NoteFeedFromExistingCases()
        task.execute()

    def task_person_feed_from_existing_cases(self):
        task = PersonFeedFromExistingCases()
        task.execute()

    def task_case_feed_from_derived_cases(self):
        task = CaseFeedFromDerivedCases()
        task.execute()
