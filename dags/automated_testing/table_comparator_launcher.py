# for airflow scanning
from airflow import settings
from automated_testing.table_comparator import TableComparator

# Separate out the launching part so that table_comparator can be reused.
globals().update(TableComparator('automated_testing',
                                 'table_comparator_config.yaml',
                                 f'{settings.DAGS_FOLDER}/config').create())  # pragma: no cover
