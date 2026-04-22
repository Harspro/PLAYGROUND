# for airflow scanning
from airflow import settings
from automated_testing.table_comparator import TableComparator

# Separate out the launching part so that table_comparator can be reused.
globals().update(TableComparator('pcl_campaign_merchant_updating_job',
                                 'loyalty_configs/campaign_merchant_comparator_config.yaml',
                                 f'{settings.DAGS_FOLDER}/config').create())  # pragma: no cover
