# for airflow scanning
from aml_processing.transaction.transaction_feed_extractor_base import TransactionFeedExtractor

# separate out the launching part so that the transaction loader can be reused
globals().update(TransactionFeedExtractor('transaction_loading_config.yaml').create_dags())  # pragma: no cover
