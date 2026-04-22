# for airflow scanning
from payments_processing.payments_file_loader import PaymentsBatchLoader

globals().update(PaymentsBatchLoader('payments_file_loader.yaml').create_dags())  # pragma: no cover
