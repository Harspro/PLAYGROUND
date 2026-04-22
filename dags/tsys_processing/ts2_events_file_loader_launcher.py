# for airflow scanning
from tsys_processing.ts2_events_file_loader import Ts2EventsFileLoader

globals().update(Ts2EventsFileLoader('ts2_events_file_ingestion_config.yaml').create_dags())  # pragma: no cover
