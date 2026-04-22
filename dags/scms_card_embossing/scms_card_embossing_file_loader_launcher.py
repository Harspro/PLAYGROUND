# for airflow scanning
from airflow import DAG, settings
from scms_card_embossing.scms_card_embossing_file_loader import CardEmbossingLoader

globals().update(CardEmbossingLoader('scms_card_embossing_file_ingestion_config.yaml', f'{settings.DAGS_FOLDER}/scms_card_embossing/config').create_dags())  # pragma: no cover
