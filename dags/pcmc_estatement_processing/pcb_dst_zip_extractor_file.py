# for airflow scanning
from pcmc_estatement_processing.pcb_dst_zip_extractor_file_extract import BroadridgeSTMTOutputFileCreation

globals().update(BroadridgeSTMTOutputFileCreation('pcb_dst_zip_extractor_file_creation.yaml').create_dags())
