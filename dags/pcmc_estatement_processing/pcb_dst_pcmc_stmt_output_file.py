# for airflow scanning
from pcmc_estatement_processing.pcb_dst_pcmc_stmt_output_file_extract import OutputFileCreation

globals().update(OutputFileCreation('pcb_dst_pcmc_stmt_output_file_creation.yaml').create_dags())
