# for airflow scanning
from airflow import DAG
from data_transfer.vendor_data_transfer.landing_to_business_data_transfer_inbound_job import LandingToBusinessInboundFileTransfer

globals().update(LandingToBusinessInboundFileTransfer('medallia_inbound_file_transfer_config.yaml', 'medallia').create())
