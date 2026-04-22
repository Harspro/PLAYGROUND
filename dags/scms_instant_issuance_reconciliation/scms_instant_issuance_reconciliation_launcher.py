# for airflow scanning
from airflow import settings

from scms_instant_issuance_reconciliation.scms_instant_issuance_reconciliation import InstantIssuanceReconciliation

globals().update(InstantIssuanceReconciliation('scms_instant_issuance_reconciliation_config.yaml',
                                               f'{settings.DAGS_FOLDER}/scms_instant_issuance_reconciliation/config').generate_dags())
