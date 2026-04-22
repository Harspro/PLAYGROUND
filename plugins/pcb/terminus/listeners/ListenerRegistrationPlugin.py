from __future__ import annotations

from pcb.terminus.listeners import DagFailureListener
from airflow.plugins_manager import AirflowPlugin


class ListenerRegistrationPlugin(AirflowPlugin):
    name = "ListenerRegistrationPlugin"
    listeners = [DagFailureListener]
