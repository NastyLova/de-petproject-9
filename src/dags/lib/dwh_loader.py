from logging import Logger
from typing import List

from lib.pg import PgConnect
from lib.vertica import VerticaConnect
from repository.dwh_repository import GlobalMetricsRepository

class DwhLoader:

    def __init__(self, vt_conn: VerticaConnect, log: Logger) -> None:
        self.target = GlobalMetricsRepository(vt_conn)
        self.log = log

    def load_to_dwh(self, sent_dttm):
        
        self.log.info(f"Start to load data into mart.")
        load_queue_mart = self.target.get_global_metrics(sent_dttm)
            
        if load_queue_mart:
            for gm in load_queue_mart:
                self.target.global_metrics_insert(gm)
            self.log.info(f"Load mart finished")
        