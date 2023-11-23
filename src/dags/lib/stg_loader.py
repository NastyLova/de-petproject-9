from logging import Logger
from typing import List

from lib.pg import PgConnect
from lib.vertica import VerticaConnect
from repository.stg_repository import TransactionServiceRepository, StgRepository

class StgLoader:

    def __init__(self, pg_conn: PgConnect, vt_conn: VerticaConnect, log: Logger) -> None:
        self.source = TransactionServiceRepository(pg_conn)
        self.target = StgRepository(vt_conn)
        self.log = log

    def load_to_stg(self, sent_dttm):
        
        self.log.info(f"Start to load data into stg schema.")
        load_queue_transactions = self.source.get_transactions(sent_dttm)
        load_queue_currencies = self.source.get_currencies(sent_dttm)
            
        if load_queue_transactions:
            for tr in load_queue_transactions:
                self.target.transactions_insert(tr)
            self.log.info(f"Load transactions finished")
        
        if load_queue_currencies:
            for cr in load_queue_currencies:
                self.target.currencies_insert(cr)
            self.log.info(f"Load currencies finished")
        