from typing import List, Dict
from datetime import datetime
from psycopg.rows import class_row
from pydantic import BaseModel
import json
import uuid

from lib.pg import PgConnect
from lib.vertica import VerticaConnect

class GlobalMetrics(BaseModel):
    date_update: datetime
    currency_from: int
    cnt_transactions: int
    cnt_accounts_make_transactions: int
    avg_transactions_per_account: float
    amount_total: float
    class Config:
        orm_mode = True

class GlobalMetricsRepository:
    def __init__(self, conn: VerticaConnect) -> None:
        self._db = conn

    def get_global_metrics(self, sent_dttm: str) -> List[GlobalMetrics]:
        with open('queries/select_global_metrics.sql', 'r') as file:
            sql = file.read()
            
        with self._db.client().cursor() as cur:
            cur.execute(sql, [sent_dttm])
            objs = cur.fetchall()
        return objs
    
    def global_metrics_insert(self, gm: GlobalMetrics) -> List[GlobalMetrics]:
        with open('queries/insert_global_metrics.sql', 'r') as file:
            sql = file.read()
            
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, gm)
