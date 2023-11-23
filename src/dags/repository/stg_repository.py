from typing import List, Dict
from datetime import datetime
from psycopg.rows import class_row
from pydantic import BaseModel
import json
import uuid

from lib.pg import PgConnect
from lib.vertica import VerticaConnect

class Transactions(BaseModel):
    operation_id: uuid.UUID
    account_number_from: int
    account_number_to: int
    currency_code: int
    country: str
    status: str
    transaction_type: str
    amount: int
    transaction_dt: datetime
    class Config:
        orm_mode = True

class Currencies(BaseModel):
    date_update: datetime
    currency_code: int
    currency_code_with: int
    currency_code_div: float
    class Config:
        orm_mode = True

class TransactionServiceRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
        
    # Метод для построения части SQL-запроса
    def build_common_sql(self, object_type: str, sent_dttm: str) -> str:
        return f"""
            FROM stg.transaction_service
            WHERE cast(sent_dttm as date) = cast('{sent_dttm}' as date)
            AND object_type = '{object_type}'
            LIMIT 10000;
        """

    def get_transactions(self, sent_dttm: str) -> List[Transactions]:
        sql = self.build_common_sql('TRANSACTION', sent_dttm)
        
        with self._db.client().cursor(row_factory=class_row(Transactions)) as cur:
            cur.execute(
                """
                    SELECT  payload::json->>'operation_id' operation_id,
                            (payload::json->>'account_number_from')::int account_number_from,
                            (payload::json->>'account_number_to')::int account_number_to,
                            (payload::json->>'currency_code')::int currency_code,
                            payload::json->>'country' country,
                            payload::json->>'status' status,
                            payload::json->>'transaction_type' transaction_type,
                            (payload::json->>'amount')::int amount,
                            (payload::json->>'transaction_dt')::timestamp transaction_dt
                """ + sql
            )
            objs = cur.fetchall()
        return objs
    
    def get_currencies(self, sent_dttm: str) -> List[Currencies]:
        sql = self.build_common_sql('CURRENCY', sent_dttm)
        
        with self._db.client().cursor(row_factory=class_row(Currencies)) as cur:
            cur.execute(
                """
                    SELECT  (payload::json->>'date_update')::timestamp date_update,
                            (payload::json->>'currency_code')::int currency_code,
                            (payload::json->>'currency_code_with')::int currency_code_with,
                            (payload::json->>'currency_with_div')::float currency_code_div
                """ + sql
            )
            objs = cur.fetchall()
        return objs
    
class StgRepository:
    def __init__(self, conn: VerticaConnect) -> None:
        self._db = conn

    def transactions_insert(self, tr: Transactions) -> List[Transactions]:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                        MERGE INTO STV2023081257__STAGING.transactions AS tgt 
                        USING (
                        SELECT  cast('{tr.operation_id}' as uuid) operation_id, 
                                {tr.account_number_from} account_number_from, 
                                {tr.account_number_to} account_number_to, 
                                {tr.currency_code} currency_code, 
                                '{tr.country}' country, 
                                '{tr.status}' status, 
                                '{tr.transaction_type}' transaction_type,
                                {tr.amount} amount, 
                                cast('{tr.transaction_dt}' as timestamp) transaction_dt
                        ) AS src 
                        ON (tgt.operation_id = src.operation_id)
                        WHEN MATCHED
                        THEN UPDATE SET account_number_from=src.account_number_from, 
                                        account_number_to=src.account_number_to, 
                                        currency_code=src.currency_code, 
                                        country=src.country, 
                                        status=src.status, 
                                        transaction_type=src.
                                        transaction_type, 
                                        amount=src.amount, 
                                        transaction_dt=src.transaction_dt 
                        WHEN NOT MATCHED 
                        THEN INSERT (operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt) 
                        VALUES (src.operation_id, src.account_number_from, src.account_number_to, src.currency_code, src.country, src.status, src.transaction_type, src.amount, src.transaction_dt);
                        
                    """
                )

    def currencies_insert(self, cr: Currencies) -> List[Currencies]:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                        MERGE INTO STV2023081257__STAGING.currencies AS tgt
                        USING (
                        SELECT  cast('{cr.date_update}' as timestamp) date_update, 
                                {cr.currency_code} currency_code, 
                                {cr.currency_code_with} currency_code_with, 
                                {cr.currency_code_div} currency_code_div
                        ) AS src 
                        ON (tgt.date_update=src.date_update and tgt.currency_code=src.currency_code)
                        WHEN MATCHED
                        THEN UPDATE SET currency_code_with=src.currency_code_with, 
                                        currency_code_div=src.currency_code_div 
                        WHEN NOT MATCHED THEN INSERT (date_update, currency_code, currency_code_with, currency_code_div)
                        VALUES (src.date_update, src.currency_code, src.currency_code_with, src.currency_code_div);
                    """
                )