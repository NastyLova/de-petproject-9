from contextlib import contextmanager
from typing import Generator

import vertica_python
from vertica_python import Connection
from airflow.hooks.base import BaseHook

class VerticaConnect:
    def __init__(self, host: str, port: int, database: str, login: str, password: str) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.login = login
        self.password = password

    def params(self) -> str:
        return {"host": self.host,
                "port": self.port,
                "user": self.login,
                "database": self.database,
                "password": self.password}
    
    def client(self):
        return vertica_python.connect(**self.params())

    @contextmanager
    def connection(self) -> Generator[Connection, None, None]:
        conn = vertica_python.connect(**self.params())
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.commit()
            raise e
        finally:
            conn.close()

class ConnectionBuilderVertica:

    @staticmethod
    def pg_conn(conn_id: str) -> VerticaConnect:
        conn = BaseHook.get_connection(conn_id)

        vc = VerticaConnect(str(conn.host),
                            str(conn.port),
                            str(conn.schema),
                            str(conn.login),
                            str(conn.password)
                            )

        return vc
