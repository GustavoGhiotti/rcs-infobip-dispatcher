import pyodbc
import time
import random
from typing import List, Tuple, Any

from src.shared.utils.env_load import (
    DB_USER,
    DB_PASS,
    DB_66,
    DB_ENCRYPT,
    DB_TRUST_SERVER_CERTIFICATE,
)
from src.infrastructure.database.base_connection import BaseConnection


class SQLServerConnection(BaseConnection):
    def __init__(self, server: str, database: str):
        self._server = server
        self._database = database

    def _connect(self) -> pyodbc.Connection:
        return pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={self._server};"
            f"DATABASE={self._database};"
            f"UID={DB_USER};"
            f"PWD={DB_PASS};"
            f"Encrypt={DB_ENCRYPT};"
            f"TrustServerCertificate={DB_TRUST_SERVER_CERTIFICATE};"
            "Connection Timeout=30;"
        )

    def execute(self, *, query: str, parametros: tuple, fetch: bool = False):
        conexao = None

        for attempt in range(3):
            try:
                conexao = self._connect()
                with conexao.cursor() as cursor:
                    cursor.execute(query, parametros)

                    if fetch:
                        colunas = [c[0] for c in cursor.description]
                        return [dict(zip(colunas, linha)) for linha in cursor.fetchall()]

                    conexao.commit()
                    return cursor.rowcount

            except pyodbc.Error as exc:
                print(f"Erro no banco: {exc}")

                if "1205" in str(exc):
                    if conexao:
                        conexao.rollback()

                    if attempt < 2:
                        time.sleep(0.5 + random.random())
                        continue

                raise

            finally:
                if conexao is not None:
                    try:
                        conexao.close()
                    except Exception:
                        pass

    def execute_many(self, *, query: str, parametros_list: List[Tuple[Any, ...]]) -> None:
        if not parametros_list:
            return

        random.shuffle(parametros_list)
        max_tentativas = 5
        delay = 1
        conexao = None
        cursor = None

        try:
            for tentativa in range(1, max_tentativas + 1):
                try:
                    if not conexao:
                        conexao = self._connect()
                        cursor = conexao.cursor()
                        cursor.execute("SET DEADLOCK_PRIORITY LOW")
                        cursor.execute("SET LOCK_TIMEOUT 2000")

                    cursor.executemany(query, parametros_list)
                    conexao.commit()
                    return

                except pyodbc.Error as exc:
                    msg = str(exc).lower()
                    if "1205" in msg or "deadlock" in msg or "timeout" in msg:
                        print(
                            f"[WARN] Deadlock/lock timeout - tentativa "
                            f"{tentativa}/{max_tentativas}"
                        )
                        if conexao:
                            conexao.rollback()
                        time.sleep(delay)
                        delay *= 2
                        continue

                    if conexao:
                        conexao.rollback()
                    print(f"[ERRO BANCO] Falha definitiva: {exc}")
                    raise

                finally:
                    if tentativa == max_tentativas:
                        print(
                            "[FALHA] Nao foi possivel executar o batch "
                            "apos multiplas tentativas."
                        )
        finally:
            if conexao:
                try:
                    conexao.close()
                except Exception:
                    pass


def get_connection():
    if not DB_66 or not "DB_SMS":
        raise Exception("Database config incompleta no .env")
    return SQLServerConnection(DB_66, "DB_SMS")._connect()
