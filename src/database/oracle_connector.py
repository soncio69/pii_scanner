import oracledb
from contextlib import contextmanager
from typing import Generator
import logging

logger = logging.getLogger(__name__)


class OracleConnector:
    def __init__(self, user: str, password: str, dsn: str, pool_alias: str = "default"):
        self.user = user
        self.password = password
        self.dsn = dsn
        self.pool_alias = pool_alias
        self._pool = None

    def _create_pool(self):
        """Create connection pool for efficiency"""
        if self._pool is None:
            self._pool = oracledb.create_pool(
                user=self.user,
                password=self.password,
                dsn=self.dsn,
                min=2,
                max=5,
                pool_alias=self.pool_alias
            )
        return self._pool

    @contextmanager
    def get_connection(self) -> Generator[oracledb.Connection, None, None]:
        """Context manager for database connections"""
        pool = self._create_pool()
        conn = pool.acquire()
        try:
            yield conn
        finally:
            pool.release(conn)

    def close(self):
        if self._pool:
            self._pool.close()
            self._pool = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def build_dsn(host: str, port: int, service_name: str) -> str:
    """Build Oracle DSN string"""
    return f"{host}:{port}/{service_name}"


if __name__ == "__main__":
    # Quick test - would need real credentials
    print("Oracle connector module loaded")