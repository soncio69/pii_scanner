import oracledb
from contextlib import contextmanager
from typing import Generator
import logging

logger = logging.getLogger(__name__)


class OracleConnector:
    def __init__(self, user: str, password: str, dsn: str, pool_alias: str = None):
        self.user = user
        self.password = password
        self.dsn = dsn
        # Use unique pool alias per user to avoid conflicts
        self.pool_alias = pool_alias or f"pool_{user.lower()}"
        self._pool = None

    def _create_pool(self):
        """Create connection pool for efficiency"""
        if self._pool is None:
            # Check if pool already exists (from previous run) and close it first
            try:
                existing_pool = oracledb.get_pool(self.pool_alias)
                existing_pool.close()
            except oracledb.Error:
                pass  # Pool doesn't exist, which is fine

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
        try:
            if self._pool:
                self._pool.close()
        except Exception:
            pass  # Ignore close errors
        finally:
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