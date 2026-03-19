import os
from contextlib import contextmanager
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv
from logger import setup_logger

load_dotenv()
log = setup_logger('layer1')

_pool = None


def _conninfo() -> str:
    return (
        f"host={os.getenv('DB_HOST', 'localhost')} "
        f"port={os.getenv('DB_PORT', '5432')} "
        f"dbname={os.getenv('DB_NAME', 'trading_system')} "
        f"user={os.getenv('DB_USER', 'postgres')} "
        f"password={os.getenv('DB_PASSWORD', '')}"
    )


def init_pool():
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            conninfo=_conninfo(),
            min_size=1,
            max_size=5,
        )
        log.info("PostgreSQL connection pool initialised (1–5 conns)")
    return _pool


@contextmanager
def db_connection():
    p = init_pool()
    with p.connection() as conn:
        yield conn


def close_pool():
    global _pool
    if _pool:
        _pool.close()
        _pool = None
        log.info("PostgreSQL connection pool closed")
