from typing import Dict

from multiprocessing import current_process
from mysql.connector import pooling, connect
from mysql.connector.cursor import MySQLCursorDict

_cnx_pool = dict()


def create_conn_pool(conf: Dict, name: str):
    _pool = pooling.MySQLConnectionPool(pool_name=name,
                                        pool_size=5,
                                        buffered=False,
                                        autocommit=True,
                                        # raise_on_warnings=True,
                                        **conf)
    print(current_process().pid, 'create pool', name)
    # traceback.print_stack()
    return _pool


def create_conn(conf: Dict):
    print(conf)
    return connect(**conf)


class MySQLDictCursorWrapper(MySQLCursorDict):
    def __init__(self, connection):
        super().__init__(connection)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
