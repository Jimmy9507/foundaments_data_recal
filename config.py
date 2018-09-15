import yaml
from mysql.connector.pooling import PooledMySQLConnection
from typing import List

from fdhandle.conn import create_conn_pool, create_conn

_config = None
_src_cnx_pool = None
_dest_cnx_pool = None


def _default_conf():
    import os
    return os.path.join(os.path.dirname(__file__), 'fdhandle.yaml')


class RQConfig:
    def __init__(self, config_file=None):
        if not config_file:
            config_file = _default_conf()
        self._conf = yaml.load(open(config_file, "rb"))

    def get(self, path):
        """
        :param path: data.source
        :return:
        """
        elements = path.split('.')
        conf = self._conf
        for i in range(len(elements)):
            conf = conf[elements[i]]
        return conf


def init_with(conf):
    global _config
    _config = conf


def _check_inited(func):
    def wrap(*args, **kwargs):
        global _config
        if not _config:
            raise RuntimeError(
                'Not inited yet. Please call fdhandle.init() first.')
        return func(*args, **kwargs)

    return wrap


@_check_inited
def get_source_connect() -> PooledMySQLConnection:
    global _config, _src_cnx_pool
    conf = _config.get("data.source")

    if _src_cnx_pool is None:
        _src_cnx_pool = create_conn_pool(conf, 'src_pool')
    return _src_cnx_pool.get_connection()


@_check_inited
def get_dest_connect(from_pool=True) -> PooledMySQLConnection:
    global _config, _dest_cnx_pool
    conf = _config.get("data.dest")

    if from_pool:
        if _dest_cnx_pool is None:
            _dest_cnx_pool = create_conn_pool(conf, 'dest_pool')
        return _dest_cnx_pool.get_connection()
    else:
        return create_conn(conf)


@_check_inited
def get_timeslot() -> int:
    global _config
    return int(_config.get("update.timeslot"))


@_check_inited
def get_inst_files() -> List:
    global _config
    return _config.get("instruments")
