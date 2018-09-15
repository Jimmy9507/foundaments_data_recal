from pkg_resources import resource_filename

from config import get_dest_connect
from fdhandle.conn import MySQLDictCursorWrapper


def _create_quarter(quarter_name: str):
    with open(resource_filename("fdhandle", "sql/quarter.sql"),
              mode="rt") as f:
        create_sql = f.read() % quarter_name
        connect = get_dest_connect(False)
        with MySQLDictCursorWrapper(connect) as cursor:
            cursor.execute(create_sql)
        connect.close()


def _create_day(day_name: str):
    with open(resource_filename("fdhandle", "sql/day.sql"), mode="rt") as f:
        create_sql = f.read() % day_name
        connect = get_dest_connect(False)
        with MySQLDictCursorWrapper(connect) as cursor:
            cursor.execute(create_sql)
        connect.close()


def create_research_quarter():
    _create_quarter("research_quarter")


def create_prepare_quarter():
    _create_quarter("prepare_quarter")


def create_strategy_quarter():
    _create_quarter("strategy_quarter")


def create_orig_day():
    _create_day("orig_day")


def create_recal_day():
    _create_day("recal_day")
