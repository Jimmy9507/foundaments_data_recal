from typing import List, Dict

from pandas import read_csv
from sqlbuilder.smartsql import Field

from config import get_inst_files, get_source_connect
from fdhandle.conn import MySQLDictCursorWrapper
from fdhandle.metrics import query, stk_code


def get_stockcode_map() -> Dict:
    filenames = get_inst_files()

    stockcodes = dict()
    for file in filenames:
        df = read_csv(file)
        for order_book_id in set(df.OrderBookID):
            stockcodes[order_book_id.split(".")[0]] = order_book_id
    return stockcodes


def get_orderbookids():
    ret = set()
    filenames = get_inst_files()
    for file in filenames:
        df = read_csv(file)
        ret |= set(df.OrderBookID.values)
    return list(ret)


def _get_code_map(*fields: List[Field]):
    stockcodes = set(get_stockcode_map().keys())
    sql, params = query.fields(
        fields
    ).tables(
        stk_code
    ).where(
        stk_code.stockcode.in_(tuple(stockcodes))
    ).select()
    return sql, params


def get_comcode_map():
    sql, params = _get_code_map(stk_code.comcode, stk_code.stockcode)
    src_conn = get_source_connect()
    with MySQLDictCursorWrapper(src_conn) as cursor:
        cursor.execute(sql, params)
        result = cursor.fetchall()
        code_map = {}
        for r in result:
            code_map[r["comcode"]] = r["stockcode"]
    src_conn.close()
    return code_map


def get_innercode_map():
    sql, params = _get_code_map(stk_code.inner_code, stk_code.stockcode)
    src_conn = get_source_connect()
    with MySQLDictCursorWrapper(src_conn) as cursor:
        cursor.execute(sql, params)
        result = cursor.fetchall()
        code_map = {}
        for r in result:
            code_map[r["inner_code"]] = r["stockcode"]
    src_conn.close()
    return code_map

