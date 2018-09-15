import fdhandle

# declare date & declare to
from config import get_dest_connect
from fdhandle.conn import MySQLDictCursorWrapper
from fdhandle.metrics import query, strategy_quarter


def verify_declare(order_book_id: str):
    src_conn = get_dest_connect()
    select_sql, select_param = query.fields(
        strategy_quarter.announce_date, strategy_quarter.announce_to,
        strategy_quarter.end_date
    ).tables(strategy_quarter).where(
        strategy_quarter.stockcode == order_book_id
    ).order_by(
        strategy_quarter.end_date.desc()
    ).select()
    with MySQLDictCursorWrapper(src_conn) as src_cursor:
        src_cursor.execute(select_sql, select_param)
        pre_ann_date = None
        for record in src_cursor:
            ann_date = record.get("announce_date")
            ann_to = record.get("announce_to")
            end_date = record.get("end_date")
            if None in (ann_date, ann_to, end_date):
                raise RuntimeError("missing announce_date, "
                                   "announce_to or end_date in record {}"
                                   .format(record))
            if pre_ann_date is None:
                pre_ann_date = ann_date
                continue

            if ann_date >= pre_ann_date:
                raise RuntimeError("announce date in old record is larger "
                                   "than that in new record")
            if ann_to != pre_ann_date:
                raise RuntimeError("Wrong announce to in record {}, pre_ann_date {}"
                                   .format(record, pre_ann_date))

            if end_date >= ann_date:
                raise RuntimeError("announce date < end_date in record {}"
                                   .format(record))
            pre_ann_date = ann_date
    src_conn.close()


if __name__ == '__main__':
    fdhandle.init()
    verify_declare('000001.XSHE')
