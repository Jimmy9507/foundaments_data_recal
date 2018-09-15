import datetime
from collections import OrderedDict
from typing import Dict

from sqlbuilder.smartsql import T, func

from config import get_source_connect, get_timeslot, get_dest_connect
from .codemap import comecode_map, stockcode_map
from .conn import MySQLDictCursorWrapper
from .createtable import create_research_quarter, \
    create_prepare_quarter, create_strategy_quarter
from .metrics import QUARTER_TABLES_MAP, query, research_quarter, \
    prepare_quarter, strategy_quarter


def _all_mtime(start_date):
    ret = {}
    src_conn = get_source_connect()
    for table, clazz in QUARTER_TABLES_MAP.items():
        if start_date is None:
            sql = 'select distinct date_format(mtime, \'%Y-%m-%d\') as ' \
                  'modified_time from {0} order by mtime ' \
                  'asc;'.format(clazz.name_(), start_date)
        else:
            sql = 'select distinct date_format(mtime, \'%Y-%m-%d\') as ' \
                  'modified_time from {0} where mtime >= \'{1}\' ' \
                  'order by mtime asc;'.format(clazz.name_(), start_date)
        with MySQLDictCursorWrapper(src_conn) as src_cursor:
            src_cursor.execute(sql)
            ret[table] = src_cursor.fetchall()
    src_conn.close()
    return ret


def _get_start_date():
    timeslot = get_timeslot()
    if timeslot < 0:
        return None

    current_date = datetime.datetime.now().date()
    start_date = current_date - datetime.timedelta(days=timeslot)
    return start_date.strftime('%Y-%m-%d')


def _insert_record(cursor: MySQLDictCursorWrapper, dest_quarter: T,
                   record: Dict, duplicate_update=True):
    # on_duplicate_key_update will take more time to invoke insert function.
    if duplicate_update:
        insert_sql, insert_params = query.tables(dest_quarter).insert(
            record,
            on_duplicate_key_update=record
        )
    else:
        insert_sql, insert_params = query.tables(dest_quarter).insert(record)
    cursor.execute(insert_sql, insert_params)  # auto commit


def _import_quarter(src_quarter: T, dest_quarter: T):
    """:param all_update, if it is True, then importing all data from
    research_quarter; Otherwise, importing latest quarter record for each stock
    from research_quarter"""
    all_update = get_timeslot() < 0

    dest_conn = get_dest_connect()
    src_conn = get_dest_connect()
    percent = 0
    for _, order_book_id in stockcode_map().items():
        percent += 1
        print("{0} {1} import data finished {2:.2f}%"
              .format(datetime.datetime.now(), dest_quarter,
                      percent / len(stockcode_map()) * 100))
        with MySQLDictCursorWrapper(src_conn) as src_cursor:
            if all_update:
                select_sql, select_params = query.fields('*').tables(
                    src_quarter).where(
                    src_quarter.stockcode == order_book_id
                ).select()
            else:
                # The normal processing method is to get current max end_date
                # from dest_quarter and then query all records which are larger
                # than the max end_date from src_quarter. Since it is
                # quarter-level data, it is enough to get latest record from
                # src_quarter.
                select_sql, select_params = query.fields('*').tables(
                    src_quarter).where(
                    src_quarter.stockcode == order_book_id
                ).order_by(
                    src_quarter.end_date.desc()
                ).limit(1).select()
            src_cursor.execute(select_sql, select_params)
            with MySQLDictCursorWrapper(dest_conn) as dest_cursor:
                for record in src_cursor:
                    _insert_record(dest_cursor, dest_quarter, record)
    src_conn.close()
    dest_conn.close()


class ResearchQuarter(object):
    """
    Get data from genius database and store data into research_quarter
    table.

    This table cannot be used for backtest or paper trading since
    announce dates are not correct for late announcement quarter record.
    """

    def __init__(self):
        self._table_mtime_map = _all_mtime(_get_start_date())
        self._table = research_quarter

    def update(self, first=False):
        # create research_quarter if the table does not exist.
        create_research_quarter()

        self._update_table(first)
        print(datetime.datetime.now(), 'update done.')

        self._remove_null_rptsrc()
        self._fill_announce_date()

    def _remove_null_rptsrc(self):
        """
        quarter research table consists of four tables (income_statement,
        balance_sheet, cash_flow, financial indicator), however financial
        indicator of genius has no rpt_src field. if rpt_src is null in one of
        records in quarter research, it means this record only comes from
        financial indicator not other three tables and it is invalid, so we
        delete it
        """
        dest_conn = get_dest_connect()
        delete_sql, param = query.tables(self._table).where(
            self._table.rpt_src == None).delete()
        with MySQLDictCursorWrapper(dest_conn) as dest_cursor:
            dest_cursor.execute(delete_sql, param)
        dest_conn.close()

    def _fill_announce_date(self):
        """
        handle record whose announcement date or declare date was missing.

        you can refer to the case: http://jira.ricequant.com/browse/ENG-2442
        to get more detail requirements of handling this kind of records.
        """
        dest_conn = get_dest_connect()
        src_conn = get_dest_connect()
        for _, order_book_id in stockcode_map().items():
            print(datetime.datetime.now(),
                  "adjust announce date for {}".format(order_book_id))
            select_sql, select_params = query.fields(
                self._table.stockcode, self._table.end_date,
                self._table.comcode, self._table.announce_date,
                self._table.rpt_quarter, self._table.rpt_year
            ).tables(
                self._table
            ).where(
                self._table.stockcode == order_book_id
            ).order_by(
                self._table.end_date.desc()
            ).select()
            with MySQLDictCursorWrapper(src_conn) as src_cursor:
                src_cursor.execute(select_sql, select_params)
                adjust_announce_date = AnnounceDateAdjustement(src_cursor)
                values = adjust_announce_date.values()
                if len(values) != 0:
                    with MySQLDictCursorWrapper(dest_conn) as dest_cursor:
                        insert_sql, insert_params = query.fields(
                            self._table.stockcode,
                            self._table.comcode,
                            self._table.end_date,
                            self._table.announce_date,
                            self._table.announce_to
                        ).tables(self._table).insert(
                            values=values,
                            on_duplicate_key_update=OrderedDict((
                                (self._table.announce_date,
                                 func.VALUES(self._table.announce_date)),
                                (self._table.announce_to,
                                 func.VALUES(self._table.announce_to))
                            ))
                        )
                        dest_cursor.execute(insert_sql, insert_params)
        src_conn.close()
        dest_conn.close()

    def _update_by_mtime(self):
        src_conn = get_source_connect()
        src_cursor = src_conn.cursor(dictionary=True)
        for table, clazz in QUARTER_TABLES_MAP.items():
            mtimes = self._table_mtime_map[table]
            while len(mtimes) != 0:
                select_sql, select_param = query.fields(
                    clazz.metrics()
                ).tables(
                    table
                ).where(
                    clazz.filter_conditions_() &
                    table.mtime.contains(mtimes.pop()['modified_time'])
                ).select()
                src_cursor.execute(select_sql, select_param)
                self._exec_update(src_cursor.fetchall())
        src_cursor.close()
        src_conn.close()

    def _first_update(self):
        src_conn = get_source_connect()
        src_cursor = src_conn.cursor(dictionary=True)
        comcodes = comecode_map()
        for comcode in comcodes:
            print(datetime.datetime.now(), comcode)
            merged_records = {}
            for table, clazz in QUARTER_TABLES_MAP.items():
                select_sql, select_param = query.fields(
                    clazz.metrics()
                ).tables(
                    table
                ).where(
                    (clazz.comcode == comcode) &
                    clazz.filter_conditions_()
                ).select()
                src_cursor.execute(select_sql, select_param)
                records = src_cursor.fetchall()
                for record in records:
                    enddate = record.get('end_date')
                    kept_record = merged_records.get((comcode, enddate))
                    if kept_record is None:
                        merged_records[(comcode, enddate)] = record
                    else:
                        kept_record.update(record)
            self._exec_update(merged_records.values(), duplicate_update=False)
        src_cursor.close()
        src_conn.close()

    def _update_table(self, first):
        self._first_update() if first else self._update_by_mtime()

    def _exec_update(self, update_records, duplicate_update=True):
        dest_conn = get_dest_connect()
        with MySQLDictCursorWrapper(dest_conn) as dest_cursor:
            for record in update_records:
                update_record = self._clear_record(record)
                if not update_record or len(update_record) == 0:
                    continue
                _insert_record(dest_cursor, self._table, update_record,
                               duplicate_update)
        dest_conn.close()

    @staticmethod
    def _clear_record(record: Dict) -> Dict:
        comcode = record.get("comcode")
        stockcode = comecode_map().get(comcode)

        # this stockcode is not in our interesting instruments
        if not stockcode:
            return None

        # handle revenue and operating_revenue for
        # http://jira.ricequant.com/browse/ENG-2449
        if record.get("revenue") == 0 and record.get("operating_revenue"):
            del record["revenue"]

        update_record = dict()
        for key, value in record.items():
            # update_record does not contain None value record.
            # and stockcode is from comcode_map based on comcode, not query
            # record.
            if not value or key == 'stockcode':
                pass
            elif key in ('rpt_src',):
                update_record[key] = value
            elif key in ('comcode',):  # comcode must be in query record.
                update_record[key] = value
                updated_code = stockcode_map().get(stockcode)
                if not updated_code:
                    raise ValueError(
                        "Impossible to get none stockcode from stockcode map")
                update_record['stockcode'] = updated_code
            elif key in ('announce_date', 'end_date',):
                date = int(value.strftime("%Y%m%d"))
                update_record[key] = date
                if key == 'end_date':
                    update_record['rpt_year'] = date / 10000
                    update_record['rpt_quarter'] = (date % 10000) / 300
            else:
                update_record[key] = float(value)
        return update_record


class PrepareQuarter(object):
    def __init__(self):
        self._table = prepare_quarter

    def update(self):
        create_prepare_quarter()
        self._import_quarter()
        self._remove_late_announce_records()

    def _import_quarter(self):
        """import all records from research_quarter"""
        _import_quarter(research_quarter, self._table)

    def _remove_late_announce_records(self):
        """
        remove record which is late to announce it. If one record's
        announce_date is equal to or larger than that of its next latter
        quarter record, then this record is so called late announcement record.
        """
        dest_conn = get_dest_connect()
        src_conn = get_dest_connect()
        percent = 0
        for _, order_book_id in stockcode_map().items():
            percent += 1
            print("{0} prepare_quarter remove late announce records finished "
                  "{1:.2f}%".format(datetime.datetime.now(),
                                    percent / len(stockcode_map()) * 100))
            select_sql, select_params = query.fields(
                self._table.stockcode, self._table.end_date,
                self._table.announce_date, self._table.comcode
            ).tables(
                self._table
            ).where(
                self._table.stockcode == order_book_id
            ).order_by(
                self._table.end_date.desc()
            ).select()
            with MySQLDictCursorWrapper(src_conn) as src_cursor:
                src_cursor.execute(select_sql, select_params)
                latest_ann_date = 29991231
                last_deleted = False
                with MySQLDictCursorWrapper(dest_conn) as dest_cursor:
                    for record in src_cursor:
                        ann_date = record.get("announce_date")
                        enddate = record.get("end_date")
                        if latest_ann_date <= ann_date:
                            delete_sql, delete_params = query.tables(
                                self._table
                            ).where(
                                (self._table.stockcode == order_book_id) &
                                (self._table.end_date == enddate)
                            ).delete()
                            dest_cursor.execute(delete_sql, delete_params)
                            last_deleted = True
                            print(
                                "deleted record: stockcode = {}, end_date = {},"
                                " announce_date = {}".format(order_book_id,
                                                             enddate,
                                                             ann_date))
                        else:
                            if last_deleted:
                                update_sql, update_params = query.tables(
                                    self._table
                                ).where(
                                    (self._table.stockcode == order_book_id) &
                                    (self._table.end_date == enddate)
                                ).update({
                                    self._table.announce_to: latest_ann_date
                                })
                                dest_cursor.execute(update_sql, update_params)
                                last_deleted = False
                            latest_ann_date = ann_date
        src_conn.close()
        dest_conn.close()


class StrategyQuarter(object):
    def __init__(self):
        self._table = strategy_quarter

    def update(self):
        create_strategy_quarter()
        self._import_quarter()
        self._update_announce_date()

    def _import_quarter(self):
        _import_quarter(prepare_quarter, self._table)

    def _update_announce_date(self):
        """
        update announce date. It is necessary to update announce_to for newly
        quarter report in prepare_quarter
        """
        src_conn = get_dest_connect()
        dest_conn = get_dest_connect()
        for _, order_book_id in stockcode_map().items():
            select_sql, select_params = query.fields(
                prepare_quarter.stockcode, prepare_quarter.end_date,
                prepare_quarter.announce_to, prepare_quarter.comcode
            ).tables(
                prepare_quarter
            ).where(
                prepare_quarter.stockcode == order_book_id
            ).order_by(
                prepare_quarter.end_date.desc()
            ).select()
            with MySQLDictCursorWrapper(src_conn) as src_cursor:
                src_cursor.execute(select_sql, select_params)
                update_records = [(
                                      record.get('stockcode'),
                                      record.get('end_date'),
                                      record.get('announce_to'),
                                      record.get('comcode')
                                  ) for record in src_cursor]
                if len(update_records) == 0:
                    continue
                with MySQLDictCursorWrapper(dest_conn) as dest_cursor:
                    insert_sql, insert_params = query.fields(
                        self._table.stockcode, self._table.end_date,
                        self._table.announce_to, self._table.comcode
                    ).tables(self._table).insert(
                        values=update_records,
                        on_duplicate_key_update=OrderedDict((
                            (self._table.announce_to,
                             func.VALUES(self._table.announce_to)),
                        ))
                    )
                    dest_cursor.execute(insert_sql, insert_params)
        dest_conn.close()
        src_conn.close()


class AnnounceDateAdjustement(object):
    """
    fill the missing announcement/declare date by following criteria.

    The last announcement date of each quarter report:
    first quarter:  April 1 - April 30
    second quarter: July 1 - Aug 31
    third quarter:  Oct. 1 - Oct. 31
    fourth quarter: Jan. 1 - April 30

    Criteria to fill the missing announcement date is based on the rule of the
    last announcement date of each quarter report. The criteria is as follows:
    first quarter: April 30
    second quarter: Aug 31
    third quarter: Oct. 31
    fourth quarter: Default: April 30, but if the first quarter in later year
                    exists, then the first quarter's announcement date is this
                    fourth quarter report's announcement date.
    """

    def __init__(self, record_cursor: MySQLDictCursorWrapper):
        self._record_cursor = record_cursor
        self._previous_record = None
        self._values = list()
        self._quarter = {
            1: self.first_quarter,
            2: self.second_quarter,
            3: self.third_quarter,
            4: self.fourth_quarter,
        }

    @staticmethod
    def parse_record(record):
        stockcode = record.get("stockcode")
        comcode = record.get("comcode")
        enddate = record.get("end_date")
        rpt_year = record.get("rpt_year")
        rpt_quarter = record.get("rpt_quarter")
        if not stockcode:
            raise ValueError("Missing stockcode! record: {}".format(record))
        if not comcode:
            raise ValueError("Missing comcode! record: {}".format(record))
        if not enddate:
            raise ValueError("Missing end_date! record: {}".format(record))
        if not rpt_year:
            raise ValueError("Missing rpt_year! record: {}".format(record))
        if not rpt_quarter:
            raise ValueError("Missing rpt_quarter! record: {}".format(record))
        return stockcode, comcode, enddate, record.get("announce_date"), \
               record.get("announce_to"), rpt_year, rpt_quarter

    def quarter(self, record):
        """fill announce_to with previous record's announce_date"""
        # previous record means the quarter report after current quarter
        # report, for example, 2016's second quarter report is the previous
        # quarter report of 2016's first quarter.
        *_, announce_date, _, rpt_year, rpt_quarter = self.parse_record(
            record)
        if not announce_date:
            ann_date = self._quarter.get(rpt_quarter)(
                rpt_year, not self._previous_record
            )
            record["announce_date"] = ann_date

        if not self._previous_record:
            record["announce_to"] = 29991231
        else:
            record["announce_to"] = self._previous_record["announce_date"]

        # check if announce date/to has value
        if not record.get("announce_date"):
            raise ValueError(
                "Missing announce date! record: {}".format(record))
        if not record.get("announce_to"):
            raise ValueError("Missing announce to! record: {}".format(record))
        self._previous_record = record
        return record

    @staticmethod
    def first_quarter(rpt_year, init=False):
        return int(str(rpt_year) + "0430")

    @staticmethod
    def second_quarter(rpt_year, init=False):
        return int(str(rpt_year) + "0831")

    @staticmethod
    def third_quarter(rpt_year, init=False):
        return int(str(rpt_year) + "1031")

    def fourth_quarter(self, rpt_year, init=False):
        rpt_year += 1
        ann_date = int(str(rpt_year) + "0430")
        if init:
            # latest fourth quarter exists this year, but missing
            # announce_date. we need to fill the announce_date with current
            # date if current date is less than this year april 30th.
            now = int(datetime.datetime.now().strftime('%Y%m%d'))
            if int(str(rpt_year) + "0101") < now < ann_date:
                ann_date = now
        else:
            # check if previous report is first quarter report. If it is, we
            # use the announce date of the first quarter report as announce
            # date of this fourth quarter report.
            pre_rpt_quarter = self._previous_record.get("rpt_quarter")
            pre_rpt_year = self._previous_record.get("rpt_year")
            if pre_rpt_quarter == 1 and pre_rpt_year == rpt_year:
                ann_date = self._previous_record.get("announce_date")
        return ann_date

    def values(self):
        for record in self._record_cursor:
            record = self.quarter(record)
            stockcode, comcode, enddate, ann_date, ann_to, *_ = \
                self.parse_record(record)
            self._values.append((
                stockcode, comcode, enddate, ann_date, ann_to
            ))
        return self._values


def update_quarter(first=False):
    research_handler = ResearchQuarter()
    research_handler.update(first)
    handlers = [PrepareQuarter(), StrategyQuarter()]
    for handler in handlers:
        handler.update()
