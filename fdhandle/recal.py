import datetime
from typing import List, Dict

from multiprocessing import Queue, Process, Lock
from pandas import to_datetime

from config import get_source_connect, get_dest_connect
from .stocks import get_orderbookids
from .codemap import orderbookid_map
from .conn import MySQLDictCursorWrapper
from .createtable import create_orig_day, create_recal_day
from .metrics import Day, strategy_quarter, QUARTER_ENDDATE_MAP, \
    stk_market, orig_day, recal_day, day_fd, query


def _day_metrics(order_book_id: str, latest_date=None):
    innercode = orderbookid_map().get(order_book_id)
    if innercode is None:
        raise RuntimeError("order_book_id %s can not get corresponding inner "
                           "code from pgenius database." % order_book_id)

    condition = (Day.inner_code_ == innercode) & (Day.filter_conditions_())
    if latest_date is not None:
        condition &= (day_fd.trd_date > latest_date)
    src_conn = get_source_connect()
    with MySQLDictCursorWrapper(src_conn) as cursor:
        cursor.execute(
            *query.fields(
                Day.metrics()
            ).tables(
                day_fd
            ).where(
                condition
            ).order_by(
                Day.trade_date.desc()
            ).select()
        )
        ret = cursor.fetchall()
    src_conn.close()
    return ret


def _closing_price(order_book_id):
    innercode = orderbookid_map().get(order_book_id)
    if innercode is None:
        raise RuntimeError("order_book_id %s can not get corresponding inner "
                           "code from pgenius database." % order_book_id)
    src_conn = get_source_connect()
    with MySQLDictCursorWrapper(src_conn) as cursor:
        cursor.execute(
            *query.fields(
                stk_market.tradedate,
                stk_market.tclose
            ).tables(
                stk_market
            ).where(
                (stk_market.inner_code == innercode) &
                (stk_market.isvalid == 1)
            ).order_by(
                stk_market.tradedate.desc()
            ).select()
        )
        ret = cursor.fetchall()
    src_conn.close()
    return ret


def _quarter_metrics(order_book_id: str) -> List[Dict]:
    """
    get quarter metrics from strategy_quarter since this quarter table has
    filled the missing announce date and removed late announce date records.

    Note: Announce date in quarter record is very important for recalculating
    day-level fundamental metrics which are based on it.

    :param order_book_id: string like "000001.XSHE"
    :return: quarter metrics of this stock and result is in end_date descending
             order
    """
    select_sql, select_param = query.fields(
        strategy_quarter.announce_date,
        strategy_quarter.rpt_year,
        strategy_quarter.rpt_quarter,
        strategy_quarter.end_date,

        # metrics
        strategy_quarter.net_profit_parent_company,
        strategy_quarter.net_profit,
        strategy_quarter.operating_revenue,
        strategy_quarter.cash_flow_from_operating_activities,
        strategy_quarter.current_assets,
        strategy_quarter.cash,
        strategy_quarter.cash_equivalent,
        strategy_quarter.interest_bearing_debt,
        strategy_quarter.ebitda,
        strategy_quarter.revenue,
        strategy_quarter.cash_equivalent_inc_net,
        strategy_quarter.book_value_per_share
    ).tables(strategy_quarter).where(
        strategy_quarter.stockcode == order_book_id
    ).order_by(
        strategy_quarter.end_date.desc()
    ).select()
    src_conn = get_dest_connect()
    with MySQLDictCursorWrapper(src_conn) as cursor:
        cursor.execute(select_sql, select_param)
        ret = cursor.fetchall()
    src_conn.close()
    return ret


def _latest_enddates(tradedate: int):
    year, r = divmod(tradedate, 10000)
    year_str = str(year)
    last_year_str = str(year - 1)
    if 101 <= r < 431:
        return [int(year_str + QUARTER_ENDDATE_MAP[1]),
                int(last_year_str + QUARTER_ENDDATE_MAP[4]),
                int(last_year_str + QUARTER_ENDDATE_MAP[3])]
    elif 431 <= r < 701:
        return [int(year_str + QUARTER_ENDDATE_MAP[1])]
    elif 701 <= r < 901:
        return [int(year_str + QUARTER_ENDDATE_MAP[2]),
                int(year_str + QUARTER_ENDDATE_MAP[1])]
    elif 901 <= r < 1001:
        return [int(year_str + QUARTER_ENDDATE_MAP[2])]
    elif 1001 <= r < 1101:
        return [int(year_str + QUARTER_ENDDATE_MAP[3]),
                int(year_str + QUARTER_ENDDATE_MAP[2])]
    else:
        return [int(year_str + QUARTER_ENDDATE_MAP[3])]


class QuarterMetrics(object):
    def __init__(self, order_book_id: str):
        self._order_book_id = order_book_id
        self._quarter_metrics = self._get_and_fill()
        self._quarter_length = len(self._quarter_metrics)

        # current access index
        self._cur_index = -1
        self._four_straight_cache = None
        self._four_latest_cache = None
        self._enddate_quarter_report_map = None

    def get(self, tradedate: int):
        """
        lazily calculate necessary quarter metrics for recalculation formula.
        :param tradedate: trading date
        :return: dictionary, necessary quarter metrics report for recalculation.
        """
        ret = {}
        if self._cur_index < 0:
            latest_enddates = _latest_enddates(tradedate)
            for i in range(self._quarter_length):
                cur_report = self._quarter_metrics[i]
                cur_enddate = cur_report.get('end_date')
                if cur_enddate in latest_enddates:
                    ann_date = cur_report.get('announce_date')
                    if ann_date is None:  # skip filled report
                        continue
                    if tradedate >= ann_date:
                        self._cur_index = i
                        break
                if cur_enddate < latest_enddates[len(latest_enddates) - 1]:
                    break
            # traverse all quarter metrics for this stock, but can not find
            # latest_enddates
            if self._cur_index < 0:
                return ret
        if self._cur_index >= self._quarter_length:
            return ret
        cur_report = self._quarter_metrics[self._cur_index]
        cur_anndate = cur_report.get('announce_date')
        latest_enddates = None
        # since the previous tradedate is latest quarter report,
        # if current tradedate >= cur_anndate, then cur_report is latest
        # quarter report at tradedate. Otherwise, we need to find the latest
        # quarter report for current tradedate
        while cur_anndate is None or cur_anndate > tradedate:
            if latest_enddates is None:
                latest_enddates = _latest_enddates(tradedate)
            cur_enddate = cur_report.get('end_date')
            if cur_enddate < latest_enddates[len(latest_enddates) - 1]:
                self._cur_index -= 1
                return ret
            self._cur_index += 1
            self._clear()
            if self._cur_index >= self._quarter_length:
                return ret
            cur_report = self._quarter_metrics[self._cur_index]
            cur_enddate = cur_report.get('end_date')
            if cur_enddate in latest_enddates:
                cur_anndate = cur_report.get('announce_date')
                continue

        four_straight_metrics = self._four_straight_quarter(
            ['net_profit', 'cash_flow_from_operating_activities', 'cash',
             'cash_equivalent', 'cash_equivalent_inc_net']
        )
        four_latest_metrics = self._four_latest_quarter(
            ['cash_flow_from_operating_activities', 'cash',
             'cash_equivalent', 'revenue', 'operating_revenue',
             'net_profit_parent_company', 'cash_equivalent_inc_net']
        )
        if len(four_straight_metrics) > 0:
            ret.update(four_straight_metrics)
        if len(four_latest_metrics) > 0:
            ret.update(four_latest_metrics)

        # interest_bearing_debt
        debt = cur_report.get('interest_bearing_debt')
        if debt is not None:
            ret['interest_bearing_debt'] = debt

        # cash + cash_equivalent
        cash = cur_report.get('cash')
        cash_equi = cur_report.get('cash_equivalent')
        cash_total = 0
        if cash is not None:
            cash_total += cash
        if cash_equi is not None:
            cash_total += cash_equi
        ret['cash_total'] = cash_total

        # ebitda
        ebitda = cur_report.get('ebitda')
        if ebitda is not None:
            ret['ebitda'] = ebitda

        # net_profit_parent_company
        nppc_value = cur_report.get('net_profit_parent_company')
        if nppc_value is not None:
            ret['net_profit_parent_company'] = nppc_value

        # book_value_per_share
        book_value = cur_report.get('book_value_per_share')
        if book_value is not None:
            ret['book_value_per_share'] = book_value

        ret['end_date'] = cur_report.get('end_date')
        return ret

    def _four_straight_quarter(self, metric_names: List[str]):
        renames = ['straight_' + metric_name for metric_name in metric_names]
        if self._four_straight_cache is not None:
            return self._four_straight_cache

        ret = {}
        self._four_straight_cache = ret
        cur_report = self._quarter_metrics[self._cur_index]
        cur_quarter = cur_report.get('rpt_quarter')
        if cur_quarter == 4:
            for i in range(len(metric_names)):
                tmp_value = cur_report.get(metric_names[i])
                if tmp_value is None:
                    continue
                ret[renames[i]] = tmp_value
        else:
            last_annual_index = self._cur_index + cur_quarter
            last_same_index = self._cur_index + 4
            if last_annual_index >= self._quarter_length or last_same_index \
                    >= self._quarter_length:
                return ret
            last_annual_report = self._quarter_metrics[last_annual_index]
            last_same_report = self._quarter_metrics[last_same_index]
            # filled report will not participate in recalculation.
            if last_annual_report.get('announce_date') is None or \
                            last_same_report.get('announce_date') is None:
                return ret
            for i in range(len(metric_names)):
                cur_value = cur_report.get(metric_names[i])
                last_annual_value = last_annual_report.get(metric_names[i])
                last_same_value = last_same_report.get(metric_names[i])
                if None in (cur_value, last_annual_value, last_same_value):
                    continue
                ret[renames[i]] = cur_value + last_annual_value - \
                                  last_same_value
        return ret

    def _four_latest_quarter(self, metric_names: List[str]):
        renames = ['latest_' + metric_name for metric_name in metric_names]
        if self._four_latest_cache is not None:
            return self._four_latest_cache
        ret = {}
        self._four_latest_cache = ret
        cur_report = self._quarter_metrics[self._cur_index]
        cur_quarter = cur_report.get('rpt_quarter')
        for i in range(len(metric_names)):
            cur_value = cur_report.get(metric_names[i])
            if cur_value is None:
                continue
            if cur_quarter == 4:
                ret[renames[i]] = cur_value
            elif cur_quarter == 3:
                ret[renames[i]] = cur_value * 4 / 3
            elif cur_quarter == 2:
                ret[renames[i]] = cur_value * 2
            else:
                ret[renames[i]] = cur_value * 4
        return ret

    def latest_annual_report(self, tradedate):
        if self._enddate_quarter_report_map is None:
            self._enddate_quarter_report_map = {
                report.get('end_date'): report for report in
                self._quarter_metrics
                }
        last_year = int(tradedate / 10000) - 1
        return self._enddate_quarter_report_map.get(
            int(str(last_year) + QUARTER_ENDDATE_MAP[4]))

    def _get_and_fill(self):
        """
        firstly query quarter metrics from strategy_quarter, then fill the
        missing quarter report with announce_date and without any metric value.
        """
        filled_reports = []
        raw_reports = _quarter_metrics(self._order_book_id)
        raw_length = len(raw_reports)
        if raw_reports is None or raw_length == 0:
            print('Empty quarter metrics for order book id %s' %
                  self._order_book_id)
            return filled_reports
        latest_report = raw_reports[0]
        latest_year = latest_report.get('rpt_year')
        latest_quarter = latest_report.get('rpt_quarter')
        first_report = raw_reports[raw_length - 1]
        first_year = first_report.get('rpt_year')
        first_quarter = first_report.get('rpt_quarter')
        raw_index = 0
        for year in range(latest_year, first_year - 1, -1):
            for quarter in range(4, 0, -1):
                if (year, quarter) < (first_year, first_quarter) \
                        or (year, quarter) > (latest_year, latest_quarter):
                    continue
                cur_report = raw_reports[raw_index]
                cur_year = cur_report.get('rpt_year')
                cur_quarter = cur_report.get('rpt_quarter')
                if year == cur_year and quarter == cur_quarter:
                    filled_reports.append(cur_report)
                    raw_index += 1
                else:
                    enddate = int(str(year) + QUARTER_ENDDATE_MAP[quarter])
                    filled_reports.append(
                        {'rpt_year': year, 'rpt_quarter': quarter,
                         'end_date': enddate}
                    )
        filled_reports.append(first_report)
        return filled_reports

    def _clear(self):
        """clear all quarter metrics cache when current quarter was changed"""
        self._four_straight_cache = None
        self._four_latest_cache = None


def _four_quarter_metric(record, quarter_metrics, rename_metric, metric_name):
    market_cap = record.get('market_cap')
    metric_value = quarter_metrics.get(rename_metric)
    if None in (market_cap, metric_value) or metric_value == 0:
        del record[metric_name]
        return
    record[metric_name] = round(market_cap / metric_value, 4)


class RecalDayMetrics(object):
    def __init__(self, order_book_id: str):
        self._order_book_id = order_book_id
        self._quarter_obj = QuarterMetrics(order_book_id)

    def get_day_metrics(self, latest_date):
        return _day_metrics(self._order_book_id, latest_date)

    def get_closing_prices(self):
        ret = {}
        results = _closing_price(self._order_book_id)
        for result in results:
            tradedate = result.get('tradedate')
            closing_price = result.get('tclose')
            if None in (tradedate, closing_price):
                continue
            ret[tradedate] = closing_price
        return ret

    @staticmethod
    def pe_ratio(record, quarter_metrics):
        _four_quarter_metric(record, quarter_metrics, 'straight_net_profit',
                             'pe_ratio')

    @staticmethod
    def pcf_ratio(record, quarter_metrics):
        _four_quarter_metric(record, quarter_metrics,
                             'straight_cash_flow_from_operating_activities',
                             'pcf_ratio')

    @staticmethod
    def pcf_ratio_1(record, quarter_metrics):
        _four_quarter_metric(record, quarter_metrics,
                             'latest_cash_flow_from_operating_activities',
                             'pcf_ratio_1')

    @staticmethod
    def ps_ratio(record, quarter_metrics):
        market_cap = record.get('market_cap')
        operating_revenue = quarter_metrics.get('latest_operating_revenue')
        revenue = quarter_metrics.get('latest_revenue')
        if revenue is None or revenue == 0:
            revenue = operating_revenue
        if None in (market_cap, revenue) or revenue == 0:
            del record['ps_ratio']
            return
        record['ps_ratio'] = round(market_cap / revenue, 4)

    @staticmethod
    def pe_ratio_2(record, quarter_metrics):
        _four_quarter_metric(record, quarter_metrics,
                             'latest_net_profit_parent_company', 'pe_ratio_2')

    @staticmethod
    def ev(record, quarter_metrics):
        ev_value = 0
        val_stk_right = record.get('val_of_stk_right')
        debt = quarter_metrics.get('interest_bearing_debt')
        if val_stk_right is not None:
            ev_value += val_stk_right
        if debt is not None:
            ev_value += debt
        record['ev'] = ev_value

    @staticmethod
    def ev2(record, quarter_metrics):
        ev_value = record.get('ev')  # ev value was recalculated before
        cash_total = quarter_metrics.get('cash_total')
        if cash_total is None:
            cash_total = 0
        record['ev_2'] = ev_value - cash_total

    @staticmethod
    def ev_to_ebit(record, quarter_metrics):
        ev_value = record.get('ev')
        ebitda = quarter_metrics.get('ebitda')
        if ebitda is None or ebitda == 0:
            del record['ev_to_ebit']
            return
        record['ev_to_ebit'] = round(ev_value / ebitda, 4)

    @staticmethod
    def pe_ratio_1(record, quarter_metrics):
        market_cap = record.get('market_cap')
        nppc_value = quarter_metrics.get('net_profit_parent_company')
        if None in (market_cap, nppc_value) or nppc_value == 0:
            del record['pe_ratio_1']
            return
        record['pe_ratio_1'] = round(market_cap / nppc_value, 4)

    def peg_ratio(self, record, quarter_metrics, trading_date):
        pe_ratio_2 = record.get('pe_ratio_2')
        latest_four_nppc_value = quarter_metrics.get(
            'latest_net_profit_parent_company')
        latest_annual_report = self._quarter_obj.latest_annual_report(
            trading_date)
        latest_annual_nppc_value = None
        if latest_annual_report is not None:
            latest_annual_nppc_value = latest_annual_report.get(
                'net_profit_parent_company')
        if None in (
                latest_four_nppc_value, latest_annual_nppc_value, pe_ratio_2) \
                or latest_annual_nppc_value == 0:
            del record['peg_ratio']
            return
        return_inc_g = (latest_four_nppc_value - latest_annual_nppc_value) / \
                       latest_annual_nppc_value * 100
        if return_inc_g == 0:
            del record['peg_ratio']
            return
        record['peg_ratio'] = round(pe_ratio_2 / return_inc_g, 4)

    @staticmethod
    def pcf_ratio_3(record, quarter_metrics):
        _four_quarter_metric(record, quarter_metrics,
                             'straight_cash_equivalent_inc_net', 'pcf_ratio_3')

    @staticmethod
    def pcf_ratio_2(record, quarter_metrics):
        _four_quarter_metric(record, quarter_metrics,
                             'latest_cash_equivalent_inc_net', 'pcf_ratio_2')

    @staticmethod
    def pb_ratio(record, quarter_metrics, closing_price):
        book_value = quarter_metrics.get('book_value_per_share')
        if None in (book_value, closing_price):
            del record['pb_ratio']
            return

        record['pb_ratio'] = round(closing_price / book_value, 4)

    @staticmethod
    def market_cap(record):
        market_cap = record.get('market_cap')
        if market_cap is None:
            del record['market_cap']

    @staticmethod
    def market_cap_2(record):
        market_cap_2 = record.get('market_cap_2')
        if market_cap_2 is None:
            del record['market_cap_2']

    @staticmethod
    def a_share_market_val(record):
        a_share_market_val = record.get('a_share_market_val')
        if a_share_market_val is None:
            del record['a_share_market_val']

    @staticmethod
    def a_share_market_val_2(record):
        a_share_market_val_2 = record.get('a_share_market_val_2')
        if a_share_market_val_2 is None:
            del record['a_share_market_val_2']

    @staticmethod
    def val_of_stk_right(record):
        val_of_stk_right = record.get('val_of_stk_right')
        if val_of_stk_right is None:
            del record['val_of_stk_right']

    @staticmethod
    def dividend_yield(record):
        dividend_yield = record.get('dividend_yield')
        if dividend_yield is None:
            del record['dividend_yield']

    def _latest_date(self, table):
        dest_conn = get_dest_connect()
        with MySQLDictCursorWrapper(dest_conn) as dest_curosr:
            dest_curosr.execute(
                *query.fields(
                    table.tradedate
                ).tables(
                    table
                ).where(
                    (table.stockcode == self._order_book_id)
                ).order_by(
                    table.tradedate
                ).limit(1).select()
            )
            ret = dest_curosr.fetchone()
        dest_conn.close()
        return ret.get('tradedate') if ret is not None else None

    @staticmethod
    def _clear_record(record):
        cleared_record = {}
        for key, value in record.items():
            if value is None:
                continue
            if key in ('tradedate',):
                cleared_record[key] = int(
                    to_datetime(value).strftime('%Y%m%d'))
                continue
            cleared_record[key] = value
        return cleared_record

    def recal(self, first):
        closing_prices = self.get_closing_prices()
        latest_date = None if first else self._latest_date(orig_day)
        day_metrics = self.get_day_metrics(latest_date)
        dest_conn = get_dest_connect()
        with MySQLDictCursorWrapper(dest_conn) as dest_cursor:
            for record in day_metrics:
                record['stockcode'] = self._order_book_id
                orig_record = self._clear_record(record)
                dest_cursor.execute(
                    *query.tables(orig_day).insert(orig_record)
                )
                tradedate = record.get('tradedate')
                trading_date = int(to_datetime(tradedate).strftime('%Y%m%d'))
                quarter_metrics = self._quarter_obj.get(trading_date)
                self.pe_ratio(record, quarter_metrics)
                self.pcf_ratio(record, quarter_metrics)
                self.pcf_ratio_1(record, quarter_metrics)
                self.ps_ratio(record, quarter_metrics)
                self.pe_ratio_2(record, quarter_metrics)
                self.ev(record, quarter_metrics)
                self.ev2(record, quarter_metrics)
                self.ev_to_ebit(record, quarter_metrics)
                self.pe_ratio_1(record, quarter_metrics)
                self.peg_ratio(record, quarter_metrics, trading_date)
                self.pcf_ratio_3(record, quarter_metrics)
                self.pcf_ratio_2(record, quarter_metrics)
                self.pb_ratio(record, quarter_metrics,
                              closing_prices.get(tradedate))

                # remove None value in non-recalculation metrics since None
                # value can not store it into mongodb.
                self.market_cap(record)
                self.market_cap_2(record)
                self.a_share_market_val(record)
                self.a_share_market_val_2(record)
                self.val_of_stk_right(record)
                self.dividend_yield(record)
                record['tradedate'] = trading_date
                dest_cursor.execute(
                    *query.tables(recal_day).insert(record)
                )
        dest_conn.close()


def recal_by_stock(i, first, id_queue):
    while True:
        order_book_id = id_queue.get()
        print(datetime.datetime.now(), 'handle ', order_book_id)
        if order_book_id is None:
            break
        recal_obj = RecalDayMetrics(order_book_id)
        recal_obj.recal(first)


def update_day(first=False):
    create_orig_day()
    create_recal_day()
    orderbookid_queue = Queue()

    process_num = 5
    workers = [
        Process(target=recal_by_stock, args=(i, first, orderbookid_queue,))
        for i in range(process_num)]
    for worker in workers:
        worker.start()

    for order_book_id in get_orderbookids():
        orderbookid_queue.put(order_book_id)

    for _ in workers:
        orderbookid_queue.put(None)

    for worker in workers:
        worker.join()
    orderbookid_queue.close()
