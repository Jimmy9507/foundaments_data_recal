from unittest import TestCase

import numpy
from pandas import DataFrame, read_csv, to_datetime

import fdhandle
from fdhandle.metrics import QUARTER_ENDDATE_MAP
from fdhandle.recal import _quarter_metrics, QuarterMetrics, _day_metrics, \
    _latest_enddates, RecalDayMetrics

fdhandle.init()

__all__ = (
    'TestFourStraightQuarter',
    'TestFourLatestQuarter',
    'TestRecalMetrics',
)


def _four_straight_quarter(quarter_metrics, metric_name):
    """
    calculate four straight quarter of metric_name in quarter_metrics. If
    specified metric_name does not exist in quarter_metrics, it will throw
    exception immediately.

    :param quarter_metrics:  list of dictionary of metrics, this list is in
        descending order by 'end_date'
    :param metric_name: string, it specifies metric name to
        calculate its four straight quarter value.
    :return: list of dictionary which key is metric name and value is its
        four straight quarter value, and the list is in descending order by
        'announce_date'.
    """
    if metric_name not in quarter_metrics[0].keys():
        raise RuntimeError('%s does not exists in quarter_metrics, so can not '
                           'calculate four straight quarter.' % metric_name)

    ret = []
    for i in range(len(quarter_metrics)):
        cur_report = quarter_metrics[i]
        ann_date = cur_report.get('announce_date')
        cur_year = cur_report.get('rpt_year')
        cur_quarter = cur_report.get('rpt_quarter')
        cur_enddate = cur_report.get('end_date')
        four_straights = {'announce_date': ann_date, 'rpt_year': cur_year,
                          'rpt_quarter': cur_quarter, 'end_date': cur_enddate}
        cur_value = cur_report.get(metric_name)

        # no value, can not calculate
        if cur_value is None:
            ret.append(four_straights)
            continue

        # if current quarter is annual report, then the value of its four
        # straight quarter is itself value. On the other hand, if value is
        # None, we does not save it, but this record need to be kept.
        if cur_quarter == 4 and cur_value is not None:
            four_straights[metric_name] = cur_value
            ret.append(four_straights)
            continue

        # calculate the expected indices of last annual report and the same
        # report of last year. If some later report is missing, then the
        # real index may be less than the expected index or missing it.
        last_annual_report_index = i + cur_quarter
        last_same_period_index = i + 4

        # check if last report index is out of range, we must traverse
        # remaining records since there may be missing some reports so that
        # last same period report index less than expected index
        if last_same_period_index >= len(quarter_metrics):
            # try to traverse remaining records to find out the indices of
            # last same period report and annual report. If either was missing,
            # the four straight quarter value cannot be calculated, so return
            # immediately.
            find_same_match = False
            find_annual_match = False
            for later_index in range(i + 1, len(quarter_metrics)):
                report = quarter_metrics[later_index]
                tmp_rpt_year = report.get('rpt_year')
                tmp_rpt_quarter = report.get('rpt_quarter')
                if not find_annual_match and tmp_rpt_year == cur_year - 1 and \
                                tmp_rpt_quarter == 4:
                    last_annual_report_index = later_index
                    find_annual_match = True
                if not find_same_match and tmp_rpt_year == cur_year - 1 and \
                                tmp_rpt_quarter == cur_quarter:
                    last_same_period_index = later_index
                    find_same_match = True
                if find_same_match and find_annual_match:
                    break

            if not find_same_match or not find_annual_match:
                ret.append(four_straights)
                continue

        last_annual_report = quarter_metrics[last_annual_report_index]
        last_annual_rpt_quarter = last_annual_report.get('rpt_quarter')
        last_annual_rpt_year = last_annual_report.get('rpt_year')
        # check if last annual report exists
        if last_annual_rpt_year != cur_year - 1 or last_annual_rpt_quarter \
                != 4:
            find_match = False
            # try to traverse later records
            for later_index in range(i + 1, i + cur_quarter):
                last_annual_report = quarter_metrics[later_index]
                last_annual_rpt_quarter = last_annual_report.get('rpt_quarter')
                last_annual_rpt_year = last_annual_report.get('rpt_year')
                if last_annual_rpt_year == cur_year - 1 and \
                                last_annual_rpt_quarter == 4:
                    find_match = True
                    break

            if not find_match:
                ret.append(four_straights)
                continue

        last_same_period_report = quarter_metrics[last_same_period_index]
        last_same_period_quarter = last_same_period_report.get('rpt_quarter')
        last_same_period_year = last_same_period_report.get('rpt_year')
        # check if last same period report exists
        if last_same_period_year != cur_year - 1 or last_same_period_quarter \
                != cur_quarter:
            find_match = False
            # try to traverse later records
            for later_index in range(i + 1, i + 4):
                last_same_period_report = quarter_metrics[later_index]
                last_same_period_quarter = last_same_period_report.get(
                    'rpt_quarter')
                last_same_period_year = last_same_period_report.get('rpt_year')
                if last_same_period_year == cur_year - 1 and \
                                last_same_period_quarter == cur_quarter:
                    find_match = True
                    break

            if not find_match:
                ret.append(four_straights)
                continue

        # calculate four straight quarter value
        last_annual_report_value = last_annual_report.get(metric_name)
        if last_annual_report_value is None:
            ret.append(four_straights)
            continue
        last_same_period_report_value = last_same_period_report.get(
            metric_name)
        if last_same_period_report_value is None:
            ret.append(four_straights)
            continue

        four_straights[metric_name] = \
            cur_value + last_annual_report_value - \
            last_same_period_report.get(metric_name)
        ret.append(four_straights)
    return ret


def _four_latest_quarter(quarter_metrics, metric_name):
    """
      calculate four latest quarter of metric_name in quarter metrics. If
      specified metric_name does not exist in quarter_metrics, it will throw
      exception immediately.


      :param quarter_metrics:  list of dictionary of metrics, this list is in
          descending order by 'end_date'
      :param metric_name: string, it specifies metric name to
          calculate its latest four quarter value.
      :return: list of dictionary which key is metric name and value is its
          latest four quarter value, and the list is in descending order by
          'announce_date'.
      """
    if metric_name not in quarter_metrics[0].keys():
        raise RuntimeError('%s does not exists in quarter_metrics, so it can '
                           'not calculate latest four quarter.' % metric_name)
    ret = []
    for i in range(len(quarter_metrics)):
        cur_report = quarter_metrics[i]
        ann_date = cur_report.get('announce_date')
        cur_year = cur_report.get('rpt_year')
        cur_quarter = cur_report.get('rpt_quarter')
        cur_enddate = cur_report.get('end_date')
        four_latest = {'announce_date': ann_date, 'end_date': cur_enddate,
                       'rpt_year': cur_year, 'rpt_quarter': cur_quarter}
        cur_value = cur_report.get(metric_name)
        if cur_value is None:
            ret.append(four_latest)
            continue
        if cur_quarter == 4:
            four_latest[metric_name] = cur_value
        if cur_quarter == 3:
            four_latest[metric_name] = cur_value * 4 / 3
        if cur_quarter == 2:
            four_latest[metric_name] = cur_value * 2
        if cur_quarter == 1:
            four_latest[metric_name] = cur_value * 4
        ret.append(four_latest)
    return ret


def _get_quarter_metrics(order_book_id):
    return _quarter_metrics(order_book_id)


def _get_day_metrics(order_book_id):
    return _day_metrics(order_book_id)


def _enddate_reports_map(reports):
    return {report.get('end_date'): report for report in reports}


def _calc_four_straight_by_manual(order_book_id, metric_name, recal_metric_name):
    ret = []
    raw_day_reports = _day_metrics(order_book_id)
    raw_quarter_reports = _quarter_metrics(order_book_id)
    four_straight_reports = _four_straight_quarter(raw_quarter_reports, metric_name)

    enddate_four_straight_map = _enddate_reports_map(four_straight_reports)
    day_df = DataFrame(raw_day_reports).fillna(numpy.nan)
    market_caps = day_df.market_cap.values
    tradedates = day_df.tradedate.values
    for i in range(len(tradedates)):
        t = int(to_datetime(tradedates[i]).strftime('%Y%m%d'))
        ret_value = {'tradedate': t}
        latest_enddates = _latest_enddates(t)
        report = None
        for latest_enddate in latest_enddates:
            latest_report = enddate_four_straight_map.get(latest_enddate)
            if latest_report is None:
                continue
            ann_date = latest_report.get('announce_date')
            if t >= ann_date:
                report = latest_report
                ret_value['announce_date'] = ann_date
                ret_value['end_date'] = latest_enddate
                break
        if report is None:
            ret.append(ret_value)
            continue
        metric_value = report.get(metric_name)
        if metric_value is None or metric_value == 0:
            ret.append(ret_value)
            continue
        ret_value[metric_name] = metric_value
        ret_value[recal_metric_name] = round(market_caps[i] / metric_value, 4)
        ret.append(ret_value)
    return ret


def _cal_four_latest_by_manual(order_book_id, metric_name, recal_metric_name):
    ret = []
    raw_day_reports = _day_metrics(order_book_id)
    raw_quarter_reports = _quarter_metrics(order_book_id)
    four_latest_reports = _four_latest_quarter(raw_quarter_reports, metric_name)

    enddate_four_latest_map = _enddate_reports_map(four_latest_reports)
    day_df = DataFrame(raw_day_reports).fillna(numpy.nan)
    market_caps = day_df.market_cap.values
    tradedates = day_df.tradedate.values
    for i in range(len(tradedates)):
        t = int(to_datetime(tradedates[i]).strftime('%Y%m%d'))
        ret_value = {'tradedate': t}
        latest_enddates = _latest_enddates(t)
        report = None
        for latest_enddate in latest_enddates:
            latest_report = enddate_four_latest_map.get(latest_enddate)
            if latest_report is None:
                continue
            ann_date = latest_report.get('announce_date')
            if t >= ann_date:
                report = latest_report
                ret_value['announce_date'] = ann_date
                ret_value['end_date'] = latest_enddate
                break
        if report is None:
            ret.append(ret_value)
            continue
        metric_value = report.get(metric_name)
        if metric_value is None or metric_value == 0:
            ret.append(ret_value)
            continue
        ret_value[metric_name] = metric_value
        ret_value[recal_metric_name] = round(market_caps[i] / metric_value, 4)
        ret.append(ret_value)
    return ret


def _cal_ev_by_manual(order_book_id):
    ret = []
    raw_day_reports = _day_metrics(order_book_id)
    raw_quarter_reports = _quarter_metrics(order_book_id)

    enddate_report_map = _enddate_reports_map(raw_quarter_reports)
    day_df = DataFrame(raw_day_reports).fillna(numpy.nan)
    tradedates = day_df.tradedate.values
    val_stk_rights = day_df.val_of_stk_right.values
    for i in range(len(tradedates)):
        t = int(to_datetime(tradedates[i]).strftime('%Y%m%d'))
        ret_value = {'tradedate': t}
        latest_enddates = _latest_enddates(t)
        report = None
        for latest_enddate in latest_enddates:
            latest_report = enddate_report_map.get(latest_enddate)
            if latest_report is None:
                continue
            ann_date = latest_report.get('announce_date')
            if t >= ann_date:
                report = latest_report
                ret_value['announce_date'] = ann_date
                ret_value['end_date'] = latest_enddate
                break
        ret_value['ev'] = 0
        val_stk_value = val_stk_rights[i]
        if val_stk_value is not None:
            ret_value['ev'] += val_stk_value
        if report is not None:
            debt = report.get('interest_bearing_debt')
            if debt is not None:
                ret_value['ev'] += debt
                ret_value['interest_bearing_debt'] = debt
        ret.append(ret_value)
    return ret


def _cal_ev2_by_manual(order_book_id):
    ret = []
    raw_day_reports = _day_metrics(order_book_id)
    raw_quarter_reports = _quarter_metrics(order_book_id)

    enddate_report_map = _enddate_reports_map(raw_quarter_reports)
    day_df = DataFrame(raw_day_reports).fillna(numpy.nan)
    tradedates = day_df.tradedate.values
    val_stk_rights = day_df.val_of_stk_right.values
    for i in range(len(tradedates)):
        t = int(to_datetime(tradedates[i]).strftime('%Y%m%d'))
        ret_value = {'tradedate': t}
        latest_enddates = _latest_enddates(t)
        report = None
        for latest_enddate in latest_enddates:
            latest_report = enddate_report_map.get(latest_enddate)
            if latest_report is None:
                continue
            ann_date = latest_report.get('announce_date')
            if t >= ann_date:
                report = latest_report
                ret_value['announce_date'] = ann_date
                ret_value['end_date'] = latest_enddate
                break
        ev_value = 0
        val_stk_value = val_stk_rights[i]
        if val_stk_value is not None:
            ev_value += val_stk_value
        if report is not None:
            debt = report.get('interest_bearing_debt')
            if debt is not None:
                ev_value += debt
            cash = report.get('cash')
            if cash is not None:
                ev_value -= cash
            cash_equi = report.get('cash_equivalent')
            if cash_equi is not None:
                ev_value -= cash_equi
        ret_value['ev_2'] = ev_value
        ret.append(ret_value)
    return ret


def _cal_ev_to_ebit_by_manual(order_book_id):
    ret = []
    raw_day_reports = _day_metrics(order_book_id)
    raw_quarter_reports = _quarter_metrics(order_book_id)

    enddate_report_map = _enddate_reports_map(raw_quarter_reports)
    day_df = DataFrame(raw_day_reports).fillna(numpy.nan)
    tradedates = day_df.tradedate.values
    val_stk_rights = day_df.val_of_stk_right.values
    for i in range(len(tradedates)):
        t = int(to_datetime(tradedates[i]).strftime('%Y%m%d'))
        ret_value = {'tradedate': t}
        latest_enddates = _latest_enddates(t)
        report = None
        for latest_enddate in latest_enddates:
            latest_report = enddate_report_map.get(latest_enddate)
            if latest_report is None:
                continue
            ann_date = latest_report.get('announce_date')
            if t >= ann_date:
                report = latest_report
                ret_value['announce_date'] = ann_date
                ret_value['end_date'] = latest_enddate
                break
        ev_value = 0
        val_stk_value = val_stk_rights[i]
        if val_stk_value is not None:
            ev_value += val_stk_value
        if report is not None:
            debt = report.get('interest_bearing_debt')
            if debt is not None:
                ev_value += debt
            ebitda = report.get('ebitda')
            if ebitda is not None and ebitda != 0:
                ret_value['ev_to_ebit'] = round(ev_value / ebitda, 4)
        ret.append(ret_value)
    return ret


def _cal_pe_ratio_1_by_manual(order_book_id):
    ret = []
    raw_day_reports = _day_metrics(order_book_id)
    raw_quarter_reports = _quarter_metrics(order_book_id)
    enddate_report_map = _enddate_reports_map(raw_quarter_reports)
    day_df = DataFrame(raw_day_reports).fillna(numpy.nan)
    market_caps = day_df.market_cap.values
    tradedates = day_df.tradedate.values
    for i in range(len(tradedates)):
        t = int(to_datetime(tradedates[i]).strftime('%Y%m%d'))
        ret_value = {'tradedate': t}
        latest_enddates = _latest_enddates(t)
        report = None
        for latest_enddate in latest_enddates:
            latest_report = enddate_report_map.get(latest_enddate)
            if latest_report is None:
                continue
            ann_date = latest_report.get('announce_date')
            if t >= ann_date:
                report = latest_report
                ret_value['announce_date'] = ann_date
                ret_value['end_date'] = latest_enddate
                break
        if report is not None:
            nppc_value = report.get('net_profit_parent_company')
            if nppc_value is not None and nppc_value != 0:
                market_cap = market_caps[i]
                ret_value['pe_ratio_1'] = round(market_cap / nppc_value, 4)
                ret_value['net_profit_parent_company'] = nppc_value
        ret.append(ret_value)
    return ret


def _cal_peg_ratio_by_manual(order_book_id):
    ret = []
    raw_quarter_reports = _quarter_metrics(order_book_id)
    enddate_report_map = _enddate_reports_map(raw_quarter_reports)
    pe_ratio_2_by_manual = _cal_four_latest_by_manual(order_book_id, 'net_profit_parent_company', 'pe_ratio_2')

    for i in range(len(pe_ratio_2_by_manual)):
        tradedate = pe_ratio_2_by_manual[i]['tradedate']
        ret_value = {'tradedate': tradedate}
        pe_ratio_2_report = pe_ratio_2_by_manual[i]
        last_year = int(tradedate / 10000) - 1
        latest_annual_report = enddate_report_map.get(int(str(last_year) + QUARTER_ENDDATE_MAP[4]))
        four_latest_report = pe_ratio_2_by_manual[i]
        pe_ratio_2_value = pe_ratio_2_report.get('pe_ratio_2')
        latest_annual_value = latest_annual_report.get('net_profit_parent_company') if latest_annual_report is not None else 0
        four_latest_value = four_latest_report.get('net_profit_parent_company')
        if None not in (pe_ratio_2_value, latest_annual_value, four_latest_value) and latest_annual_value != 0:
            return_inc_g = (four_latest_value - latest_annual_value) / latest_annual_value * 100
            if return_inc_g != 0:
                ret_value['peg_ratio'] = round(pe_ratio_2_value / return_inc_g, 4)
        ret.append(ret_value)
    return ret


class TestFourStraightQuarter(TestCase):
    """test function _four_straight_quarter in this file"""
    # read csv file which was recalculated by manual
    def test_four_straight_quarter(self):
        quarter_metrics = _quarter_metrics('000001.XSHE')
        calc_four_straight_quarter = _four_straight_quarter(quarter_metrics,
                                                            'net_profit')
        calc_df = DataFrame(calc_four_straight_quarter)
        calc_df = calc_df[calc_df['announce_date'] <= 20160812]
        calc_df['four_straight_net_profit'] = calc_df.apply(
            lambda row: float(row['net_profit']), axis=1)

        manual_df = read_csv('recal_by_manual/four_straight_net_profit.csv')
        manual_df = manual_df[manual_df['announce_date'] <= 20160812]
        self.assertEqual(manual_df['four_straight_net_profit'].equals(
            calc_df['four_straight_net_profit']), True)


class TestFourLatestQuarter(TestCase):
    """test function _four_latest_quarter in this file"""
    # read csv file which was recalculated by manual
    def test_four_straight_quarter(self):
        quarter_metrics = _quarter_metrics('000001.XSHE')
        calc_four_latest_quarter = _four_latest_quarter(quarter_metrics,
                                                        'net_profit')
        calc_df = DataFrame(calc_four_latest_quarter)
        calc_df = calc_df[calc_df['announce_date'] <= 20160812]
        calc_df['four_latest_net_profit'] = calc_df.apply(
            lambda row: round(float(row['net_profit']), 4), axis=1)

        manual_df = read_csv('recal_by_manual/four_latest_net_profit.csv')
        manual_df = manual_df[manual_df['announce_date'] <= 20160812]
        # self.assertEqual(manual_df['four_latest_net_profit'].values, calc_df['four_latest_net_profit'].values)
        self.assertEqual(manual_df['four_latest_net_profit'].equals(
            calc_df['four_latest_net_profit']), True)


class TestRecalMetrics(TestCase):
    order_book_id = '000001.XSHE'
    recal_day_metrics = RecalDayMetrics(order_book_id).recal()

    def test_pe_ratio(self):
        pe_ratio_by_manual = _calc_four_straight_by_manual(self.order_book_id, 'net_profit', 'pe_ratio')
        quarter_obj = QuarterMetrics(self.order_book_id)
        for i in range(len(pe_ratio_by_manual)):
            manual_record = pe_ratio_by_manual[i]
            tradedate = manual_record.get('tradedate')
            manual_net_profit = manual_record.get('net_profit')

            recal_quarter_report = quarter_obj.get(tradedate)
            recal_net_profit = recal_quarter_report.get('straight_net_profit')
            self.assertEqual(manual_net_profit, recal_net_profit, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

            manual_pe_ratio = manual_record.get('pe_ratio')
            recal_pe_ratio = self.recal_day_metrics[i].get('pe_ratio')
            self.assertEqual(manual_pe_ratio, recal_pe_ratio, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

    def test_pcf_ratio(self):
        pcf_ratio_by_manual = _calc_four_straight_by_manual(self.order_book_id, 'cash_flow_from_operating_activities', 'pcf_ratio')
        quarter_obj = QuarterMetrics(self.order_book_id)
        for i in range(len(pcf_ratio_by_manual)):
            manual_record = pcf_ratio_by_manual[i]
            tradedate = manual_record.get('tradedate')
            manual_operating_activities = manual_record.get('cash_flow_from_operating_activities')

            recal_quarter_report = quarter_obj.get(tradedate)
            recal_operating_activities = recal_quarter_report.get('straight_cash_flow_from_operating_activities')
            self.assertEqual(manual_operating_activities, recal_operating_activities, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

            manual_pcf_ratio = manual_record.get('pcf_ratio')
            recal_pcf_ratio = self.recal_day_metrics[i].get('pcf_ratio')
            self.assertEqual(manual_pcf_ratio, recal_pcf_ratio, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

    def test_pcf_ratio_1(self):
        pcf_ratio1_by_manual = _cal_four_latest_by_manual(self.order_book_id, 'cash_flow_from_operating_activities', 'pcf_ratio_1')
        quarter_obj = QuarterMetrics(self.order_book_id)
        for i in range(len(pcf_ratio1_by_manual)):
            manual_record = pcf_ratio1_by_manual[i]
            tradedate = manual_record.get('tradedate')
            manual_operating_activities = manual_record.get('cash_flow_from_operating_activities')

            recal_quarter_report = quarter_obj.get(tradedate)
            recal_operating_activities = recal_quarter_report.get('latest_cash_flow_from_operating_activities')
            self.assertEqual(manual_operating_activities, recal_operating_activities, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

            manual_pcf_ratio = manual_record.get('pcf_ratio_1')
            recal_pcf_ratio = self.recal_day_metrics[i].get('pcf_ratio_1')
            self.assertEqual(manual_pcf_ratio, recal_pcf_ratio, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

    def test_ps_ratio(self):
        # 000001.XSHE belongs to finance sector, it need to use operating revenue as revenue to calculate.
        ps_ratio_by_manual = _cal_four_latest_by_manual(self.order_book_id, 'operating_revenue', 'ps_ratio')
        quarter_obj = QuarterMetrics(self.order_book_id)
        for i in range(len(ps_ratio_by_manual)):
            manual_record = ps_ratio_by_manual[i]
            tradedate = manual_record.get('tradedate')
            recal_quarter_report = quarter_obj.get(tradedate)
            manual_ps_ratio = manual_record.get('ps_ratio')
            recal_ps_ratio = self.recal_day_metrics[i].get('ps_ratio')
            self.assertEqual(manual_ps_ratio, recal_ps_ratio, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

    def test_pe_ratio_2(self):
        pe_ratio_2_by_manual = _cal_four_latest_by_manual(self.order_book_id, 'net_profit_parent_company', 'pe_ratio_2')
        quarter_obj = QuarterMetrics(self.order_book_id)
        for i in range(len(pe_ratio_2_by_manual)):
            manual_record = pe_ratio_2_by_manual[i]
            tradedate = manual_record.get('tradedate')
            manual_value = manual_record.get('net_profit_parent_company')

            recal_quarter_report = quarter_obj.get(tradedate)
            recal_value = recal_quarter_report.get('latest_net_profit_parent_company')
            self.assertEqual(manual_value, recal_value, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

            manual_pe_ratio_2 = manual_record.get('pe_ratio_2')
            recal_pe_ratio_2 = self.recal_day_metrics[i].get('pe_ratio_2')
            self.assertEqual(manual_pe_ratio_2, recal_pe_ratio_2, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

    def test_ev(self):
        ev_by_manual = _cal_ev_by_manual(self.order_book_id)
        quarter_obj = QuarterMetrics(self.order_book_id)
        for i in range(len(ev_by_manual)):
            manual_record = ev_by_manual[i]
            tradedate = manual_record.get('tradedate')
            manual_value = manual_record.get('interest_bearing_debt')

            recal_quarter_report = quarter_obj.get(tradedate)
            recal_value = recal_quarter_report.get('interest_bearing_debt')
            self.assertEqual(manual_value, recal_value, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

            manual_ev = manual_record.get('ev')
            recal_ev = self.recal_day_metrics[i].get('ev')
            self.assertEqual(manual_ev, recal_ev, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

    def test_ev2(self):
        ev2_by_manual = _cal_ev2_by_manual(self.order_book_id)
        quarter_obj = QuarterMetrics(self.order_book_id)
        for i in range(len(ev2_by_manual)):
            manual_record = ev2_by_manual[i]
            tradedate = manual_record.get('tradedate')

            recal_quarter_report = quarter_obj.get(tradedate)

            manual_ev2 = manual_record.get('ev_2')
            recal_ev2 = self.recal_day_metrics[i].get('ev_2')
            self.assertEqual(manual_ev2, recal_ev2, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

    def test_ev_to_ebit(self):
        ev_to_ebit_by_manual = _cal_ev_to_ebit_by_manual(self.order_book_id)
        quarter_obj = QuarterMetrics(self.order_book_id)
        for i in range(len(ev_to_ebit_by_manual)):
            manual_record = ev_to_ebit_by_manual[i]
            tradedate = manual_record.get('tradedate')

            recal_quarter_report = quarter_obj.get(tradedate)

            manual_ev_to_ebit = manual_record.get('ev_to_ebit')
            recal_ev_to_ebit = self.recal_day_metrics[i].get('ev_to_ebit')
            self.assertEqual(manual_ev_to_ebit, recal_ev_to_ebit, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

    def test_pe_ratio_1(self):
        pe_ratio_1_by_manual = _cal_pe_ratio_1_by_manual(self.order_book_id)
        quarter_obj = QuarterMetrics(self.order_book_id)
        for i in range(len(pe_ratio_1_by_manual)):
            manual_record = pe_ratio_1_by_manual[i]
            tradedate = manual_record.get('tradedate')
            manual_value = manual_record.get('net_profit_parent_company')

            recal_quarter_report = quarter_obj.get(tradedate)
            recal_value = recal_quarter_report.get('net_profit_parent_company')
            self.assertEqual(manual_value, recal_value, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

            manual_pe_ratio_1 = manual_record.get('pe_ratio_1')
            recal_pe_ratio_1 = self.recal_day_metrics[i].get('pe_ratio_1')
            self.assertEqual(manual_pe_ratio_1, recal_pe_ratio_1, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

    def test_peg_ratio(self):
        peg_ratio_by_manual = _cal_peg_ratio_by_manual(self.order_book_id)
        quarter_obj = QuarterMetrics(self.order_book_id)
        for i in range(len(peg_ratio_by_manual)):
            manual_record = peg_ratio_by_manual[i]
            tradedate = manual_record.get('tradedate')
            manual_peg_ratio = manual_record.get('peg_ratio')

            recal_quarter_report = quarter_obj.get(tradedate)
            recal_peg_ratio = self.recal_day_metrics[i].get('peg_ratio')
            self.assertEqual(manual_peg_ratio, recal_peg_ratio, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

    def test_pcf_ratio_3(self):
        pcf_ratio3_by_manual = _calc_four_straight_by_manual(self.order_book_id, 'cash_equivalent_inc_net', 'pcf_ratio_3')
        quarter_obj = QuarterMetrics(self.order_book_id)
        for i in range(len(pcf_ratio3_by_manual)):
            manual_record = pcf_ratio3_by_manual[i]
            tradedate = manual_record.get('tradedate')
            manual_pcf3 = manual_record.get('cash_equivalent_inc_net')

            recal_quarter_report = quarter_obj.get(tradedate)
            recal_pcf3 = recal_quarter_report.get('straight_cash_equivalent_inc_net')
            self.assertEqual(manual_pcf3, recal_pcf3, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

            manual_pcf3 = manual_record.get('pcf_ratio_3')
            recal_pcf3 = self.recal_day_metrics[i].get('pcf_ratio_3')
            self.assertEqual(manual_pcf3, recal_pcf3, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

    def test_pcf_ratio_2(self):
        pcf_ratio_2_by_manual = _cal_four_latest_by_manual(self.order_book_id, 'cash_equivalent_inc_net', 'pcf_ratio_2')
        quarter_obj = QuarterMetrics(self.order_book_id)
        for i in range(len(pcf_ratio_2_by_manual)):
            manual_record = pcf_ratio_2_by_manual[i]
            tradedate = manual_record.get('tradedate')
            manual_value = manual_record.get('cash_equivalent_inc_net')

            recal_quarter_report = quarter_obj.get(tradedate)
            recal_value = recal_quarter_report.get('latest_cash_equivalent_inc_net')
            self.assertEqual(manual_value, recal_value, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

            manual_pcf_ratio_2 = manual_record.get('pcf_ratio_2')
            recal_pcf_ratio_2 = self.recal_day_metrics[i].get('pcf_ratio_2')
            self.assertEqual(manual_pcf_ratio_2, recal_pcf_ratio_2, 'tradedate = {0}, \nrecal_report = {1}, \nmanual_report = {2}'.format(tradedate, recal_quarter_report, manual_record))

    def test_latest_enddates(self):
        latest_enddates = _latest_enddates(20161125)
        expected = [20160930]
        self.assertCountEqual(latest_enddates, expected)

        latest_enddates = _latest_enddates(20161101)
        expected = [20160930]
        self.assertCountEqual(latest_enddates, expected)

        latest_enddates = _latest_enddates(20161020)
        expected = [20160930, 20160630]
        self.assertCountEqual(latest_enddates, expected)

        latest_enddates = _latest_enddates(20160910)
        expected = [20160630]
        self.assertCountEqual(latest_enddates, expected)

        latest_enddates = _latest_enddates(20160715)
        expected = [20160630, 20160331]
        self.assertCountEqual(latest_enddates, expected)

        latest_enddates = _latest_enddates(20160526)
        expected = [20160331]
        self.assertCountEqual(latest_enddates, expected)

        latest_enddates = _latest_enddates(20160431)
        expected = [20160331]
        self.assertCountEqual(latest_enddates, expected)

        latest_enddates = _latest_enddates(20160210)
        expected = [20160331, 20151231, 20150930]
        self.assertCountEqual(latest_enddates, expected)
