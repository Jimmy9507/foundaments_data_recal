import unittest
from unittest import TestCase
import fdhandle
from fdhandle.recal import recal_four_straight_indicator, \
    _get_quarter_metrics, _day_metrics, _four_straight_quarter,\
    strategy_quarter_index_on_year
from pandas import DataFrame, read_csv
from decimal import Decimal
import math
fdhandle.init()


class TestPcf3Ratio(TestCase):
    def test_pcf_ratio_3(self):
        stockcode = '000002.XSHE'
        quarter_metrics = _get_quarter_metrics(stockcode)
        year_quarters_map = strategy_quarter_index_on_year(quarter_metrics)
        stockcode_without_suffix = stockcode.split(".")[0]
        market_caps = _day_metrics(stockcode_without_suffix)
        four_straight_quarter_cash_flow=_four_straight_quarter(quarter_metrics,
                                                     'cash')
        four_straight_quarter_increment_cash_flow=_four_straight_increment_quarter(
                                                        four_straight_quarter_cash_flow,
                                                     'cash')
        pcf_ratio_3_dict = {'TRD_DATE': [], 'pcf_ratio_3_recal': [],
                         'TCAP_1': []}
        print(four_straight_quarter_cash_flow)
        print(four_straight_quarter_increment_cash_flow)
        for index, record in enumerate(market_caps):
            recal_four_straight_indicator(record,four_straight_quarter_increment_cash_flow,
                                          year_quarters_map, 'pcf_ratio_3',
                                          ['cash','cash_equivalent'])



            pcf_ratio_3_dict['TRD_DATE'].append(record['tradedate'])
            pcf_ratio_3_dict['pcf_ratio_3_recal'].append(record['pcf_ratio_3'])

            pcf_ratio_3_dict['TCAP_1'].append(record['market_cap'])

        calc_df = DataFrame(pcf_ratio_3_dict)
        calc_df.to_csv('~/Desktop/eod_indicator_identify/000002_pcf_ratio_3_identify.csv')



if __name__ == '__main__':
    unittest.main()
