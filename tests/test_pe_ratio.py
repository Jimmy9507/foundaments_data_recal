import unittest
from unittest import TestCase
import fdhandle
from fdhandle.recal import recal_four_straight_indicator, \
    _get_quarter_metrics, _day_metrics, _four_straight_quarter, \
    strategy_quarter_index_on_year
from pandas import DataFrame, read_csv
from decimal import Decimal
import math

fdhandle.init()


class TestPeRatio(TestCase):
    def test_pe_ratio(self):
        stockcode = '000001.XSHE'
        quarter_metrics = _get_quarter_metrics(stockcode)
        year_quarters_map = strategy_quarter_index_on_year(quarter_metrics)
        stockcode_without_suffix = stockcode.split(".")[0]
        market_caps = _day_metrics(stockcode_without_suffix)
        four_straight_quarters_net_profit = _four_straight_quarter(
            quarter_metrics, 'net_profit')

        pe_ratio_dict = {'TRD_DATE': [], 'pe_ratio_recal': [], 'pe_ratio_manual': [],
                         'TCAP_1': [], 'isdifference': []}
        manual_df = read_csv('~/Desktop/recal_manual/000001_pe_ratio.csv')

        for index, record in enumerate(market_caps):
            recal_four_straight_indicator(record, four_straight_quarters_net_profit,
                                          year_quarters_map, 'pe_ratio', ['net_profit'])

            pe_ratio_manual = float(manual_df['pe_ratio'][index])
            if math.isnan(pe_ratio_manual):
                pe_ratio_manual = None
            else:
                pe_ratio_manual = Decimal(manual_df['pe_ratio'][index]).quantize(Decimal('0.0000'))
            if record['pe_ratio'] == pe_ratio_manual:
                record['isdifference'] = ""
            else:
                record['isdifference'] = "******"

            pe_ratio_dict['TRD_DATE'].append(record['tradedate'])
            pe_ratio_dict['pe_ratio_recal'].append(record['pe_ratio'])
            pe_ratio_dict['pe_ratio_manual'].append(pe_ratio_manual)
            pe_ratio_dict['TCAP_1'].append(record['market_cap'])
            pe_ratio_dict['isdifference'].append(record['isdifference'])
        calc_df = DataFrame(pe_ratio_dict)
        calc_df.to_csv('~/Desktop/eod_indicator_identify/000001_pe_ratio_identify.csv')

        self.assertEqual(manual_df['pe_ratio'].equals(calc_df['pe_ratio_recal']), True)


if __name__ == '__main__':
    unittest.main()
