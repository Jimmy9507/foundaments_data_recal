import unittest
from unittest import TestCase
import fdhandle
from fdhandle.recal import recal_latest_quarter_indicator, \
    _get_quarter_metrics, _day_metrics, strategy_quarter_index_on_year
from pandas import DataFrame, read_csv
from decimal import Decimal
import math
fdhandle.init()


class TestPbRatio(TestCase):
    def test_pb_ratio(self):
        stockcode = '000001.XSHE'
        quarter_metrics = _get_quarter_metrics(stockcode)
        year_quarters_map = strategy_quarter_index_on_year(quarter_metrics)
        stockcode_without_suffix = stockcode.split(".")[0]
        market_caps = _day_metrics(stockcode_without_suffix)

        pb_ratio_dict = {'TRD_DATE': [], 'pb_ratio_recal': [], 'pb_ratio_manual': [],
                         'TCAP_1': [], 'isdifference': []}
        manual_df = read_csv('~/Desktop/recal_manual/000001_pb_ratio.csv')

        for index, record in enumerate(market_caps):
            recal_latest_quarter_indicator(record, quarter_metrics,
                                           year_quarters_map, 'pb_ratio', ['current_assets'])

            pb_ratio_manual = float(manual_df['pb_ratio'][index])
            if math.isnan(pb_ratio_manual):
                pb_ratio_manual = None
            else:
                pb_ratio_manual = Decimal(manual_df['pb_ratio'][index]).quantize(Decimal('0.0000'))

            if record['pb_ratio'] == pb_ratio_manual:
                record['isdifference'] = ""
            else:
                record['isdifference'] = "******"

            pb_ratio_dict['TRD_DATE'].append(record['tradedate'])
            pb_ratio_dict['pb_ratio_recal'].append(record['pb_ratio'])
            pb_ratio_dict['pb_ratio_manual'].append(pb_ratio_manual)
            pb_ratio_dict['TCAP_1'].append(record['market_cap'])
            pb_ratio_dict['isdifference'].append(record['isdifference'])
        calc_df = DataFrame(pb_ratio_dict)
        calc_df.to_csv('~/Desktop/eod_indicator_identify/000001_pb_ratio_identify.csv')

        self.assertEqual(manual_df['pb_ratio'].equals(calc_df['pb_ratio_recal']), True)


if __name__ == '__main__':
    unittest.main()
