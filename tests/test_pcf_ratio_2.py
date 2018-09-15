import unittest
from unittest import TestCase
import fdhandle
from fdhandle.recal import recal_four_latest_indicator, \
    _get_quarter_metrics, _day_metrics,_four_latest_quarter,\
    _four_latest_increment_quarter,\
    strategy_quarter_index_on_year
from pandas import DataFrame, read_csv
from decimal import Decimal
import math
fdhandle.init()


class TestPcf2Ratio(TestCase):
    def test_pcf_ratio_2(self):
        stockcode = '000001.XSHE'
        quarter_metrics = _get_quarter_metrics(stockcode)
        year_quarters_map = strategy_quarter_index_on_year(quarter_metrics)
        stockcode_without_suffix = stockcode.split(".")[0]
        market_caps = _day_metrics(stockcode_without_suffix)
        four_latest_quarters_cash_flow = _four_latest_increment_quarter(
            quarter_metrics, 'cash_flow_from_operating_activities')

        pcf_ratio_2_dict = {'TRD_DATE': [], 'pcf_ratio_2_recal': [], 'pcf_ratio_2_manual': [],
                         'TCAP_1': [], 'isdifference': []}
        manual_df = read_csv('~/Desktop/recal_manual/000001_pcf_ratio_2.csv')

        for index, record in enumerate(market_caps):
            recal_four_latest_indicator(record, four_latest_quarters_cash_flow,
                                          year_quarters_map, 'pcf_ratio_2',
                                          ['cash_flow_from_operating_activities'])
            pcf_ratio_2_manual = float(manual_df['pcf_ratio_2'][index])
            if math.isnan(pcf_ratio_2_manual):
                pcf_ratio_2_manual = None
            else:
                pcf_ratio_2_manual = Decimal(manual_df['pcf_ratio_2'][index]).quantize(Decimal('0.0000'))

            if record['pcf_ratio_2'] == pcf_ratio_2_manual:
                record['isdifference'] = ""
            else:
                record['isdifference'] = "******"

            pcf_ratio_2_dict['TRD_DATE'].append(record['tradedate'])
            pcf_ratio_2_dict['pcf_ratio_2_recal'].append(record['pcf_ratio_2'])
            pcf_ratio_2_dict['pcf_ratio_2_manual'].append(pcf_ratio_2_manual)
            pcf_ratio_2_dict['TCAP_1'].append(record['market_cap'])
            pcf_ratio_2_dict['isdifference'].append(record['isdifference'])
        calc_df = DataFrame(pcf_ratio_2_dict)
        calc_df.to_csv('~/Desktop/eod_indicator_identify/000001_pcf_ratio_2_identify.csv')

        self.assertEqual(manual_df['pcf_ratio_2'].equals(calc_df['pcf_ratio_2_recal']), True)


if __name__ == '__main__':
    unittest.main()
