from sqlbuilder.smartsql.compilers.mysql import compile as mysql_compile
from sqlbuilder.smartsql import T, Q, Result

query = Q(result=Result(compile=mysql_compile))

# tables
research_quarter = T.research_quarter
prepare_quarter = T.prepare_quarter
strategy_quarter = T.strategy_quarter
orig_day = T.orig_day
recal_day = T.recal_day
day_fd = T.ana_stk_val_idx
balance_sheet = T.stk_bala_gen
income_statement = T.stk_income_gen
cash_flow = T.stk_cash_gen
finance_indicator = T.ana_stk_fin_idx
stk_code = T.stk_code
stk_market = T.stk_mkt

# fields
trade_date = day_fd.trd_date


RPT_TYPE = "合并"
RPT_SRC = ("第一季度报", "中报", "第三季度报", "年报")


class Metrics(object):
    @classmethod
    def metrics(cls):
        return parse_metrics(cls)


class Day(Metrics):  # 19
    stock_code = day_fd.stockcode.as_('stockcode')
    trade_date = day_fd.trd_date.as_('tradedate')
    inner_code_ = day_fd.inner_code

    pe_ratio = day_fd.PE.as_('pe_ratio')
    pcf_ratio = day_fd.PC.as_('pcf_ratio')
    pb_ratio = day_fd.PB.as_('pb_ratio')
    market_cap = day_fd.TCAP_1.as_('market_cap')
    market_cap_2 = day_fd.TCAP_2.as_('market_cap_2')
    a_share_market_val = day_fd.A_TCAP_1.as_('a_share_market_val')
    a_share_market_val_2 = day_fd.A_TCAP_2.as_('a_share_market_val_2')
    val_of_stk_right = day_fd.SRV.as_('val_of_stk_right')
    ev = day_fd.EV1.as_('ev')
    ev_2 = day_fd.EV2.as_('ev_2')
    ev_to_ebit = day_fd.EV_EBIT.as_('ev_to_ebit')
    dividend_yield = day_fd.DIV_RATE.as_('dividend_yield')
    pe_ratio_1 = day_fd.PE1.as_('pe_ratio_1')
    pe_ratio_2 = day_fd.PE2.as_('pe_ratio_2')
    peg_ratio = day_fd.PEG.as_('peg_ratio')
    pcf_ratio_1 = day_fd.PC1.as_('pcf_ratio_1')
    pcf_ratio_2 = day_fd.PC2.as_('pcf_ratio_2')
    pcf_ratio_3 = day_fd.PC3.as_('pcf_ratio_3')
    ps_ratio = day_fd.PS.as_('ps_ratio')

    @staticmethod
    def filter_conditions_():
        return day_fd.isvalid == 1


class Income(Metrics):  # 49

    stock_code = income_statement.a_stockcode.as_('stockcode')
    announce_date = income_statement.declaredate.as_('announce_date')
    end_date = income_statement.enddate.as_('end_date')
    comcode = income_statement.comcode
    rpt_src = income_statement.rpt_src
    mtime_ = income_statement.mtime

    revenue = income_statement.P110100.as_('revenue')
    operating_revenue = income_statement.P110101.as_('operating_revenue')
    sales_discount = income_statement.P110112.as_('sales_discount')
    total_expense = income_statement.P110200.as_('total_expense')
    cost_of_goods_sold = income_statement.P110202.as_('cost_of_goods_sold')
    sales_tax = income_statement.P110302.as_('sales_tax')
    gross_profit = income_statement.P120101.as_('gross_profit')
    other_operating_income = income_statement.P120201.as_('other_operating_income')
    inventory_shrinkage = income_statement.P120302.as_('inventory_shrinkage')
    selling_expense = income_statement.P120442.as_('selling_expense')
    operating_expense = income_statement.P120412.as_('operating_expense')
    ga_expense = income_statement.P120422.as_('ga_expense')
    financing_expense = income_statement.P120432.as_('financing_expense')
    period_cost = income_statement.P120402.as_('period_cost')
    order_cost = income_statement.P120502.as_('order_cost')
    prospecting_cost = income_statement.P120702.as_('prospecting_cost')
    exchange_gains_or_losses = income_statement.P120601.as_('exchange_gains_or_losses')
    asset_depreciation = income_statement.P131102.as_('asset_depreciation')
    profit_from_operation = income_statement.P130101.as_('profit_from_operation')
    investment_income = income_statement.P130201.as_('investment_income')
    subsidy_income = income_statement.P130401.as_('subsidy_income')
    non_operating_revenue = income_statement.P130501.as_('non_operating_revenue')
    pnl_adjustment = income_statement.P130601.as_('pnl_adjustment')
    non_operating_expense = income_statement.P130702.as_('non_operating_expense')
    disposal_loss_on_asset = income_statement.P130712.as_('disposal_loss_on_asset')
    non_operating_net_profit = income_statement.P130801.as_('non_operating_net_profit')
    profit_before_tax = income_statement.P140101.as_('profit_before_tax')
    income_tax = income_statement.P140202.as_('income_tax')
    profit_from_ma = income_statement.P140702.as_('profit_from_ma')
    unrealised_investment_losses = income_statement.P140801.as_('unrealised_investment_losses')
    income_tax_refund = income_statement.P140901.as_('income_tax_refund')
    net_profit = income_statement.P150101.as_('net_profit')
    net_profit_parent_company = income_statement.P160101.as_('net_profit_parent_company')
    net_profit_before_ma = income_statement.P180101.as_('net_profit_before_ma')
    retained_profit_at_beginning = income_statement.P210101.as_('retained_profit_at_beginning')
    profit_available_for_distribution = income_statement.P220101.as_('profit_available_for_distribution')
    statutory_welfare_reserve = income_statement.P220302.as_('statutory_welfare_reserve')
    staff_incentive_welfare_reserve = income_statement.P220402.as_('staff_incentive_welfare_reserve')
    enterprise_expansion_reserve = income_statement.P220602.as_('enterprise_expansion_reserve')
    profit_available_for_owner_distribution = income_statement.P230101.as_('profit_available_for_owner_distribution')
    preferred_stock_dividends = income_statement.P230202.as_('preferred_stock_dividends')
    other_surplus_reserve = income_statement.P230302.as_('other_surplus_reserve')
    ordinary_stock_dividends = income_statement.P230402.as_('ordinary_stock_dividends')
    loss_on_debt_restructuring = income_statement.P240602.as_('loss_on_debt_restructuring')
    basic_earnings_per_share = income_statement.P240801.as_('basic_earnings_per_share')
    other_income = income_statement.P250100.as_('other_income')
    total_income = income_statement.P260100.as_('total_income')
    total_income_parent_company = income_statement.P260101.as_('total_income_parent_company')
    total_income_minority = income_statement.P260102.as_('total_income_minority')

    @staticmethod
    def filter_conditions_():
        # SELECT * FROM stk_income_gen WHERE COMCODE=#{code} AND ISVALID=1 and rpt_type="合并" and (rpt_src="第一季度报" or
        # rpt_src="中报" or rpt_src="第三季度报" or rpt_src="年报") AND RPT_DATE=ENDDATE and startdate like '%-01-01%'
        return (income_statement.isvalid == 1) & \
               (income_statement.rpt_type == "合并") & \
               (income_statement.rpt_src.in_(RPT_SRC)) & \
               (income_statement.rpt_date == income_statement.enddate) & \
               (income_statement.startdate.contains('-01-01'))

    @staticmethod
    def name_():
        return 'stk_income_gen'


class Balance(Metrics):  # 106
    stock_code = balance_sheet.a_stockcode.as_('stockcode')
    announce_date = balance_sheet.declaredate.as_('announce_date')
    end_date = balance_sheet.enddate.as_('end_date')
    comcode = balance_sheet.comcode
    rpt_src = balance_sheet.rpt_src
    mtime_ = balance_sheet.mtime

    cash = balance_sheet.B110101.as_('cash')
    financial_asset_held_for_trading = balance_sheet.B112201.as_('financial_asset_held_for_trading')
    cash_equivalent = balance_sheet.B110201.as_('cash_equivalent')
    current_investment = balance_sheet.B110311.as_('current_investment')
    current_investment_reserve = balance_sheet.B110322.as_('current_investment_reserve')
    net_current_investment = balance_sheet.B110301.as_('net_current_investment')
    bill_receivable = balance_sheet.B110401.as_('bill_receivable')
    devidend_receivable = balance_sheet.B110501.as_('devidend_receivable')
    interest_receivable = balance_sheet.B110601.as_('interest_receivable')
    accts_receivable = balance_sheet.B110711.as_('accts_receivable')
    other_accts_receivable = balance_sheet.B110721.as_('other_accts_receivable')
    bad_debt_reserve = balance_sheet.B110732.as_('bad_debt_reserve')
    net_accts_receivable = balance_sheet.B110701.as_('net_accts_receivable')
    other_receivables = balance_sheet.B110801.as_('other_receivables')
    prepayment = balance_sheet.B110901.as_('prepayment')
    subsidy_receivable = balance_sheet.B111001.as_('subsidy_receivable')
    prepaid_tax = balance_sheet.B111101.as_('prepaid_tax')
    inventory = balance_sheet.B111511.as_('inventory')
    inventory_depreciation_reserve = balance_sheet.B111522.as_('inventory_depreciation_reserve')
    net_inventory = balance_sheet.B111501.as_('net_inventory')
    deferred_expense = balance_sheet.B111601.as_('deferred_expense')
    contract_work = balance_sheet.B111801.as_('contract_work')
    long_term_debt_due_one_year = balance_sheet.B112001.as_('long_term_debt_due_one_year')
    non_current_debt_due_one_year = balance_sheet.B112301.as_('non_current_debt_due_one_year')
    other_current_assets = balance_sheet.B112101.as_('other_current_assets')
    current_assets = balance_sheet.B110001.as_('current_assets')
    financial_asset_available_for_sale = balance_sheet.B120801.as_('financial_asset_available_for_sale')
    financial_asset_hold_to_maturity = balance_sheet.B120901.as_('financial_asset_hold_to_maturity')
    real_estate_investment = balance_sheet.B121001.as_('real_estate_investment')
    long_term_equity_investment = balance_sheet.B120111.as_('long_term_equity_investment')
    long_term_receivables = balance_sheet.B121101.as_('long_term_receivables')
    long_term_debt_investment = balance_sheet.B120121.as_('long_term_debt_investment')
    other_long_term_investment = balance_sheet.B120131.as_('other_long_term_investment')
    long_term_investment = balance_sheet.B120101.as_('long_term_investment')
    provision_long_term_investment = balance_sheet.B120202.as_('provision_long_term_investment')
    net_long_term_equity_investment = balance_sheet.B120301.as_('net_long_term_equity_investment')
    net_long_term_debt_investment = balance_sheet.B120401.as_('net_long_term_debt_investment')
    net_long_term_investment = balance_sheet.B120001.as_('net_long_term_investment')
    cost_fixed_assets = balance_sheet.B130111.as_('cost_fixed_assets')
    accumulated_depreciation = balance_sheet.B130122.as_('accumulated_depreciation')
    net_val_fixed_assets = balance_sheet.B130131.as_('net_val_fixed_assets')
    depreciation_reserve = balance_sheet.B130142.as_('depreciation_reserve')
    net_fixed_assets = balance_sheet.B130101.as_('net_fixed_assets')
    engineer_material = balance_sheet.B130201.as_('engineer_material')
    construction_in_progress = balance_sheet.B130301.as_('construction_in_progress')
    fixed_asset_to_be_disposed = balance_sheet.B130401.as_('fixed_asset_to_be_disposed')
    capitalized_biological_assets = balance_sheet.B130601.as_('capitalized_biological_assets')
    oil_and_gas_assets = balance_sheet.B130701.as_('oil_and_gas_assets')
    total_fixed_assets = balance_sheet.B130001.as_('total_fixed_assets')
    intangible_assets = balance_sheet.B140101.as_('intangible_assets')
    impairment_intangible_assets = balance_sheet.B140601.as_('impairment_intangible_assets')
    goodwill = balance_sheet.B140701.as_('goodwill')
    deferred_charges = balance_sheet.B140301.as_('deferred_charges')
    long_term_deferred_expenses = balance_sheet.B140401.as_('long_term_deferred_expenses')
    other_long_term_assets = balance_sheet.B140501.as_('other_long_term_assets')
    total_intangible_and_other_assets = balance_sheet.B140001.as_('total_intangible_and_other_assets')
    deferred_income_tax_assets = balance_sheet.B150001.as_('deferred_income_tax_assets')
    other_non_current_assets = balance_sheet.B160101.as_('other_non_current_assets')
    non_current_assets = balance_sheet.B160000.as_('non_current_assets')
    total_assets = balance_sheet.B100000.as_('total_assets')
    short_term_loans = balance_sheet.B210101.as_('short_term_loans')
    financial_liabilities = balance_sheet.B212301.as_('financial_liabilities')
    notes_payable = balance_sheet.B210201.as_('notes_payable')
    accts_payable = balance_sheet.B210301.as_('accts_payable')
    advance_from_customers = balance_sheet.B210401.as_('advance_from_customers')
    proxy_sale_revenue = balance_sheet.B210501.as_('proxy_sale_revenue')
    payroll_payable = balance_sheet.B210601.as_('payroll_payable')
    walfare_payable = balance_sheet.B210701.as_('walfare_payable')
    dividend_payable = balance_sheet.B210801.as_('dividend_payable')
    tax_payable = balance_sheet.B210901.as_('tax_payable')
    interest_payable = balance_sheet.B212401.as_('interest_payable')
    other_fees_payable = balance_sheet.B211101.as_('other_fees_payable')
    internal_accts_payable = balance_sheet.B211201.as_('internal_accts_payable')
    other_payable = balance_sheet.B211301.as_('other_payable')
    short_term_debt = balance_sheet.B211401.as_('short_term_debt')
    accrued_expense = balance_sheet.B211501.as_('accrued_expense')
    estimated_liabilities = balance_sheet.B211901.as_('estimated_liabilities')
    deferred_income = balance_sheet.B212701.as_('deferred_income')
    long_term_liabilities_due_one_year = balance_sheet.B212001.as_('long_term_liabilities_due_one_year')
    other_current_liabilities = balance_sheet.B212101.as_('other_current_liabilities')
    current_liabilities = balance_sheet.B210001.as_('current_liabilities')
    long_term_loans = balance_sheet.B220101.as_('long_term_loans')
    bond_payable = balance_sheet.B220201.as_('bond_payable')
    long_term_payable = balance_sheet.B220301.as_('long_term_payable')
    grants_received = balance_sheet.B220401.as_('grants_received')
    housing_revolving_funds = balance_sheet.B220501.as_('housing_revolving_funds')
    other_long_term_liabilities = balance_sheet.B220601.as_('other_long_term_liabilities')
    long_term_liabilities = balance_sheet.B220001.as_('long_term_liabilities')
    deferred_income_tax_liabilities = balance_sheet.B240001.as_('deferred_income_tax_liabilities')
    other_non_current_liabilities = balance_sheet.B250001.as_('other_non_current_liabilities')
    non_current_liabilities = balance_sheet.B270001.as_('non_current_liabilities')
    total_liabilities = balance_sheet.B200000.as_('total_liabilities')
    paid_in_capital = balance_sheet.B310101.as_('paid_in_capital')
    invesment_refund = balance_sheet.B311202.as_('invesment_refund')
    capital_reserve = balance_sheet.B310201.as_('capital_reserve')
    surplus_reserve = balance_sheet.B310301.as_('surplus_reserve')
    statutory_reserve = balance_sheet.B310401.as_('statutory_reserve')
    welfare_reserve = balance_sheet.B310501.as_('welfare_reserve')
    unrealised_investment_loss = balance_sheet.B310601.as_('unrealised_investment_loss')
    undistributed_profit = balance_sheet.B310701.as_('undistributed_profit')
    equity_parent_company = balance_sheet.B311101.as_('equity_parent_company')
    total_equity = balance_sheet.B300000.as_('total_equity')
    minority_interest = balance_sheet.B400000.as_('minority_interest')
    total_equity_and_liabilities = balance_sheet.B500000.as_('total_equity_and_liabilities')
    provision = balance_sheet.B290003.as_('provision')
    deferred_revenue = balance_sheet.B221001.as_('deferred_revenue')

    @staticmethod
    def filter_conditions_():
        # select * from stk_bala_gen where ISVALID=1 AND COMCODE=#{code} and rpt_type="合并" and (rpt_src="第一季度报" or
        # rpt_src="中报" or rpt_src="第三季度报" or rpt_src="年报") AND RPT_DATE=ENDDATE
        return (balance_sheet.isvalid == 1) & \
               (balance_sheet.rpt_type == "合并") & \
               (balance_sheet.rpt_src.in_(RPT_SRC)) & \
               (balance_sheet.rpt_date == balance_sheet.enddate)

    @staticmethod
    def name_():
        return 'stk_bala_gen'


class CashFlow(Metrics):  # 36
    stock_code = cash_flow.a_stockcode.as_('stockcode')
    announce_date = cash_flow.declaredate.as_('announce_date')
    end_date = cash_flow.enddate.as_('end_date')
    comcode = cash_flow.comcode
    rpt_src = cash_flow.rpt_src
    mtime_ = cash_flow.mtime

    cash_received_from_sales_of_goods = cash_flow.C110101.as_('cash_received_from_sales_of_goods')
    rental_cash = cash_flow.C110201.as_('rental_cash')
    refunds_of_vat = cash_flow.C110311.as_('refunds_of_vat')
    refunds_of_other_taxes = cash_flow.C110321.as_('refunds_of_other_taxes')
    refunds_of_taxes = cash_flow.C110301.as_('refunds_of_taxes')
    cash_from_other_operating_activities = cash_flow.C110401.as_('cash_from_other_operating_activities')
    cash_from_operating_activities = cash_flow.C110000.as_('cash_from_operating_activities')
    cash_paid_for_goods_and_services = cash_flow.C120101.as_('cash_paid_for_goods_and_services')
    cash_paid_for_rental = cash_flow.C120201.as_('cash_paid_for_rental')
    cash_paid_for_employee = cash_flow.C120301.as_('cash_paid_for_employee')
    cash_paid_for_taxes = cash_flow.C120401.as_('cash_paid_for_taxes')
    cash_paid_for_other_operation_activities = cash_flow.C120501.as_('cash_paid_for_other_operation_activities')
    cash_paid_for_operation_activities = cash_flow.C120000.as_('cash_paid_for_operation_activities')
    cash_flow_from_operating_activities = cash_flow.C100000.as_('cash_flow_from_operating_activities')
    cash_received_from_disposal_of_investment = cash_flow.C210101.as_('cash_received_from_disposal_of_investment')
    cash_received_from_dividend = cash_flow.C210211.as_('cash_received_from_dividend')
    cash_received_from_interest = cash_flow.C210221.as_('cash_received_from_interest')
    cash_received_from_disposal_of_asset = cash_flow.C210301.as_('cash_received_from_disposal_of_asset')
    cash_received_from_other_investment_activities = cash_flow.C210401.as_('cash_received_from_other_investment_activities')
    cash_received_from_investment_activities = cash_flow.C210000.as_('cash_received_from_investment_activities')
    cash_paid_for_asset = cash_flow.C220101.as_('cash_paid_for_asset')
    cash_paid_to_acquire_investment = cash_flow.C220201.as_('cash_paid_to_acquire_investment')
    cash_paid_for_other_investment_activities = cash_flow.C220301.as_('cash_paid_for_other_investment_activities')
    cash_paid_for_investment_activities = cash_flow.C220000.as_('cash_paid_for_investment_activities')
    cash_flow_from_investing_activities = cash_flow.C200000.as_('cash_flow_from_investing_activities')
    cash_received_from_equity_investors = cash_flow.C310101.as_('cash_received_from_equity_investors')
    cash_received_from_debt_investors = cash_flow.C310201.as_('cash_received_from_debt_investors')
    cash_received_from_investors = cash_flow.C310301.as_('cash_received_from_investors')
    cash_received_from_financial_institution_borrows = cash_flow.C310401.as_('cash_received_from_financial_institution_borrows')
    cash_received_from_other_financing_activities = cash_flow.C310501.as_('cash_received_from_other_financing_activities')
    cash_received_from_financing_activities = cash_flow.C310000.as_('cash_received_from_financing_activities')
    cash_paid_for_debt = cash_flow.C320101.as_('cash_paid_for_debt')
    cash_paid_for_dividend_and_interest = cash_flow.C320301.as_('cash_paid_for_dividend_and_interest')
    cash_paid_for_other_financing_activities = cash_flow.C320701.as_('cash_paid_for_other_financing_activities')
    cash_paid_to_financing_activities = cash_flow.C320000.as_('cash_paid_to_financing_activities')
    cash_flow_from_financing_activities = cash_flow.C300000.as_('cash_flow_from_financing_activities')
    cash_equivalent_inc_net = cash_flow.C410201.as_('cash_equivalent_inc_net')

    @staticmethod
    def filter_conditions_():
        # select * from stk_cash_gen where ISVALID=1 AND COMCODE=#{code} and rpt_type="合并" and (rpt_src="第一季度报" or
        # rpt_src="中报" or rpt_src="第三季度报" or rpt_src="年报") AND RPT_DATE=ENDDATE
        return (cash_flow.isvalid == 1) & \
               (cash_flow.rpt_type == "合并") & \
               (cash_flow.rpt_src.in_(RPT_SRC)) & \
               (cash_flow.rpt_date == cash_flow.enddate) & \
               (cash_flow.startdate.contains('-01-01'))

    @staticmethod
    def name_():
        return 'stk_cash_gen'


class Indicator(Metrics):  # 110
    stock_code = finance_indicator.a_stockcode.as_('stockcode')
    end_date = finance_indicator.enddate.as_('end_date')
    comcode = finance_indicator.comcode
    mtime_ = finance_indicator.mtime

    earnings_per_share = finance_indicator.EPSP.as_('earnings_per_share')
    fully_diluted_earnings_per_share = finance_indicator.EPSFD.as_('fully_diluted_earnings_per_share')
    diluted_earnings_per_share = finance_indicator.EPSEED.as_('diluted_earnings_per_share')
    new_diluted_earnings_per_share = finance_indicator.EPSNED.as_('new_diluted_earnings_per_share')
    adjusted_earnings_per_share = finance_indicator.EPSP_DED.as_('adjusted_earnings_per_share')
    adjusted_fully_diluted_earnings_per_share = finance_indicator.EPSFD_DED.as_('adjusted_fully_diluted_earnings_per_share')
    adjusted_diluted_earnings_per_share = finance_indicator.EPSEED_DED.as_('adjusted_diluted_earnings_per_share')
    book_value_per_share = finance_indicator.BPS.as_('book_value_per_share')
    new_diluted_book_value_per_share = finance_indicator.BPSNED.as_('new_diluted_book_value_per_share')
    operating_cash_flow_per_share = finance_indicator.PS_NET_VAL.as_('operating_cash_flow_per_share')
    operating_total_revenue_per_share = finance_indicator.PS_OTR.as_('operating_total_revenue_per_share')
    operating_revenue_per_share = finance_indicator.PS_OR.as_('operating_revenue_per_share')
    capital_reserve_per_share = finance_indicator.PS_CR.as_('capital_reserve_per_share')
    earned_reserve_per_share = finance_indicator.PS_LR.as_('earned_reserve_per_share')
    undistributed_profit_per_share = finance_indicator.PS_UP.as_('undistributed_profit_per_share')
    retained_earnings_per_share = finance_indicator.PS_RE.as_('retained_earnings_per_share')
    cash_flow_from_operations_per_share = finance_indicator.PS_CN.as_('cash_flow_from_operations_per_share')
    ebit_per_share = finance_indicator.PS_EBIT.as_('ebit_per_share')
    free_cash_flow_company_per_share = finance_indicator.PS_COM_CF.as_('free_cash_flow_company_per_share')
    free_cash_flow_equity_per_share = finance_indicator.PS_SH_CF.as_('free_cash_flow_equity_per_share')
    dividend_per_share = finance_indicator.PS_CASH_BT.as_('dividend_per_share')
    return_on_equity = finance_indicator.ROEA.as_('return_on_equity')
    return_on_equity_weighted_average = finance_indicator.ROER.as_('return_on_equity_weighted_average')
    return_on_equity_diluted = finance_indicator.ROED.as_('return_on_equity_diluted')
    adjusted_return_on_equity_average = finance_indicator.ROEA_DED.as_('adjusted_return_on_equity_average')
    adjusted_return_on_equity_weighted_average = finance_indicator.ROER_DED.as_('adjusted_return_on_equity_weighted_average')
    adjusted_return_on_equity_diluted = finance_indicator.ROED_DED.as_('adjusted_return_on_equity_diluted')
    return_on_asset = finance_indicator.ROA.as_('return_on_asset')
    return_on_asset_net_profit = finance_indicator.ROA_NP.as_('return_on_asset_net_profit')
    return_on_invested_capital = finance_indicator.ROIC.as_('return_on_invested_capital')
    annual_return_on_equity = finance_indicator.ROE_YEAR.as_('annual_return_on_equity')
    annual_return_on_asset = finance_indicator.ROA_YEAR.as_('annual_return_on_asset')
    annual_return_on_asset_net_profit = finance_indicator.ROA_NYEAR.as_('annual_return_on_asset_net_profit')
    net_profit_margin = finance_indicator.SEL_NINT.as_('net_profit_margin')
    gross_profit_margin = finance_indicator.SEL_RINT.as_('gross_profit_margin')
    cost_to_sales = finance_indicator.SEL_COST.as_('cost_to_sales')
    net_profit_to_revenue = finance_indicator.TR_NP.as_('net_profit_to_revenue')
    profit_from_operation_to_revenue = finance_indicator.TR_TP.as_('profit_from_operation_to_revenue')
    ebit_to_revenue = finance_indicator.TR_EBIT.as_('ebit_to_revenue')
    expense_to_revenue = finance_indicator.TR_TC.as_('expense_to_revenue')
    operating_profit_to_profit_before_tax = finance_indicator.TP_ONI.as_('operating_profit_to_profit_before_tax')
    invesment_profit_to_profit_before_tax = finance_indicator.TP_VNI.as_('invesment_profit_to_profit_before_tax')
    non_operating_profit_to_profit_before_tax = finance_indicator.TP_OON.as_('non_operating_profit_to_profit_before_tax')
    income_tax_to_profit_before_tax = finance_indicator.TP_TAX.as_('income_tax_to_profit_before_tax')
    adjusted_profit_to_total_profit = finance_indicator.TP_DNP.as_('adjusted_profit_to_total_profit')
    debt_to_asset_ratio = finance_indicator.CAP_LAB.as_('debt_to_asset_ratio')
    equity_multiplier = finance_indicator.CAP_RIG.as_('equity_multiplier')
    current_asset_to_total_asset = finance_indicator.CAP_FLO.as_('current_asset_to_total_asset')
    non_current_asset_to_total_asset = finance_indicator.CAP_NFL.as_('non_current_asset_to_total_asset')
    tangible_asset_to_total_asset = finance_indicator.CAP_SA.as_('tangible_asset_to_total_asset')
    interest_bearing_debt_to_capital = finance_indicator.CAP_ILAB.as_('interest_bearing_debt_to_capital')
    current_debt_to_total_debt = finance_indicator.CAP_FLO_F.as_('current_debt_to_total_debt')
    non_current_debt_to_total_debt = finance_indicator.CAP_FLO_N.as_('non_current_debt_to_total_debt')
    current_ratio = finance_indicator.LAB_FLO.as_('current_ratio')
    quick_ratio = finance_indicator.LAB_SLO.as_('quick_ratio')
    super_quick_ratio = finance_indicator.LAB_NSO.as_('super_quick_ratio')
    debt_to_equity_ratio = finance_indicator.LAB_PR.as_('debt_to_equity_ratio')
    equity_to_debt_ratio = finance_indicator.LAB_OPR.as_('equity_to_debt_ratio')
    equity_to_interest_bearing_debt = finance_indicator.LAB_APR.as_('equity_to_interest_bearing_debt')
    tangible_asset_to_debt = finance_indicator.LAB_TAN.as_('tangible_asset_to_debt')
    tangible_asset_to_interest_bearing_debt = finance_indicator.LAB_ITAN.as_('tangible_asset_to_interest_bearing_debt')
    tangible_asset_to_net_debt = finance_indicator.LAB_NIAN.as_('tangible_asset_to_net_debt')
    ebit_to_debt = finance_indicator.LAB_EBIT.as_('ebit_to_debt')
    ocf_to_debt = finance_indicator.LAB_OC.as_('ocf_to_debt')
    ocf_to_interest_bearing_debt = finance_indicator.LAB_IOC.as_('ocf_to_interest_bearing_debt')
    ocf_to_current_ratio = finance_indicator.LAB_FOC.as_('ocf_to_current_ratio')
    ocf_to_net_debt = finance_indicator.LAB_LOC.as_('ocf_to_net_debt')
    time_interest_earned_ratio = finance_indicator.LAB_IEBIT.as_('time_interest_earned_ratio')
    long_term_debt_to_working_capital = finance_indicator.LAB_LO.as_('long_term_debt_to_working_capital')
    net_debt_to_stock_right = finance_indicator.LAB_SRV.as_('net_debt_to_stock_right')
    interest_bearing_debt_to_stock_right = finance_indicator.LAB_ISRV.as_('interest_bearing_debt_to_stock_right')
    account_payable_turnover_rate = finance_indicator.OPE_APR.as_('account_payable_turnover_rate')
    account_payable_turnover_days = finance_indicator.OPE_APC.as_('account_payable_turnover_days')
    account_receivable_turnover_days = finance_indicator.OPE_ARC.as_('account_receivable_turnover_days')
    inventory_turnover = finance_indicator.OPE_STCI.as_('inventory_turnover')
    account_receivable_turnover_rate = finance_indicator.OPE_ACI.as_('account_receivable_turnover_rate')
    current_asset_turnover = finance_indicator.OPE_FAI.as_('current_asset_turnover')
    fixed_asset_turnover = finance_indicator.OPE_FCI.as_('fixed_asset_turnover')
    total_asset_turnover = finance_indicator.OPE_TAI.as_('total_asset_turnover')
    inc_earnings_per_share = finance_indicator.RIS_EPS.as_('inc_earnings_per_share')
    inc_diluted_earnings_per_share = finance_indicator.RIS_EPSD.as_('inc_diluted_earnings_per_share')
    inc_revenue = finance_indicator.RIS_TR.as_('inc_revenue')
    inc_operating_revenue = finance_indicator.RIS_OR.as_('inc_operating_revenue')
    inc_gross_profit = finance_indicator.RIS_OP.as_('inc_gross_profit')
    inc_profit_before_tax = finance_indicator.RIS_TP.as_('inc_profit_before_tax')
    inc_net_profit = finance_indicator.RIS_MNP.as_("inc_net_profit")
    inc_adjusted_net_profit = finance_indicator.RIS_MNPC.as_("inc_adjusted_net_profit")
    inc_cash_from_operations = finance_indicator.RIS_NC.as_('inc_cash_from_operations')
    inc_return_on_equity = finance_indicator.RIS_ROE.as_('inc_return_on_equity')
    inc_book_per_share = finance_indicator.RIS_NA.as_('inc_book_per_share')
    inc_total_asset = finance_indicator.RIS_TA.as_('inc_total_asset')
    du_return_on_equity = finance_indicator.DU_ROE.as_('du_return_on_equity')
    du_equity_multiplier = finance_indicator.DU_RS.as_('du_equity_multiplier')
    du_asset_turnover_ratio = finance_indicator.DU_TAC.as_('du_asset_turnover_ratio')
    du_profit_margin = finance_indicator.DU_NP_TP.as_('du_profit_margin')
    du_return_on_sales = finance_indicator.DU_EBIT_OR.as_('du_return_on_sales')
    non_recurring_profit_and_loss = finance_indicator.INC_A.as_('non_recurring_profit_and_loss')
    adjusted_net_profit = finance_indicator.INC_B.as_('adjusted_net_profit')
    ebit = finance_indicator.INC_F.as_('ebit')
    ebitda = finance_indicator.INC_G.as_('ebitda')
    invested_capital = finance_indicator.BAL_A.as_('invested_capital')
    working_capital = finance_indicator.BAL_B.as_('working_capital')
    net_working_capital = finance_indicator.BAL_C.as_('net_working_capital')
    tangible_assets = finance_indicator.BAL_D.as_('tangible_assets')
    retained_earnings = finance_indicator.BAL_E.as_('retained_earnings')
    interest_bearing_debt = finance_indicator.BAL_F.as_('interest_bearing_debt')
    net_debt = finance_indicator.BAL_G.as_('net_debt')
    non_interest_bearing_current_debt = finance_indicator.BAL_H.as_('non_interest_bearing_current_debt')
    non_interest_bearing_non_current_debt = finance_indicator.BAL_I.as_('non_interest_bearing_non_current_debt')
    fcff = finance_indicator.BAL_J.as_('fcff')
    fcfe = finance_indicator.BAL_K.as_('fcfe')
    depreciation_and_amortization = finance_indicator.BAL_L.as_('depreciation_and_amortization')

    @staticmethod
    def filter_conditions_():
        # select * from ana_stk_fin_idx where ISVALID=1 AND COMCODE=#{code}
        return finance_indicator.isvalid == 1

    @staticmethod
    def name_():
        return 'ana_stk_fin_idx'


def parse_metrics(clazz):
    variables = clazz.__dict__
    mem_vars = []
    for var in variables:
        if var.startswith('_') or var.endswith('_'):
            continue
        mem_vars.append(getattr(clazz, var))
    return mem_vars


QUARTER_TABLES_MAP = {
    income_statement: Income,
    balance_sheet: Balance,
    cash_flow: CashFlow,
    finance_indicator: Indicator,
}

QUARTER_ENDDATE_MAP = {
    1: '0331',
    2: '0630',
    3: '0930',
    4: '1231',
}
