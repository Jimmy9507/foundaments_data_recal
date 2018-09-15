CREATE TABLE IF NOT EXISTS %s
(
   stockcode char(11) NOT NULL,
   tradedate int(11) NOT NULL,

   pe_ratio decimal(18,4),
   pcf_ratio decimal(18,4),
   pb_ratio decimal(18,4),
   market_cap decimal(21,4),
   market_cap_2 decimal(21,4),
   a_share_market_val decimal(21,4),
   a_share_market_val_2 decimal(21,4),
   val_of_stk_right decimal(21,4),
   ev decimal(21,4),
   ev_2 decimal(21,4),
   ev_to_ebit decimal(18,4),
   dividend_yield decimal(18,4),
   pe_ratio_1 decimal(18,4),
   pe_ratio_2 decimal(18,4),
   peg_ratio decimal(18,4),
   pcf_ratio_1 decimal(18,4),
   pcf_ratio_2 decimal(18,4),
   pcf_ratio_3 decimal(18,4),
   ps_ratio decimal(18,4),

   PRIMARY KEY (STOCKCODE, TRADEDATE)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
