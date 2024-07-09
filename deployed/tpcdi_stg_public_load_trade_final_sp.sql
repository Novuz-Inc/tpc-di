-- Databricks notebook source
create or refresh streaming live table public_trade_stg
(CDC_FLAG string COMMENT 'I, U DENOTES INSERT, UPDATE',
 CDC_DSN DECIMAL ( 12 ) COMMENT 'DATABASE SEQUENCE NUMBER',
 T_ID DECIMAL ( 15 ) NOT NULL COMMENT 'TRADE IDENTIFIER.',
 T_DTS TIMESTAMP NOT NULL COMMENT 'DATE AND TIME OF TRADE.',
 T_ST_ID string NOT NULL COMMENT 'STATUS TYPE IDENTIFIER',
 T_TT_ID string NOT NULL COMMENT 'TRADE TYPE IDENTIFIER',
 T_IS_CASH INT COMMENT 'IS THIS TRADE A CASH (1) OR MARGIN (0) TRADE?',
 T_S_SYMB string NOT NULL COMMENT 'SECURITY SYMBOL OF THE SECURITY',
 T_QTY DECIMAL ( 6 ) COMMENT 'QUANTITY OF SECURITIES TRADED.',
 T_BID_PRICE DECIMAL ( 8 , 2 ) COMMENT 'THE REQUESTED UNIT PRICE.',
 T_CA_ID DECIMAL ( 11 ) NOT NULL COMMENT 'CUSTOMER ACCOUNT IDENTIFIER.',
 T_EXEC_NAME string NOT NULL COMMENT 'NAME OF THE PERSON EXECUTING THE TRADE.', 
 T_TRADE_PRICE DECIMAL ( 8 , 2 ) COMMENT 'UNIT PRICE AT WHICH THE SECURITY WAS TRADED.', 
 T_CHRG DECIMAL ( 10 , 2 ) COMMENT 'FEE CHARGED FOR PLACING THIS TRADE REQUEST.', 
 T_COMM DECIMAL ( 10 , 2 ) COMMENT 'COMMISSION EARNED ON THIS TRADE', 
 T_TAX DECIMAL ( 10 , 2 ) COMMENT 'AMOUNT OF TAX DUE ON THIS TRADE')
using delta 
as select * from stream(live.public_trade_stg_1)
union all select * from stream(live.public_trade_stg_2)
union all select * from stream(live.public_trade_stg_3)
