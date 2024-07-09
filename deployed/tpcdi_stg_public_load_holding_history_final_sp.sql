-- Databricks notebook source
create or refresh streaming live table public_holdinghistory_stg
(CDC_FLAG string COMMENT 'I DENOTES INSERT',
 CDC_DSN INT COMMENT 'DATABASE SEQUENCE NUMBER',
 HH_H_T_ID DECIMAL ( 15 ) NOT NULL COMMENT 'TRADE IDENTIFIER OF THE TRADE THAT ORIGINALLY CREATED THE HOLDING ROW.', HH_T_ID DECIMAL ( 15 ) NOT NULL COMMENT 'TRADE IDENTIFIER OF THE CURRENT TRADE', 
 HH_BEFORE_QTY DECIMAL ( 6 ) NOT NULL COMMENT 'QUANTITY OF THIS SECURITY HELD BEFORE THE MODIFYING TRADE.', 
 HH_AFTER_QTY DECIMAL ( 6 ) NOT NULL COMMENT 'QUANTITY OF THIS SECURITY HELD AFTER THE MODIFYING TRADE.')
using delta 
as select * from stream(live.public_holdinghistory_stg_1)
union all select * from stream(live.public_holdinghistory_stg_2)
union all select * from stream(live.public_holdinghistory_stg_3)
