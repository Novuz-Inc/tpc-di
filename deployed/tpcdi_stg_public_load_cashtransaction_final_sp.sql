-- Databricks notebook source
create or refresh streaming live table public_cashtransaction_stg
(CDC_FLAG string COMMENT 'I DENOTES INSERT',
 CDC_DSN INT COMMENT 'DATABASE SEQUENCE NUMBER',
 CT_CA_ID INT NOT NULL COMMENT 'CUSTOMER ACCOUNT IDENTIFIER',
 CT_DTS TIMESTAMP NOT NULL COMMENT 'TIMESTAMP OF WHEN THE TRADE TOOK PLACE',
 CT_AMT DECIMAL ( 10 , 2 ) NOT NULL COMMENT 'AMOUNT OF THE CASH TRANSACTION',
 CT_NAME string NOT NULL COMMENT 'TRANSACTION NAME OR DESCRIPTION: E.G. "CASH FROM SALE OF DUPONT STOCK".')
using delta 
as select * from stream(live.public_cashtransaction_stg_1)
union all select * from stream(live.public_cashtransaction_stg_2)
union all select * from stream(live.public_cashtransaction_stg_3)
