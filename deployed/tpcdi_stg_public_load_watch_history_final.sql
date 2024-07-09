-- Databricks notebook source
create or refresh streaming live table public_watch_history_stg
(CDC_FLAG string COMMENT 'I, U DENOTES INSERT, UPDATE',
 CDC_DSN DECIMAL ( 12 ) COMMENT 'DATABASE SEQUENCE NUMBER',
 W_C_ID DECIMAL ( 11 ) NOT NULL COMMENT 'CUSTOMER IDENTIFIER',
 W_S_SYMB string NOT NULL COMMENT 'SYMBOL OF THE SECURITY TO WATCH',
 W_DTS TIMESTAMP NOT NULL COMMENT 'DATE AND TIME STAMP FOR THE ACTION',
 W_ACTION string COMMENT 'WHETHER ACTIVATING OR CANCELING THE WATCH')
using delta 
as select * from stream(live.public_watch_history_stg_1)
union all select * from stream(live.public_watch_history_stg_2)
union all select * from stream(live.public_watch_history_stg_3)
