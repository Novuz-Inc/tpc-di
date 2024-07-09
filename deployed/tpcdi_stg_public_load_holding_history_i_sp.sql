-- Databricks notebook source
create or refresh streaming live table public_holdinghistory_stg_2
(CDC_FLAG string COMMENT 'I DENOTES INSERT',
 CDC_DSN INT COMMENT 'DATABASE SEQUENCE NUMBER',
 HH_H_T_ID DECIMAL ( 15 ) NOT NULL COMMENT 'TRADE IDENTIFIER OF THE TRADE THAT ORIGINALLY CREATED THE HOLDING ROW.', HH_T_ID DECIMAL ( 15 ) NOT NULL COMMENT 'TRADE IDENTIFIER OF THE CURRENT TRADE', HH_BEFORE_QTY DECIMAL ( 6 ) NOT NULL COMMENT 'QUANTITY OF THIS SECURITY HELD BEFORE THE MODIFYING TRADE.', HH_AFTER_QTY DECIMAL ( 6 ) NOT NULL COMMENT 'QUANTITY OF THIS SECURITY HELD AFTER THE MODIFYING TRADE.')
using delta 
as select
 _c0::string CDC_FLAG, _c1::INT CDC_DSN, _c2::DECIMAL(15) HH_H_T_ID,
 _c3::DECIMAL(15) HH_T_ID, _c4::DECIMAL(6) HH_BEFORE_QTY,
 _c5::DECIMAL(6) HH_AFTER_QTY
from cloud_files(
                 ---"s3://tpcdi-files/tmp/tpcdi/sf=${tpcdi_scale}/Batch[2-3]/HoldingHistory",
                 "s3://tpcdi-files/tmp/tpcdi/sf=1000/Batch[2-3]/HoldingHistory",
                 "csv",
                 map("inferSchema", "False", 
                     
                     "header", "False",
                     "delimiter", "|")
                )
