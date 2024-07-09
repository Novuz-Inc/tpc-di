-- Databricks notebook source
create or refresh streaming live table public_watch_history_stg_2
(CDC_FLAG string COMMENT 'I, U DENOTES INSERT, UPDATE',
 CDC_DSN DECIMAL ( 12 ) COMMENT 'DATABASE SEQUENCE NUMBER',
 W_C_ID DECIMAL ( 11 ) NOT NULL COMMENT 'CUSTOMER IDENTIFIER',
 W_S_SYMB string NOT NULL COMMENT 'SYMBOL OF THE SECURITY TO WATCH',
 W_DTS TIMESTAMP NOT NULL COMMENT 'DATE AND TIME STAMP FOR THE ACTION',
 W_ACTION string COMMENT 'WHETHER ACTIVATING OR CANCELING THE WATCH')
using delta 
as select
 _c0::string CDC_FLAG, _c1::DECIMAL(12) CDC_DSN, _c2::DECIMAL(11) W_C_ID,
 _c3::string W_S_SYMB, _c4::TIMESTAMP W_DTS, _c5::string W_ACTION
from cloud_files(
                 --"s3://tpcdi-files/tmp/tpcdi/sf=${tpcdi_scale}/Batch[2-3]/WatchHistory",
                 "s3://tpcdi-files/tmp/tpcdi/sf=1000/Batch[2-3]/WatchHistory",
                 "csv",
                 map("inferSchema", "False", 
                     
                     "header", "False",
                     "delimiter", "|")
                )
