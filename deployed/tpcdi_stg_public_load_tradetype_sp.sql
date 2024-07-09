-- Databricks notebook source
create or refresh streaming live table public_tradetype_stg
(TT_ID string NOT NULL COMMENT 'TRADE TYPE CODE',
 TT_NAME string NOT NULL COMMENT 'TRADE TYPE DESCRIPTION',
 TT_IS_SELL INT NOT NULL COMMENT 'FLAG INDICATING A SALE',
 TT_IS_MRKT INT NOT NULL COMMENT 'FLAG INDICATING A MARKET ORDER')
using delta 
as select
 _c0::string TT_ID, _c1::string TT_NAME, _c2::INT TT_IS_SELL,
 _c3::INT TT_IS_MRKT
from cloud_files(
                 --"s3://tpcdi-files/tmp/tpcdi/sf=${tpcdi_scale}/Batch1/TradeType.txt",
                 "s3://tpcdi-files/tmp/tpcdi/sf=1000/Batch1/TradeType.txt",
                 "csv",
                 map("inferSchema", "False", 
                     "allowOverwrites", "True",
                     "header", "False",
                     "delimiter", "|")
                )
