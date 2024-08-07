-- Databricks notebook source
create or refresh streaming live table public_dailymarket_stg_3
(CDC_FLAG string COMMENT 'I DENOTES INSERT',
 CDC_DSN INT COMMENT 'DATABASE SEQUENCE NUMBER',
 DM_DATE DATE NOT NULL COMMENT 'DATE OF LAST COMPLETED TRADING DAY.',
 DM_S_SYMB string NOT NULL COMMENT 'SECURITY SYMBOL OF THE SECURITY',
 DM_CLOSE DECIMAL ( 8 , 2 ) NOT NULL COMMENT 'CLOSING PRICE OF THE SECURITY ON THIS DAY.',
 DM_HIGH DECIMAL ( 8 , 2 ) NOT NULL COMMENT 'HIGHEST PRICE FOR THE SECUIRITY ON THIS DAY.',
 DM_LOW DECIMAL ( 8 , 2 ) NOT NULL COMMENT 'LOWEST PRICE FOR THE SECURITY ON THIS DAY.',
 DM_VOL INT NOT NULL COMMENT 'VOLUME OF THE SECURITY ON THIS DAY.')
using delta 
as select
 _c0::DATE DM_DATE, _c1::string DM_S_SYMB, _c2::DECIMAL(8, 2) DM_CLOSE,
 _c3::DECIMAL(8, 2) DM_HIGH, _c4::DECIMAL(8, 2) DM_LOW, _c5::INT DM_VOL
from cloud_files(
                 --"s3://tpcdi-files/tmp/tpcdi/sf=${tpcdi_scale}/Batch1/DailyMarket",
                 "s3://tpcdi-files/tmp/tpcdi/sf=1000/Batch1/DailyMarket",
                 "csv",
                 map("inferSchema", "False", 
                     
                     "header", "False",
                     "delimiter", "|")
                )
