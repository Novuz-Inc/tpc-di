-- Databricks notebook source
create or refresh streaming live table public_taxrate_stg
(TX_ID string NOT NULL COMMENT 'TAX RATE CODE',
 TX_NAME string NOT NULL COMMENT 'TAX RATE DESCRIPTION', 
 TX_RATE DECIMAL (6,5 ) COMMENT 'TAX RATE')
using delta 
as select
 _c0::string TX_ID, _c1::string TX_NAME, _c2::DECIMAL(6,5) TX_RATE
from cloud_files(
                 --"s3://tpcdi-files/tmp/tpcdi/sf=${tpcdi_scale}/Batch1/TaxRate.txt",
                 "s3://tpcdi-files/tmp/tpcdi/sf=1000/Batch1/TaxRate.txt",
                 "csv",
                 map("inferSchema", "True", 
                     
                     "header", "False",
                     "delimiter", "|")
                )
