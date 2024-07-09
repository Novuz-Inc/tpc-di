-- Databricks notebook source
create or refresh streaming live table public_industry_stg
(IN_ID string NOT NULL COMMENT 'INDUSTRY CODE',
 IN_NAME string NOT NULL COMMENT 'INDUSTRY DESCRIPTION',
 IN_SC_ID string NOT NULL COMMENT 'SECTOR IDENTIFIER')
using delta 
as select
 _c0::string IN_ID, _c1::string IN_NAME, _c2::string IN_SC_ID
from cloud_files(
                 --"s3://tpcdi-files/tmp/tpcdi/sf=${tpcdi_scale}/Batch1/Industry.txt",
                 "s3://tpcdi-files/tmp/tpcdi/sf=1000/Batch1/Industry.txt",
                 "csv",
                 map("inferSchema", "False", 
                     
                     "header", "False",
                     "delimiter", "|")
                )
