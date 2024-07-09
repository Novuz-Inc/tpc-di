-- Databricks notebook source
create or refresh streaming live table public_statustype_stg
(ST_ID string NOT NULL COMMENT 'STATUS CODE',
 ST_NAME string NOT NULL COMMENT 'STATUS DESCRIPTION')
using delta 
as select
 _c0::string ST_ID, _c1::string ST_NAME
from cloud_files(
                 --"s3://tpcdi-files/tmp/tpcdi/sf=${tpcdi_scale}/Batch1/StatusType.txt",
                 "s3://tpcdi-files/tmp/tpcdi/sf=1000/Batch1/StatusType.txt",
                 "csv",
                 map("inferSchema", "False", 
                     
                     "header", "False",
                     "delimiter", "|")
                )
