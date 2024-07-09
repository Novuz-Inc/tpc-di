-- Databricks notebook source
create or refresh streaming live table public_customer_mgmt_stg
(XML string COMMENT 'XML STRING')
using delta 
as select
 _c0::string XML
from cloud_files(
                 --"s3://tpcdi-files/tmp/tpcdi/sf=${tpcdi_scale}/Batch1/CustomerMgmt",
                 "s3://tpcdi-files/tmp/tpcdi/sf=1000/Batch1/CustomerMgmt",
                 "csv",
                 map("inferSchema", "False", 
                     
                     "header", "False",
                     "delimiter", ",")
                )
