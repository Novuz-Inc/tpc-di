-- Databricks notebook source
create or refresh streaming live table public_account_stg
(CDC_FLAG string COMMENT 'I OR U DENOTES INSERT OR UPDATE',
 CDC_DSN INT COMMENT 'DATABASE SEQUENCE NUMBER',
 CA_ID INT NOT NULL COMMENT 'CUSTOMER ACCOUNT IDENTIFIER',
 CA_B_ID INT NOT NULL COMMENT 'IDENTIFIER OF THE MANAGING BROKER',
 CA_C_ID INT NOT NULL COMMENT 'OWNING CUSTOMER IDENTIFIER',
 CA_NAME string COMMENT 'NAME OF CUSTOMER ACCOUNT', 
 CA_TAX_ST INT COMMENT '0, 1 OR 2 TAX STATUS OF THIS ACCOUNT',
 CA_ST_ID string COMMENT 'ACTV OR INAC CUSTOMER STATUS TYPE IDENTIFIER')
using delta 
as select
 _c0::string CDC_FLAG, _c1::INT CDC_DSN, _c2::INT CA_ID, _c3::INT CA_B_ID,
 _c4::INT CA_C_ID, _c5::string CA_NAME, _c6::INT CA_TAX_ST,
 _c7::string CA_ST_ID
from cloud_files(
                 --"s3://tpcdi-files/tmp/tpcdi/sf=${tpcdi_scale}/Batch[2-3]/Account",
                 "s3://tpcdi-files/tmp/tpcdi/sf=1000/Batch[2-3]/Account",
                 "csv",
                 map("inferSchema", "False", 
                     
                     "header", "False",
                     "delimiter", "|")
                )
