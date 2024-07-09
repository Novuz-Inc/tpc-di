-- Databricks notebook source
create or refresh streaming live table public_cashtransaction_stg_1
(CDC_FLAG string COMMENT 'I DENOTES INSERT',
 CDC_DSN INT COMMENT 'DATABASE SEQUENCE NUMBER',
 CT_CA_ID INT NOT NULL COMMENT 'CUSTOMER ACCOUNT IDENTIFIER',
 CT_DTS TIMESTAMP NOT NULL COMMENT 'TIMESTAMP OF WHEN THE TRADE TOOK PLACE',
 CT_AMT DECIMAL ( 10 , 2 ) NOT NULL COMMENT 'AMOUNT OF THE CASH TRANSACTION',
 CT_NAME string NOT NULL COMMENT 'TRANSACTION NAME OR DESCRIPTION: E.G. "CASH FROM SALE OF DUPONT STOCK".')
using delta 
as select
 _c0::string CDC_FLAG, _c1::INT CDC_DSN, _c2::INT CT_CA_ID,
 _c3::TIMESTAMP CT_DTS, _c4::DECIMAL(10, 2) CT_AMT, _c5::string CT_NAME
from cloud_files(
                 "s3://tpcdi-files/load/cash_transaction/",
                 "csv",
                 map("inferSchema", "False", 
                     
                     "header", "False",
                     "delimiter", "|")
                )
