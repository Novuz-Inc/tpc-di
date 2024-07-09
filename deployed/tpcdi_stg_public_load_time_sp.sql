-- Databricks notebook source
create or refresh streaming live table public_time_stg
(SK_TIMEID INT NOT NULL COMMENT 'SURROGATE KEY FOR THE TIME',
 TIMEVALUE string NOT NULL COMMENT 'THE TIME AS TEXT, E.G. “01:23:45”',
 HOURID DECIMAL ( 2 ) NOT NULL COMMENT 'HOUR NUMBER AS A NUMBER, E.G. 01',
 HOURDESC string NOT NULL COMMENT 'HOUR NUMBER AS TEXT, E.G. “01”',
 MINUTEID DECIMAL ( 2 ) NOT NULL COMMENT 'MINUTE AS A NUMBER, E.G. 23',
 MINUTEDESC string NOT NULL COMMENT 'MINUTE AS TEXT, E.G. “01:23”',
 SECONDID DECIMAL ( 2 ) NOT NULL COMMENT 'SECOND AS A NUMBER, E.G. 45',
 SECONDDESC string NOT NULL COMMENT 'SECOND AS TEXT, E.G. “01:23:45”',
 MARKETHOURSFLAG BOOLEAN COMMENT 'INDICATES A TIME DURING MARKET HOURS',
 OFFICEHOURSFLAG BOOLEAN COMMENT 'INDICATES A TIME DURING OFFICE HOURS')
using delta 
as select
 _c0::INT SK_TIMEID, _c1::string TIMEVALUE, _c2::DECIMAL(2) HOURID,
 _c3::string HOURDESC, _c4::DECIMAL(2) MINUTEID, _c5::string MINUTEDESC,
 _c6::DECIMAL(2) SECONDID, _c7::string SECONDDESC,
 _c8::BOOLEAN MARKETHOURSFLAG, _c9::BOOLEAN OFFICEHOURSFLAG
from cloud_files(
                 --"s3://tpcdi-files/tmp/tpcdi/sf=${tpcdi_scale}/Batch1/Time.txt",
                 "s3://tpcdi-files/tmp/tpcdi/sf=1000/Batch1/Time.txt",
                 "csv",
                 map("inferSchema", "False", 
                     
                     "header", "False",
                     "delimiter", "|")
                )
