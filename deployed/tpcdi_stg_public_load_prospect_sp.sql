-- Databricks notebook source
create or refresh streaming live table public_prospect_stg
(AGENCYID string NOT NULL COMMENT 'UNIQUE IDENTIFIER FROM AGENCY',
 LASTNAME string NOT NULL COMMENT 'LAST NAME',
 FIRSTNAME string NOT NULL COMMENT 'FIRST NAME',
 MIDDLEINITIAL string COMMENT 'MIDDLE INITIAL',
 GENDER string COMMENT 'M OR F OR U',
 ADDRESSLINE1 string COMMENT 'POSTAL ADDRESS',
 ADDRESSLINE2 string COMMENT 'POSTAL ADDRESS',
 POSTALCODE string COMMENT 'POSTAL CODE', 
 CITY string NOT NULL COMMENT 'CITY',
 STATE string NOT NULL COMMENT 'STATE OR PROVINCE',
 COUNTRY string COMMENT 'POSTAL COUNTRY',
 PHONE string COMMENT 'TELEPHONE NUMBER',
 INCOME DECIMAL ( 9 ) COMMENT 'ANNUAL INCOME',
 NUMBERCARS DECIMAL ( 2 ) COMMENT 'CARS OWNED',
 NUMBERCHILDREN DECIMAL ( 2 ) COMMENT 'DEPENDENT CHILDREN',
 MARITALSTATUS string COMMENT 'S OR M OR D OR W OR U',
 AGE DECIMAL ( 3 ) COMMENT 'CURRENT AGE',
 CREDITRATING DECIMAL ( 4 ) COMMENT 'NUMERIC RATING',
 OWNORRENTFLAG string COMMENT 'O OR R OR U',
 EMPLOYER string COMMENT 'NAME OF EMPLOYER',
 NUMBERCREDITCARDS DECIMAL ( 2 ) COMMENT 'CREDIT CARDS',
 NETWORTH DECIMAL ( 12 ) COMMENT 'ESTIMATED TOTAL NET WORTH')
using delta 
as select
 _c0::string AGENCYID, _c1::string LASTNAME, _c2::string FIRSTNAME,
 _c3::string MIDDLEINITIAL, _c4::string GENDER, _c5::string ADDRESSLINE1,
 _c6::string ADDRESSLINE2, _c7::string POSTALCODE, _c8::string CITY,
 _c9::string STATE, _c10::string COUNTRY, _c11::string PHONE,
 _c12::DECIMAL(9) INCOME, _c13::DECIMAL(2) NUMBERCARS,
 _c14::DECIMAL(2) NUMBERCHILDREN, _c15::string MARITALSTATUS,
 _c16::DECIMAL(3) AGE, _c17::DECIMAL(4) CREDITRATING,
 _c18::string OWNORRENTFLAG, _c19::string EMPLOYER,
 _c20::DECIMAL(2) NUMBERCREDITCARDS, _c21::DECIMAL(12) NETWORTH
from cloud_files(
                 --"s3://tpcdi-files/tmp/tpcdi/sf=${tpcdi_scale}/Batch[2-3]/Prospect",
                 "s3://tpcdi-files/tmp/tpcdi/sf=1000/Batch[2-3]/Prospect",
                 "csv",
                 map("inferSchema", "False", 
                     
                     "header", "False",
                     "delimiter", ",")
                )
