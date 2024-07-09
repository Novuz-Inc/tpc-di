-- Databricks notebook source
create or refresh streaming live table public_dailymarket_stg
(CDC_FLAG string COMMENT 'I DENOTES INSERT',
 CDC_DSN INT COMMENT 'DATABASE SEQUENCE NUMBER',
 DM_DATE DATE NOT NULL COMMENT 'DATE OF LAST COMPLETED TRADING DAY.',
 DM_S_SYMB string NOT NULL COMMENT 'SECURITY SYMBOL OF THE SECURITY',
 DM_CLOSE DECIMAL ( 8 , 2 ) NOT NULL COMMENT 'CLOSING PRICE OF THE SECURITY ON THIS DAY.',
 DM_HIGH DECIMAL ( 8 , 2 ) NOT NULL COMMENT 'HIGHEST PRICE FOR THE SECUIRITY ON THIS DAY.',
 DM_LOW DECIMAL ( 8 , 2 ) NOT NULL COMMENT 'LOWEST PRICE FOR THE SECURITY ON THIS DAY.',
 DM_VOL INT NOT NULL COMMENT 'VOLUME OF THE SECURITY ON THIS DAY.')
using delta 
as select * from stream(live.public_dailymarket_stg_1)
union all select * from stream(live.public_dailymarket_stg_2)
union all select * from stream(live.public_dailymarket_stg_3)
