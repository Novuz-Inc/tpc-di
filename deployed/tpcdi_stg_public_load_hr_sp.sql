-- Databricks notebook source
create or refresh streaming live table public_hr_stg
(EMPLOYEEID DECIMAL ( 11 ) NOT NULL COMMENT 'ID OF EMPLOYEE',
 MANAGERID DECIMAL ( 11 ) NOT NULL COMMENT 'ID OF EMPLOYEES MANAGER',
 EMPLOYEEFIRSTNAME string NOT NULL COMMENT 'FIRST NAME',
 EMPLOYEELASTNAME string NOT NULL COMMENT 'LAST NAME',
 EMPLOYEEMI string COMMENT 'MIDDLE INITIAL',
 EMPLOYEEJOBCODE DECIMAL ( 3 ) COMMENT 'NUMERIC JOB CODE',
 EMPLOYEEBRANCH string COMMENT 'FACILITY IN WHICH EMPLOYEE HAS OFFICE',
 EMPLOYEEOFFICE string COMMENT 'OFFICE NUMBER OR DESCRIPTION',
 EMPLOYEEPHONE string COMMENT 'EMPLOYEE PHONE NUMBER')
using delta 
as select
 _c0::DECIMAL(11) EMPLOYEEID, _c1::DECIMAL(11) MANAGERID,
 _c2::string EMPLOYEEFIRSTNAME, _c3::string EMPLOYEELASTNAME,
 _c4::string EMPLOYEEMI, _c5::DECIMAL(3) EMPLOYEEJOBCODE,
 _c6::string EMPLOYEEBRANCH, _c7::string EMPLOYEEOFFICE,
 _c8::string EMPLOYEEPHONE
from cloud_files(
                 --"s3://tpcdi-files/tmp/tpcdi/sf=${tpcdi_scale}/Batch1/HR",
                 "s3://tpcdi-files/tmp/tpcdi/sf=1000/Batch1/HR",
                 "csv",
                 map("inferSchema", "False", 
                     
                     "header", "False",
                     "delimiter", ",")
                )
