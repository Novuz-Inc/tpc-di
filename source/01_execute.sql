-- Execute TPC-DI Benchmark with Scale Factor : 5000

-- Swith to use the TPC_DI warehouse
USE WAREHOUSE TPCDI_GENERAL;

-- Execute the TPC DI Benchmark
CALL TPCDI_WH.PUBLIC.RUN_ALL_SP(100);

